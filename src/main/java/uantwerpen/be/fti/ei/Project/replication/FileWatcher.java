package uantwerpen.be.fti.ei.Project.replication;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

import static java.nio.file.StandardWatchEventKinds.*;

public class FileWatcher implements Runnable {
    private final Path dir;
    private final ReplicationManager manager;
    private final Set<String> knownFiles;

    private final long SETTLE_DELAY_MS = 7000;

    public FileWatcher(String directory, ReplicationManager manager) {
        this.dir = Paths.get(directory).toAbsolutePath();
        this.manager = manager;
        this.knownFiles = new HashSet<>();

        System.out.println("FileWatcher: Initializing for directory: " + dir);
        try {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                System.out.println("FileWatcher: Created watched directory: " + dir);
            }
            scanInitialFiles();
        } catch (IOException e) {
            System.err.println("FileWatcher: Initialization failed - " + e.getMessage());
        }
    }

    private void scanInitialFiles() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    knownFiles.add(entry.getFileName().toString());
                }
            }
            System.out.println("FileWatcher: Scanned initial files. Found: " + knownFiles.size() + " files: " + knownFiles);
        } catch (IOException e) {
            System.err.println("FileWatcher: Error during initial file scan: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        System.out.println("FileWatcher: Starting watcher thread for " + dir + " with a settle delay of " + SETTLE_DELAY_MS + "ms.");
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key;
                try {
                    key = watcher.take(); // Blocks until an event occurs
                } catch (InterruptedException | ClosedWatchServiceException e) {
                    System.out.println("FileWatcher: Watcher thread interrupted or service closed. Stopping.");
                    Thread.currentThread().interrupt();
                    break;
                }

                // Process all events received for this key before resetting
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    Path changedPathContext = (Path) event.context();

                    if (changedPathContext == null) continue;
                    String fileName = changedPathContext.getFileName().toString();
                    Path fullPath = dir.resolve(fileName);

                    if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                        System.out.println("FileWatcher: Detected CREATE or MODIFY for '" + fileName + "'. Waiting " + SETTLE_DELAY_MS + "ms for file to settle...");
                        try {
                            Thread.sleep(SETTLE_DELAY_MS);
                        } catch (InterruptedException e) {
                            System.out.println("FileWatcher: Sleep interrupted while waiting for '" + fileName + "' to settle. Skipping.");
                            Thread.currentThread().interrupt();
                            continue;
                        }

                        // After delay, check if file still exists and is a regular file
                        if (Files.exists(fullPath) && Files.isRegularFile(fullPath)) {
                            System.out.println("FileWatcher: File '" + fileName + "' settled. Processing addition/modification.");
                            manager.handleFileAdditionOrModification(fileName);
                            knownFiles.add(fileName);
                        } else {
                            System.out.println("FileWatcher: File '" + fileName + "' no longer exists or is not a regular file after settle delay. It might have been a temporary file or deleted quickly.");
                            knownFiles.remove(fileName);
                        }

                    } else if (kind == ENTRY_DELETE) {
                        if (knownFiles.contains(fileName)) {
                            System.out.println("FileWatcher: Detected DELETE for '" + fileName + "'. Processing deletion.");
                            manager.handleFileDeletion(fileName);
                            knownFiles.remove(fileName);
                        } else {
                            System.out.println("FileWatcher: Detected DELETE for unknown file '" + fileName + "'. Ignoring.");
                        }
                    } else if (kind == OVERFLOW) {
                        System.err.println("FileWatcher: OVERFLOW event for " + dir + ". Events may have been lost!");
                        scanInitialFiles();
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    System.err.println("FileWatcher: WatchKey no longer valid for " + dir + ". Stopping watcher.");
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("FileWatcher: Watcher thread for " + dir + " finished.");
    }

    public void removeKnownFile(String fileName){
        knownFiles.remove(fileName);
    }
}