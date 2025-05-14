package uantwerpen.be.fti.ei.Project.replication;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

public class FileWatcher implements Runnable {
    // this class is for the Update phase
    private final Path dir;
    private final ReplicationManager manager;
    private Set<String> knownFiles;

    public FileWatcher(String directory, ReplicationManager manager) {
        this.dir = Paths.get(directory).toAbsolutePath();
        this.manager = manager;
        this.knownFiles = new HashSet<>();

        System.out.println("🔍 Initializing file watcher for: " + dir);

        try {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                System.out.println("✅ Created watched directory: " + dir);
            }
            scanInitialFiles();
            System.out.println("👀 Now watching " + knownFiles.size() + " existing files");
        } catch (IOException e) {
            System.err.println("❌ File watcher initialization failed: " + e.getMessage());
            throw new RuntimeException("File watcher startup failed", e);
        }
    }

    private void scanInitialFiles() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!Files.isDirectory(entry)) {
                    knownFiles.add(entry.getFileName().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE);

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key = watcher.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changed = (Path) event.context();
                    String fileName = changed.toString();

                    if (event.kind() == ENTRY_CREATE) {
                        if (!knownFiles.contains(fileName)) {
                            manager.handleFileAddition(fileName);
                            knownFiles.add(fileName);
                        }
                    } else if (event.kind() == ENTRY_DELETE) {
                        if (knownFiles.contains(fileName)) {
                            manager.handleFileDeletion(fileName);
                            knownFiles.remove(fileName);
                        }
                    }
                }
                key.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
