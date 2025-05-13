package uantwerpen.be.fti.ei.Project.Replication;

import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class FolderWatcher implements Runnable{
    private final Node node;
    private final String folderPath;
    private Set<String> knownFiles;

    public FolderWatcher(Node node, String ipAddress) {
        this.node = node;
        this.folderPath = "nodes_storage/" + ipAddress;
        this.knownFiles = new HashSet<>();
    }

    @Override
    public void run() {
        System.out.println("FolderWatcher gestart voor update-detectie...");

        while (true) {
            try {
                File folder = new File(folderPath);
                if (!folder.exists()) continue;

                Set<String> currentFiles = new HashSet<>();
                for (File file : folder.listFiles()) {
                    if (file.isFile()) {
                        String name = file.getName().replace(".txt", "");
                        currentFiles.add(name);
                    }
                }

                // Nieuwe bestanden gevonden
                Set<String> added = new HashSet<>(currentFiles);
                added.removeAll(knownFiles);

                // Verwijderde bestanden
                Set<String> removed = new HashSet<>(knownFiles);
                removed.removeAll(currentFiles);

                for (String file : added) {
                    System.out.println("Nieuw bestand ontdekt: " + file);
                    node.handleNewFile(file);
                }

                for (String file : removed) {
                    System.out.println("Bestand verwijderd: " + file);
                    node.handleRemovedFile(file);
                }

                knownFiles = currentFiles;

                Thread.sleep(5000); // check elke 5 seconden
            } catch (Exception e) {
                System.err.println("Fout in FolderWatcher");
                e.printStackTrace();
            }
        }
    }
}
