package uantwerpen.be.fti.ei.Project.replication;

import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ReplicationManager {
    // this class is the manager, will delete and add files
    private final String nodeName;
    private final String ipAddress;
    private final String namingServerUrl;
    private final RestTemplate restTemplate;
    private final String storageDirectory;

    public ReplicationManager(String nodeName, String ipAddress, String namingServerUrl,
                              RestTemplate restTemplate, String storageDirectory) {
        this.nodeName = nodeName;
        this.ipAddress = ipAddress;
        this.namingServerUrl = namingServerUrl;
        this.restTemplate = restTemplate;
        this.storageDirectory = storageDirectory;
    }

    // Phase 1: Starting - Initial replication
    public void replicateInitialFiles() {
        try {
            Path storagePath = Paths.get(storageDirectory);
            if (!Files.exists(storagePath)) return;

            Files.list(storagePath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        String fileName = file.getFileName().toString();
                        int fileHash = HashingUtil.generateHash(fileName);

                        // Get target node from naming server
                        Map<String, String> response = restTemplate.getForObject(
                                namingServerUrl + "/api/replicate?hash=" + fileHash,
                                Map.class);

                        if (response != null && !response.get("ip").equals(ipAddress)) {
                            try {
                                byte[] fileData = Files.readAllBytes(file);
                                FileReplicator.transferFile(ipAddress, response.get("ip"), fileName, fileData);

                                // Update naming server about replication
                                restTemplate.postForObject(
                                        namingServerUrl + "/api/files",
                                        Map.of("fileName", fileName, "owner", response.get("ip"), "replicas", ipAddress),
                                        Void.class);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        } catch (IOException e) {
            System.err.println("Replication error: " + e.getMessage());
        }
    }

    // Phase 2: Update - Handle new files
    public void handleFileAddition(String fileName) {
        int fileHash = HashingUtil.generateHash(fileName);
        Map<String, String> response = restTemplate.getForObject(
                namingServerUrl + "/api/replicate?hash=" + fileHash,
                Map.class);

        if (response != null && !response.get("ip").equals(ipAddress)) {
            try {
                byte[] fileData = Files.readAllBytes(Paths.get(storageDirectory, fileName));
                FileReplicator.transferFile(ipAddress, response.get("ip"), fileName, fileData);

                restTemplate.postForObject(
                        namingServerUrl + "/api/files",
                        Map.of(
                                "fileName", fileName,
                                "owner", response.get("ip"),
                                "replicas", ipAddress),
                        Void.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Phase 2: Update - Handle file deletions
    public void handleFileDeletion(String fileName) {
        restTemplate.delete(namingServerUrl + "/api/files/" + fileName + "/replicas/" + ipAddress);
    }

    // Phase 3: Shutdown - Transfer replicated files
    public void transferReplicatedFiles() {
        // Get list of files we're responsible for
        Map<String, String>[] replicatedFiles = restTemplate.getForObject(
                namingServerUrl + "/api/nodes/" + HashingUtil.generateHash(nodeName) + "/replicated",
                Map[].class);

        if (replicatedFiles != null) {
            for (Map<String, String> fileInfo : replicatedFiles) {
                String fileName = fileInfo.get("fileName");
                String newOwner = findNewOwner(fileInfo.get("currentOwner"));

                try {
                    byte[] fileData = Files.readAllBytes(Paths.get(storageDirectory, fileName));
                    FileReplicator.transferFile(ipAddress, newOwner, fileName, fileData);

                    // Update naming server
                    restTemplate.put(
                            namingServerUrl + "/api/files/" + fileName + "/owner",
                            Map.of("newOwner", newOwner));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String findNewOwner(String currentOwner) {
        // Implementation to find the new owner based on hash ring
        // This would query the naming server for the appropriate node
        return restTemplate.getForObject(
                namingServerUrl + "/api/nodes/" + currentOwner + "/nextowner",
                String.class);
    }
}
