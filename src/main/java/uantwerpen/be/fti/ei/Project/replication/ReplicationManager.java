package uantwerpen.be.fti.ei.Project.replication;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil; // Assuming you have this

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationManager {
    private final String nodeName;
    private final String ipAddress;
    private final String namingServerUrl;
    private final RestTemplate restTemplate;
    private final String storageDirectory;
    private final int DEFAULT_TARGET_FILE_RECEIVER_PORT = 8082;

    public ReplicationManager(String nodeName, String ipAddress, String namingServerUrl,
                              RestTemplate restTemplate, String storageDirectory) {
        this.nodeName = nodeName;
        this.ipAddress = ipAddress;
        this.namingServerUrl = namingServerUrl;
        this.restTemplate = restTemplate;
        this.storageDirectory = storageDirectory;
        System.out.println("ReplicationManager for node '" + nodeName + "' initialized with storage: " + storageDirectory);
    }

    // --- Phase 1: Starting - Initial replication ---
    public void replicateInitialFiles() {
        System.out.println("Node '" + nodeName + "': Starting initial file replication from directory: " + storageDirectory);
        Path storagePath = Paths.get(storageDirectory);
        if (!Files.exists(storagePath) || !Files.isDirectory(storagePath)) {
            System.err.println("Node '" + nodeName + "': Storage directory missing or not a directory: " + storagePath.toAbsolutePath());
            return;
        }

        AtomicInteger processedCount = new AtomicInteger(0);
        try {
            Files.list(storagePath)
                    .filter(Files::isRegularFile)
                    .forEach(file -> {
                        String fileName = file.getFileName().toString();
                        System.out.println("Node '" + nodeName + "': Processing initial file for replication: " + fileName);
                        replicateSingleFileOrUpdate(fileName, file);
                        processedCount.getAndIncrement();
                    });
            System.out.println("Node '" + nodeName + "': Finished processing " + processedCount.get() + " initial file(s) for replication attempts.");
        } catch (IOException e) {
            System.err.println("Node '" + nodeName + "': Error listing files in storage directory for initial replication: " + e.getMessage());
        }
    }

    // --- Phase 2: Update - Handle new or modified files ---
    public void handleFileAdditionOrModification(String fileName) { // Renamed from handleFileAddition
        Path filePath = Paths.get(storageDirectory, fileName);
        System.out.println("Node '" + nodeName + "': ReplicationManager handling create/modify for file: " + filePath.toAbsolutePath());

        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
            System.err.println("Node '" + nodeName + "': File '" + fileName + "' to replicate does not exist or is not a regular file at: " + filePath.toAbsolutePath() + ". This can happen if deleted after event.");
            return;
        }
        replicateSingleFileOrUpdate(fileName, filePath);
    }

    // Helper method for replicating/updating a single file
    private void replicateSingleFileOrUpdate(String fileName, Path filePath) {
        try {
            int fileHash = HashingUtil.generateHash(fileName);
            System.out.println("Node '" + nodeName + "': Calculating replication target for '" + fileName + "' (hash: " + fileHash + ")");

            // Ask Naming Server where this file (primarily owned by another node) should be replicated
            // This implies the Naming Server's /api/replicate endpoint knows the replication strategy
            // (e.g., replicate to the node *previous* to the owner, or to N-1 other nodes).
            // The current implementation of your NamingServer.getNodeForReplication seems to aim for this.
            String replicationQueryUrl = namingServerUrl + "/api/replicate?hash=" + fileHash;
            Map<String, String> response;
            try {
                response = restTemplate.getForObject(replicationQueryUrl, Map.class);
            } catch (HttpClientErrorException.NotFound e) {
                System.out.println("Node '" + nodeName + "': No replication target found by Naming Server for file: " + fileName + ". This node might be the sole responsible node or alone.");
                return;
            } catch (Exception e) {
                System.err.println("Node '" + nodeName + "': Error contacting Naming Server ("+ replicationQueryUrl +") for replication target of '" + fileName + "': " + e.getMessage());
                return;
            }

            if (response == null || !response.containsKey("ip")) {
                System.out.println("Node '" + nodeName + "': Naming Server returned no valid replication target IP for file: " + fileName);
                return;
            }

            String targetNodeIpForReplica = response.get("ip");

            if (targetNodeIpForReplica.equals(ipAddress)) {
                System.out.println("Node '" + nodeName + "': Skipping replication of '" + fileName + "' - Naming Server indicated target for replica is self (" + targetNodeIpForReplica + ").");
                return;
            }

            System.out.println("Node '" + nodeName + "': Replicating '" + fileName + "' to target: " + targetNodeIpForReplica);
            byte[] fileData = Files.readAllBytes(filePath);

            FileReplicator.transferFile(ipAddress, targetNodeIpForReplica, fileName, fileData);

            // After successful transfer, tell the Naming Server:
            // "File 'fileName' (owned by 'targetNodeIpForReplica') now has a replica on me ('ipAddress')"
            // OR, if this node *is* the owner and is replicating to targetNodeIpForReplica:
            // "File 'fileName' (owned by 'ipAddress') now has a replica on 'targetNodeIpForReplica'"
            // The semantics of your /api/files/replicate endpoint's ownerIp vs replicaIp are key here.
            // Based on Phase:Starting slide 3.3: "It becomes the owner of this file" (the replicated node)
            // This implies the targetNodeIpForReplica becomes the new owner, and this node (ipAddress) might just be a download location.
            // Let's assume for `replicateSingleFileOrUpdate`, this node *has* the file and is replicating it to where the NS says it *should* go.
            // If `targetNodeIpForReplica` is where it should go, then `targetNodeIpForReplica` is the owner.
            Map<String, String> replicationRegistrationPayload = Map.of(
                    "fileName", fileName,
                    "ownerIp", targetNodeIpForReplica, // The node the NS designated as responsible/owner for this file hash
                    "replicaIp", this.ipAddress       // This node initially had the file, but it might not be the true owner
                    // This needs clarification based on your system's definition of "owner" vs "download location"
                    // For "Starting Phase" slide 6.3, the replicated node becomes the owner.
                    // If this node is *sending* its local file, it's the current download location, and targetIp becomes owner.
            );
            // Re-evaluating: if this node has a local file, it *reports* it to NS (slide 6.2)
            // NS receives hash, determines replication (slide 6.3) -> target node becomes owner.
            // NS informs originating node (this node), originating node transfers to target (slide 6.4).
            // So, when this node (originating) calls /api/files/replicate, ownerIp should be targetIp.
            String registerUrl = namingServerUrl + "/api/files/replicate";
            try {
                restTemplate.postForObject(registerUrl, replicationRegistrationPayload, Void.class);
                System.out.println("Node '" + nodeName + "': Successfully replicated '" + fileName + "' to " + targetNodeIpForReplica + " and registered new ownership/replication with Naming Server.");
            } catch (Exception e) {
                System.err.println("Node '" + nodeName + "': Failed to register replication of '" + fileName + "' with Naming Server: " + e.getMessage());
            }

        } catch (IOException e) {
            System.err.println("Node '" + nodeName + "': IOException during replication of '" + fileName + "': " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Unexpected error during replication of '" + fileName + "': " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- Phase 2: Update - Handle file deletions from local storage ---
    public void handleFileDeletion(String fileName) {
        System.out.println("Node '" + nodeName + "': Handling deletion of local file: " + fileName + ". Notifying Naming Server.");
        try {
            // Tell NS that this node (ipAddress) no longer has 'fileName'.
            // NS will update its records. If this node was an owner or important replica, NS might trigger other actions.
            String deleteFileLocationUrl = namingServerUrl + "/api/files/" + fileName + "/locations/" + ipAddress;
            restTemplate.delete(deleteFileLocationUrl);
            System.out.println("Node '" + nodeName + "': Notified Naming Server that '" + fileName + "' is no longer at " + ipAddress + ".");
        } catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Error notifying Naming Server of local file deletion for '" + fileName + "': " + e.getMessage());
        }
    }



    public void transferReplicasToPreviousNodeOnShutdown() {
        System.out.println("Node '" + nodeName + "' (IP: " + ipAddress + "): Initiating transfer of its REPLICAS to previous node due to shutdown.");

        // 1. Get the list of files that THIS node is holding as a REPLICA.
        //    This means the Naming Server considers another node to be the "owner" of these files.
        //    The endpoint should return: List of {"fileName": "name.txt", "originalOwnerIp": "ip_of_actual_owner"}
        String listHeldReplicasUrl = namingServerUrl + "/api/nodes/ip/" + ipAddress + "/held-replicas"; // NEW/MODIFIED Endpoint needed on NS
        List<Map<String, String>> filesHeldAsReplicas;
        try {
            ResponseEntity<List<Map<String, String>>> responseEntity = restTemplate.exchange(
                    listHeldReplicasUrl, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});
            filesHeldAsReplicas = responseEntity.getBody();
            if (filesHeldAsReplicas != null) {
                System.out.println("Node '" + nodeName + "': Found " + filesHeldAsReplicas.size() + " files held as replicas on this node.");
            } else {
                System.out.println("Node '" + nodeName + "': No files listed as held replicas by Naming Server.");
                filesHeldAsReplicas = List.of(); // Empty list
            }
        } catch (HttpClientErrorException.NotFound e) {
            System.out.println("Node '" + nodeName + "': Naming Server endpoint not found or no replicas listed for this node at " + listHeldReplicasUrl);
            filesHeldAsReplicas = List.of();
        }
        catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Error fetching list of files held as replicas from Naming Server (" + listHeldReplicasUrl + "): " + e.getMessage());
            return;
        }

        if (filesHeldAsReplicas.isEmpty()) {
            System.out.println("Node '" + nodeName + "': No files held as replicas to transfer.");
            return;
        }

        // 2. Get this node's PREVIOUS node's IP and File Receiver Port from Naming Server.
        //    Endpoint example: GET /api/nodes/ip/{thisNodesIp}/previous-node-contact
        String previousNodeIp = null;
        int previousNodeFilePort = DEFAULT_TARGET_FILE_RECEIVER_PORT; // Default, ideally from NS

        try {
            String prevNodeQueryUrl = namingServerUrl + "/api/nodes/ip/" + ipAddress + "/previous-node-contact"; // NEW Endpoint needed on NS
            Map<String, String> prevNodeInfo = restTemplate.getForObject(prevNodeQueryUrl, Map.class);
            if (prevNodeInfo != null && prevNodeInfo.containsKey("ip")) {
                previousNodeIp = prevNodeInfo.get("ip");
                previousNodeFilePort = Integer.parseInt(prevNodeInfo.getOrDefault("filePort", String.valueOf(DEFAULT_TARGET_FILE_RECEIVER_PORT)));
                System.out.println("Node '" + nodeName + "': Previous node for transfers identified as: " + previousNodeIp + ":" + previousNodeFilePort);
            } else {
                System.err.println("Node '" + nodeName + "': Could not determine previous node from Naming Server via " + prevNodeQueryUrl + ". Cannot transfer replicated files.");
                return;
            }
        } catch (HttpClientErrorException.NotFound e) {
            System.err.println("Node '" + nodeName + "': Previous node endpoint not found or no previous node for this node. Cannot transfer replicated files.");
            return;
        } catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Error fetching previous node details from Naming Server: " + e.getMessage());
            return;
        }

        if (previousNodeIp.equals(this.ipAddress)) {
            System.out.println("Node '" + nodeName + "': Previous node is self. This implies only one node or an issue. No files to transfer.");
            return;
        }

        // 3. For each file this node was replicating, transfer it to the previousNodeIp.
        for (Map<String, String> fileInfo : filesHeldAsReplicas) {
            String fileName = fileInfo.get("fileName");
            // String originalOwnerIp = fileInfo.get("originalOwnerIp"); // Good to have for context

            if (fileName == null || fileName.isEmpty()) continue;

            Path filePath = Paths.get(storageDirectory, fileName);
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                try {
                    System.out.println("Node '" + nodeName + "': Transferring its replica of '" + fileName + "' to previous node: " + previousNodeIp + ":" + previousNodeFilePort);
                    byte[] fileData = Files.readAllBytes(filePath);

                    FileReplicator.transferFile(ipAddress, previousNodeIp, fileName, fileData);

                    // 4. Update Naming Server: The 'previousNodeIp' now holds the replica that this node (ipAddress) used to hold.
                    //    The original owner remains the same. We are just moving the replica location.
                    //    And the previous node "becomes the owner OF THIS REPLICA" (takes responsibility for it).
                    Map<String, String> replicaMovePayload = Map.of(
                            "fileName", fileName,
                            "newReplicaHolderIp", previousNodeIp,    // Previous node now has this replica
                            "oldReplicaHolderIp", ipAddress          // This node no longer has it
                            // "originalOwnerIp", originalOwnerIp   // Could be useful for NS to update its fileLog
                    );
                    String updateReplicaLocationUrl = namingServerUrl + "/api/files/replicas/move"; // NEW Endpoint needed on NS
                    restTemplate.postForObject(updateReplicaLocationUrl, replicaMovePayload, Void.class);
                    System.out.println("Node '" + nodeName + "': Transferred replica '" + fileName + "' to " + previousNodeIp + " and notified Naming Server of replica location change.");

                } catch (IOException e) {
                    System.err.println("Node '" + nodeName + "': IOException during shutdown transfer of replica '" + fileName + "' to " + previousNodeIp + ": " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("Node '" + nodeName + "': Error during shutdown transfer of replica '" + fileName + "' or updating Naming Server: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Node '" + nodeName + "': File '" + fileName + "' listed as a held replica, but not found in local storage (" + filePath.toAbsolutePath() + ") during shutdown. Skipping transfer.");
            }
        }
        System.out.println("Node '" + nodeName + "': Finished attempting to transfer its held replicas on shutdown.");
    }
}