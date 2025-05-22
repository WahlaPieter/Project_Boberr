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
    private final String storageDirectory; // Absolute path

    // Placeholder: This needs to be the actual listening port of the target's FileReplicator
    // This should ideally be discovered or configured per node.
    private final int DEFAULT_TARGET_FILE_RECEIVER_PORT = 8090; // <<--- EXAMPLE - NEEDS REAL SOLUTION

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
        System.out.println("Node '" + nodeName + "': Handling deletion of local file/replica: " + fileName);
        try {
            // Inform Naming Server that this node (ipAddress) no longer holds a replica/copy of 'fileName'.
            // The Naming Server then needs to update its records. If this node was the owner,
            // the NS might need to trigger deletion from other replicas or assign a new owner.
            // Slide 4: "if deleted, it has to be deleted from the replicated files of the file owner as well."
            // This implies this node tells the NS, and the NS coordinates with the owner.
            String deleteFileRecordUrl = namingServerUrl + "/api/files/" + fileName + "/locations/" + ipAddress; // Hypothetical more precise endpoint
            // Your current endpoint: namingServerUrl + "/api/files/" + fileName + "/replicas/" + ipAddress
            // This seems to mean "remove ipAddress as a replica location for fileName"
            restTemplate.delete(namingServerUrl + "/api/files/" + fileName + "/replicas/" + ipAddress);
            System.out.println("Node '" + nodeName + "': Notified Naming Server of local file/replica deletion for '" + fileName + "'.");
        } catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Error notifying Naming Server of file/replica deletion for '" + fileName + "': " + e.getMessage());
        }
    }


    public void notifyNamingServerOfShutdown() {
        System.out.println("Node '" + nodeName + "' (IP: " + ipAddress + "): Notifying Naming Server about replicas held before shutdown.");

        // 1. Get the list of files this node is currently storing (acting as a replica for).
        // The endpoint "/api/nodes/{hash}/replicated" should return files this node has.
        // Each map should contain at least "fileName".
        String listReplicasUrl = namingServerUrl + "/api/nodes/" + HashingUtil.generateHash(nodeName) + "/replicated";
        List<Map<String, String>> filesHeldAsReplicas;
        try {
            ResponseEntity<List<Map<String, String>>> responseEntity = restTemplate.exchange(
                    listReplicasUrl,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<Map<String, String>>>() {});
            filesHeldAsReplicas = responseEntity.getBody();
            System.out.println("Node '" + nodeName + "': Found " + (filesHeldAsReplicas != null ? filesHeldAsReplicas.size() : 0) + " files held as replicas on this node.");

        } catch (Exception e) {
            System.err.println("Node '" + nodeName + "': Error fetching list of files held as replicas from Naming Server (" + listReplicasUrl + "): " + e.getMessage());
            // If we can't get this list, we can't notify properly.
            // The Naming Server's failure detection will eventually remove the node anyway.
            return;
        }

        if (filesHeldAsReplicas == null || filesHeldAsReplicas.isEmpty()) {
            System.out.println("Node '" + nodeName + "': No files found to be held as replicas on this node. No specific file notifications needed for shutdown.");
            return;
        }

        // 2. For each file, tell the Naming Server that this replica location is going away.
        for (Map<String, String> fileInfo : filesHeldAsReplicas) {
            String fileName = fileInfo.get("fileName");
            if (fileName == null || fileName.isEmpty()) {
                System.err.println("Node '" + nodeName + "': Found a replica entry with no fileName. Skipping.");
                continue;
            }

            // This is the same call as in handleFileDeletion.
            // The Naming Server should understand that if it receives this,
            // and knows the node is shutting down (via the later DELETE /api/nodes/{hash}),
            // it needs to ensure data persistence, possibly by triggering re-replication
            // from the file's actual owner to a new replica node.
            try {
                String deleteReplicaUrl = namingServerUrl + "/api/files/" + fileName + "/replicas/" + ipAddress;
                restTemplate.delete(deleteReplicaUrl);
                System.out.println("Node '" + nodeName + "': Notified Naming Server that replica for '" + fileName + "' (at " + ipAddress + ") is going away.");
            } catch (Exception e) {
                System.err.println("Node '" + nodeName + "': Error notifying Naming Server about replica '" + fileName + "' during shutdown: " + e.getMessage());
            }
        }
        System.out.println("Node '" + nodeName + "': Finished notifying Naming Server about its replicas during shutdown.");
    }
}