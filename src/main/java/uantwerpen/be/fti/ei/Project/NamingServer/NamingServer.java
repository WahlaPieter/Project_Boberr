package uantwerpen.be.fti.ei.Project.NamingServer;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.replication.FileLogEntry;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;
import uantwerpen.be.fti.ei.Project.storage.JsonService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

@Component
@Profile("namingserver")
public class NamingServer {
    @Autowired
    private RestTemplate restTemplate;

    private final TreeMap<Integer, Node> nodeMap;
    private final Map<String, Set<String>> storedFiles;
    private Map<String, FileLogEntry> fileLogs = new HashMap<>();

    public NamingServer() {
        this.nodeMap = JsonService.loadFromJson();
        this.storedFiles = JsonService.loadStoredFiles();
        this.fileLogs = JsonService.loadFileLogs();
    }

    @PostConstruct
    public void init() {
        if (nodeMap.isEmpty()) {
            fileLogs.clear();
            JsonService.saveFileLogs(fileLogs);
            System.out.println("Previous File logs are deleted");
        }
        updateRingPointers();
        redistributeFiles();
        startFailureDetection();


    }

    public synchronized boolean addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (nodeMap.containsKey(hash)) return false;
        Node n = new Node();
        n.setCurrentID(hash);
        n.setNodeName(nodeName);
        n.setIpAddress(ipAddress);
        nodeMap.put(hash, n);
        saveNodeMap();
        updateRingPointers();
        storedFiles.putIfAbsent(ipAddress, new HashSet<>());
        redistributeFiles();
        saveFileMap();
        initiateRedistributionOrReplicationDueToNewNode(n);
        System.out.println("Node added: " + nodeName + " -> " + ipAddress);
        return true;
    }

    public synchronized boolean removeNode(int hash) {
        Node doomed = nodeMap.get(hash);
        if (doomed == null) return false;

        int prevKey = doomed.getPreviousID();
        int nextKey = doomed.getNextID();
        Node prev = nodeMap.get(prevKey);
        Node next = nodeMap.get(nextKey);

        // update pointers
        if (prev != null) prev.setNextID(nextKey);
        if (next != null) next.setPreviousID(prevKey);
        nodeMap.remove(hash);

        // let neighbours know
        RestTemplate rt = new RestTemplate();
        try {
            if (prev != null) {
                rt.postForObject("http://" + prev.getIpAddress() + ":8081/api/bootstrap/update", // Update the previous node with a new next
                        Map.of("updatedField", 2,          // updateNext
                                "nodeID",       nextKey),   // new next
                        Void.class);
            }
            if (next != null) {
                rt.postForObject("http://" + next.getIpAddress() + ":8081/api/bootstrap/update", // Update the next node with a new previous
                        Map.of("updatedField", 1,          // updatePrevious
                                "nodeID",       prevKey),   // new previous
                        Void.class);
            }
        } catch (Exception e) {
            System.err.println("⚠️  neighbour-update failed: " + e.getMessage());
        }


        String ip = doomed.getIpAddress();
        storedFiles.remove(ip);
        saveNodeMap();
        redistributeFiles();
        saveFileMap();
        for (Map.Entry<String, FileLogEntry> entry : fileLogs.entrySet()) {
            String file = entry.getKey();
            FileLogEntry log = entry.getValue();

            // Als de verwijderde node de owner was, wijs nieuwe toe
            if (log.getOwner().equals(ip)) {
                String newOwner = getNextValidOwner(file, ip); // helper nodig
                log.setOwner(newOwner);
                log.addDownloadLocation(newOwner);
            }

            // Verwijder als downloadLocation
            log.removeDownloadLocation(ip);
        }
        JsonService.saveFileLogs(fileLogs);

        System.out.println("File logs updated after shutdown of " + ip + ":");
        fileLogs.forEach((file, log) -> {
            System.out.println("  - " + file + " → owner: " + log.getOwner() + ", downloads: " + log.getDownloadLocations());
        });

        System.out.println("Node removed: " + hash);
        return true;
    }

    public synchronized boolean storeFile(String fileName) {
        int fileHash = HashingUtil.generateHash(fileName);
        String ip = findResponsibleNode(fileHash);
        if (ip == null) return false;
        try {
            FileStorage.storeFile(ip, fileName, "Content: " + fileName);
            storedFiles.get(ip).add(fileName);
            fileLogs.putIfAbsent(fileName, new FileLogEntry(ip));
            fileLogs.get(fileName).addDownloadLocation(ip);
            JsonService.saveFileLogs(fileLogs);
            saveFileMap();

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public synchronized String findFileLocation(String fileName) {
        int fileHash = HashingUtil.generateHash(fileName);
        String ip = findResponsibleNode(fileHash);
        if (ip == null || !storedFiles.getOrDefault(ip, Set.of()).contains(fileName)) return null;
        return ip;
    }

    private String findResponsibleNode(int hash) {
        if (nodeMap.isEmpty()) return null;
        Integer key = nodeMap.ceilingKey(hash);
        if (key == null) key = nodeMap.firstKey();
        return nodeMap.get(key).getIpAddress();
    }

    private void updateRingPointers() {
        if (nodeMap.isEmpty()) return;
        List<Integer> keys = new ArrayList<>(nodeMap.keySet());
        Collections.sort(keys);
        int n = keys.size();
        for (int i = 0; i < n; i++) {
            int id = keys.get(i);
            int prev = keys.get((i - 1 + n) % n);
            int next = keys.get((i + 1) % n);
            Node node = nodeMap.get(id);
            node.setPreviousID(prev);
            node.setNextID(next);
        }
    }

    private void redistributeFiles() {
        Map<String, Set<String>> toMove = new HashMap<>();

        for (var entry : storedFiles.entrySet()) {
            String ip = entry.getKey();
            for (String f : entry.getValue()) {
                int h = HashingUtil.generateHash(f);
                String target = findResponsibleNode(h);
                if (!Objects.equals(target, ip)) {
                    toMove.computeIfAbsent(ip, k -> new HashSet<>()).add(f);
                }
            }
        }

        toMove.forEach((src, files) -> {
            for (String f : files) {
                String dst = findResponsibleNode(HashingUtil.generateHash(f));
                Path sourcePath = Paths.get("nodes_storage/" + src + "/" + f);
                Path targetPath = Paths.get("nodes_storage/" + dst + "/" + f);

                if (Files.exists(sourcePath)) {
                    try {
                        Files.createDirectories(targetPath.getParent());
                        Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

                        storedFiles.get(src).remove(f);
                        storedFiles.computeIfAbsent(dst, k -> new HashSet<>()).add(f);

                        fileLogs.putIfAbsent(f, new FileLogEntry(dst));
                        fileLogs.get(f).removeDownloadLocation(src);
                        fileLogs.get(f).setOwner(dst);
                        fileLogs.get(f).addDownloadLocation(dst);
                        JsonService.saveFileLogs(fileLogs);

                        System.out.println("Files redistributed: " + f + " van " + src + " → " + dst);
                    } catch (IOException e) {
                        System.err.println("Error redistribution of files: " + f);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println(" File not found for redistribution: " + sourcePath);
                }
            }
        });

        saveFileMap();
    }

    private synchronized void initiateRedistributionOrReplicationDueToNewNode(Node newNodeJustAdded) {
        System.out.println("NamingServer: Re-evaluating file ownership and replication due to new node " + newNodeJustAdded.getIpAddress());

        // First, ensure primary ownerships are correct (your existing redistributeFiles should handle this part)
        redistributeFiles(); // This moves files to their new *primary* owner if necessary.

        // After primary ownership is settled, now check for creating/updating replicas.
        // For every file that is currently "owned", see if it needs to be replicated to its designated replica node.
        for (String fileName : new HashSet<>(fileLogs.keySet())) { // Iterate over a copy of known files
            FileLogEntry log = fileLogs.get(fileName);
            if (log == null || log.getOwner() == null || log.getOwner().isEmpty()) {
                System.out.println("NamingServer: Skipping file '" + fileName + "' for re-replication check (no owner in log).");
                continue;
            }

            String currentOwnerIp = log.getOwner();
            Node currentOwnerNode = getNodeByIp(currentOwnerIp); // Assumes Node object has httpPort

            if (currentOwnerNode == null) {
                System.err.println("NamingServer: Owner node " + currentOwnerIp + " for file '" + fileName + "' not found in map. Cannot instruct replication.");
                continue;
            }

            int fileHash = HashingUtil.generateHash(fileName);
            // Ask where a replica of this file should go NOW, with the new node in the ring.
            Map<String, Object> replicaTargetInfo = getNodeForReplication(fileHash);

            if (replicaTargetInfo != null && replicaTargetInfo.containsKey("ip")) {
                String designatedReplicaIp = (String) replicaTargetInfo.get("ip");
                // int designatedReplicaFilePort = ((Number) replicaTargetInfo.get("filePort")).intValue(); // We won't pass this

                // If the designated replica location is NOT the owner itself,
                // AND this replica doesn't already exist at that location according to our logs:
                if (!designatedReplicaIp.equals(currentOwnerIp) && !log.getDownloadLocations().contains(designatedReplicaIp)) {
                    System.out.println("NamingServer: File '" + fileName + "' (owner: " + currentOwnerIp +
                            ") needs new/updated replica at " + designatedReplicaIp +
                            ". Instructing owner to re-evaluate replication for this file.");
                    try {
                        // Instruct currentOwnerNode to re-run its replication logic for this specific file.
                        // The owner node, when it runs its replicateSingleFileOrUpdate, will again ask the NS
                        // where to send it, and the NS should now give the correct new target.
                        String triggerReplicationUrl = "http://" + currentOwnerIp + ":8081/api/bootstrap/files/ensure-replication"; // NEW ENDPOINT on Node
                        Map<String, String> payload = Map.of("fileName", fileName);
                        restTemplate.postForObject(triggerReplicationUrl, payload, String.class);
                    } catch (Exception e) {
                        System.err.println("NamingServer: Failed to instruct owner " + currentOwnerIp +
                                " to ensure replication for " + fileName + " to " + designatedReplicaIp +
                                ": " + e.getMessage());
                    }
                } else if (designatedReplicaIp.equals(currentOwnerIp)) {
                    System.out.println("NamingServer: For file '" + fileName + "', owner " + currentOwnerIp + " is also the designated replica target (e.g. only 2 nodes). No further action needed from NS.");
                } else if (log.getDownloadLocations().contains(designatedReplicaIp)) {
                    System.out.println("NamingServer: For file '" + fileName + "', replica already exists at designated target " + designatedReplicaIp + ".");
                }
            } else {
                System.out.println("NamingServer: For file '" + fileName + "' (owner: " + currentOwnerIp + "), no distinct replica target found after new node addition.");
            }
        }
    }

    public synchronized void handleNodeFailure(int failedHash, String failedIp) {
        if (!nodeMap.containsKey(failedHash)) return;
        Map.Entry<Integer, Node> prev = nodeMap.lowerEntry(failedHash);
        Map.Entry<Integer, Node> next = nodeMap.higherEntry(failedHash);
        if (prev == null) prev = nodeMap.lastEntry();
        if (next == null) next = nodeMap.firstEntry();
        prev.getValue().setNextID(next.getKey());
        next.getValue().setPreviousID(prev.getKey());
        nodeMap.remove(failedHash);
        storedFiles.remove(failedIp);
        RestTemplate rt = new RestTemplate();
        try {
            rt.postForObject("http://" + prev.getValue().getIpAddress() + ":8081/api/bootstrap/update",
                    Map.of("updatedField", 2, "nodeID", next.getKey()), Void.class);
            rt.postForObject("http://" + next.getValue().getIpAddress() + ":8081/api/bootstrap/update",
                    Map.of("updatedField", 1, "nodeID", prev.getKey()), Void.class);
        } catch (Exception ignore) {}
        saveNodeMap();
        saveFileMap();
        System.out.println("Handled failure of " + failedIp);
    }

    private void startFailureDetection() {
        Thread t = new Thread(() -> {
            while (true) {
                new HashMap<>(nodeMap).forEach((hash, node) -> {
                    if (!isAlive(node.getIpAddress())) {
                        System.out.println("Node failure detected: " + node.getIpAddress());
                        handleNodeFailure(hash, node.getIpAddress());
                    }
                });
                try { Thread.sleep(30000); } catch (InterruptedException ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private boolean isAlive(String ip) {
        try {
            restTemplate.getForEntity("http://" + ip + ":8081/actuator/health", String.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Persistence helpers
    public Map<Integer, Node> getNodeMap() { return nodeMap; }
    public void saveNodeMap() { JsonService.saveToJson(nodeMap); }
    public void saveFileMap() { JsonService.saveStoredFiles(storedFiles); }

    // In NamingServer.java
    public synchronized Map<String, Object> getNodeForReplication(int fileHash) { // Renamed and to return port
        if (nodeMap.isEmpty()) {
            System.out.println("NamingServer.getNodeAndPortForReplication: Node map is empty.");
            return null;
        }

        // Determine the node primarily responsible for this fileHash (the "owner")
        Map.Entry<Integer, Node> ownerEntry = nodeMap.ceilingEntry(fileHash);
        if (ownerEntry == null) {
            ownerEntry = nodeMap.firstEntry();
        }
        Node ownerNode = ownerEntry.getValue();
        System.out.println("NamingServer.getNodeAndPortForReplication: For file hash " + fileHash + ", determined owner is " + ownerNode.getIpAddress() + " (ID: " + ownerNode.getCurrentID() + ")");

        if (nodeMap.size() == 1) {
            System.out.println("NamingServer.getNodeAndPortForReplication: Only one node in system (" + ownerNode.getIpAddress() + "). Target for replica is self (no external replication).");
            Map<String, Object> selfTarget = new HashMap<>();
            selfTarget.put("ip", ownerNode.getIpAddress());
            return selfTarget; // Requesting node should check if target IP is its own
        }

        // If more than one node, replicate to the owner's NEXT distinct node
        Node replicaTargetNode = nodeMap.get(ownerNode.getNextID());
        int attempts = 0;
        while (replicaTargetNode == null || replicaTargetNode.getIpAddress().equals(ownerNode.getIpAddress())) {
            if (replicaTargetNode == null) { // Should not happen if pointers are correct
                System.err.println("NamingServer.getNodeAndPortForReplication: Owner " + ownerNode.getIpAddress() + "'s nextID (" + ownerNode.getNextID() + ") does not exist in map! Map: " + nodeMap.keySet());
                return null; // Critical error in ring consistency
            }
            System.out.println("NamingServer.getNodeAndPortForReplication: Initial replica target " + replicaTargetNode.getIpAddress() + " is same as owner " + ownerNode.getIpAddress() + ". Finding next distinct.");
            replicaTargetNode = nodeMap.get(replicaTargetNode.getNextID()); // Try next's next
            attempts++;
            if (attempts >= nodeMap.size()) { // Prevent infinite loop
                System.err.println("NamingServer.getNodeAndPortForReplication: Could not find a distinct replica target for file hash " + fileHash + " (owner: " + ownerNode.getIpAddress() + "). All nodes might be the owner or ring is broken.");
                return null;
            }
        }

        System.out.println("NamingServer.getNodeAndPortForReplication: For file hash " + fileHash + " (owner: " + ownerNode.getIpAddress() + "), replication target is " + replicaTargetNode.getIpAddress() + " (ID: " + replicaTargetNode.getCurrentID() + ")");
        Map<String, Object> targetInfo = new HashMap<>();
        targetInfo.put("ip", replicaTargetNode.getIpAddress());
        return targetInfo;
    }

    public synchronized void registerFileReplication(String fileName, String ownerIp, String replicaIp) {
        storedFiles.computeIfAbsent(ownerIp, k -> new HashSet<>()).add(fileName);

        if (!ownerIp.equals(replicaIp)) {
            storedFiles.computeIfAbsent(replicaIp, k -> new HashSet<>()).add(fileName);
        }
        fileLogs.putIfAbsent(fileName, new FileLogEntry(ownerIp));
        fileLogs.get(fileName).addDownloadLocation(replicaIp);
        JsonService.saveFileLogs(fileLogs);

        saveFileMap();
    }

    public synchronized List<Map<String, String>> getFilesHeldAsReplicasByNode(String shuttingDownNodeIp) {
        List<Map<String, String>> heldReplicas = new ArrayList<>();
        if (shuttingDownNodeIp == null) return heldReplicas;

        for (Map.Entry<String, FileLogEntry> logEntry : fileLogs.entrySet()) {
            String fileName = logEntry.getKey();
            FileLogEntry fileLog = logEntry.getValue();
            if (fileLog.getDownloadLocations().contains(shuttingDownNodeIp) &&
                    !Objects.equals(fileLog.getOwner(), shuttingDownNodeIp)) {
                Map<String, String> fileInfo = new HashMap<>();
                fileInfo.put("fileName", fileName);
                fileInfo.put("originalOwnerIp", fileLog.getOwner());
                heldReplicas.add(fileInfo);
            }
        }
        return heldReplicas;
    }

    public synchronized Map<String, String> getPreviousNodeContact(String currentNodeIp) {
        Node currentNode = getNodeByIp(currentNodeIp);
        if (currentNode == null) {
            System.err.println("NamingServer.getPreviousNodeContact: Current node with IP " + currentNodeIp + " not found in map.");
            return null;
        }
        if (nodeMap.size() < 2) {
            System.out.println("NamingServer.getPreviousNodeContact: Not enough nodes for a distinct previous node for " + currentNodeIp);
            return null; // No other node to be previous
        }

        Node previousNode = nodeMap.get(currentNode.getPreviousID());

        // If previousID points to self (e.g. in a 2-node ring where it's also the next of the other)
        // or if the found previousNode is somehow the currentNode itself (shouldn't happen if PIDs are correct)
        if (previousNode == null || previousNode.getIpAddress().equals(currentNodeIp)) {
            // Try to find any *other* node if this is a 2-node scenario or PID is self
            if (nodeMap.size() == 2) {
                for (Node n : nodeMap.values()) {
                    if (!n.getIpAddress().equals(currentNodeIp)) {
                        previousNode = n; // The other node is the previous
                        break;
                    }
                }
            } else {
                // More than 2 nodes, but previousID is self or previousNode not found by ID. This indicates a ring inconsistency.
                System.err.println("NamingServer.getPreviousNodeContact: Previous node for " + currentNodeIp + " (prevID: " + currentNode.getPreviousID() +") is self or not found in a ring > 2 nodes. Ring data: " + nodeMap);
                return null; // Cannot reliably determine distinct previous
            }
        }

        if (previousNode == null) { // Should be caught above if size is 2 and still no other node
            System.err.println("NamingServer.getPreviousNodeContact: Could not resolve a distinct previous node for " + currentNodeIp);
            return null;
        }

        Map<String, String> contactInfo = new HashMap<>();
        contactInfo.put("ip", previousNode.getIpAddress());
        return contactInfo;
    }


    // To be called by POST /api/files/replicas/move
    public synchronized void moveReplicaLocation(String fileName, String newReplicaHolderIp, String oldReplicaHolderIp) {
        System.out.println("NamingServer: Moving replica location for '" + fileName + "' from " + oldReplicaHolderIp + " to " + newReplicaHolderIp);
        FileLogEntry log = fileLogs.get(fileName);
        if (log != null) {
            log.removeDownloadLocation(oldReplicaHolderIp);
            log.addDownloadLocation(newReplicaHolderIp);
            JsonService.saveFileLogs(fileLogs); // Persist changes
            System.out.println("NamingServer: Updated fileLog for '" + fileName + "'. New locations: " + log.getDownloadLocations());
        } else {
            System.err.println("NamingServer: No fileLog found for '" + fileName + "' during replica move. Cannot update.");
        }

        // Update storedFiles map as well
        Set<String> filesOnOldHolder = storedFiles.get(oldReplicaHolderIp);
        if (filesOnOldHolder != null) {
            filesOnOldHolder.remove(fileName);
        }
        storedFiles.computeIfAbsent(newReplicaHolderIp, k -> new HashSet<>()).add(fileName);
        saveFileMap(); // Persist changes
    }

    public synchronized void removeFileReplica(String fileName, String replicaIpAddressThatDeleted) {
        System.out.println("NamingServer: Received request to remove replica of '" + fileName + "' from node: " + replicaIpAddressThatDeleted);

        // 1. Update storedFiles: Remove fileName from the set for replicaIpAddressThatDeleted
        boolean replicaRecordRemoved = false;
        Set<String> filesOnDeletingNode = storedFiles.get(replicaIpAddressThatDeleted);
        if (filesOnDeletingNode != null) {
            if (filesOnDeletingNode.remove(fileName)) {
                System.out.println("NamingServer: Removed '" + fileName + "' from storedFiles record of node: " + replicaIpAddressThatDeleted);
                replicaRecordRemoved = true;
                if (filesOnDeletingNode.isEmpty()) {
                    // storedFiles.remove(replicaIpAddressThatDeleted); // Optional: remove entry if set is empty
                }
            }
        }

        // 2. Update fileLogs: Remove replicaIpAddressThatDeleted from downloadLocations
        FileLogEntry log = fileLogs.get(fileName);
        if (log != null) {
            log.removeDownloadLocation(replicaIpAddressThatDeleted);
            System.out.println("NamingServer: Removed '" + replicaIpAddressThatDeleted + "' as download location for '" + fileName + "'. Current locations: " + log.getDownloadLocations());
        } else {
            System.out.println("NamingServer: No fileLog entry found for '" + fileName + "' during replica removal. This might be okay if it was never fully registered.");
            // If no log, perhaps it wasn't considered owned/replicated officially.
            if (replicaRecordRemoved) saveFileMap(); // Still save if storedFiles was updated
            return; // Can't proceed to notify owner if no log entry
        }

        // 3. Determine the OWNER from the fileLog
        String ownerIp = log.getOwner();

        // 4. Action based on who deleted and who is the owner
        if (ownerIp == null || ownerIp.isEmpty()) {
            System.err.println("NamingServer: FileLog for '" + fileName + "' has no owner! Cannot process replica deletion notification further.");
            JsonService.saveFileLogs(fileLogs); // Save changes to download locations
            if (replicaRecordRemoved) saveFileMap();
            return;
        }

        if (ownerIp.equals(replicaIpAddressThatDeleted)) {
            // The OWNER itself deleted its primary copy.
            System.out.println("NamingServer: Owner node '" + ownerIp + "' deleted its primary copy of '" + fileName + "'.");
            // According to the PDF, "if deleted, it has to be deleted from the replicated files of the file owner as well."
            // Since the owner deleted it, we should now instruct ALL OTHER download locations (replicas) to delete their copies.
            Set<String> remainingDownloadLocations = new HashSet<>(log.getDownloadLocations()); // Iterate over a copy
            if (!remainingDownloadLocations.isEmpty()) {
                System.out.println("NamingServer: Instructing remaining replicas of '" + fileName + "' to delete their copies: " + remainingDownloadLocations);
            }
            for (String otherReplicaIp : remainingDownloadLocations) {
                if (!otherReplicaIp.equals(ownerIp)) { // Should already be true as owner deleted its copy from log
                    notifyNodeToDeleteLocalCopy(fileName, otherReplicaIp, "owner deleted primary copy");
                    log.removeDownloadLocation(otherReplicaIp); // Update log as we instruct them
                    Set<String> filesOnOtherReplica = storedFiles.get(otherReplicaIp);
                    if (filesOnOtherReplica != null) {
                        filesOnOtherReplica.remove(fileName);
                    }
                }
            }
            // After owner deletes and all replicas are told to delete, the file is effectively gone from the system.
            // We can remove the fileLog entry itself.
            fileLogs.remove(fileName);
            System.out.println("NamingServer: File '" + fileName + "' and its log entry removed from system as owner deleted it.");

        } else {
            // A replica node (not the owner) deleted its copy.
            // The PDF says: "deleted from the replicated files OF THE FILE OWNER as well."
            // This means the owner should also delete its copy if one of its replicas is gone.
            // This is a strong interpretation ensuring the file disappears if its replication chain breaks.
            System.out.println("NamingServer: Replica node '" + replicaIpAddressThatDeleted + "' deleted its copy of '" + fileName + "'. Owner is '" + ownerIp + "'.");
            System.out.println("NamingServer: Instructing owner '" + ownerIp + "' to delete its primary copy of '" + fileName + "' because a replica was lost.");
            notifyNodeToDeleteLocalCopy(fileName, ownerIp, "a replica was deleted");
            // If owner successfully deletes, it will trigger the above "owner deleted" path for its other replicas.
            // For now, just remove its owner status and the file from logs to signify it's gone.
            // This might lead to the file "disappearing" from the system if the owner is the only one left.
            log.removeDownloadLocation(ownerIp); // Owner also loses its "download" status for this file
            Set<String> filesOnOwner = storedFiles.get(ownerIp);
            if(filesOnOwner != null) filesOnOwner.remove(fileName);

            // If the owner deletes, all other replicas should also be deleted.
            // This is getting complex, let's simplify: if a replica is deleted, we tell the owner.
            // The owner deleting it will then cascade to other replicas if that's the desired logic.
            // The PDF is a bit ambiguous here. Let's stick to: replica tells NS, NS tells owner.
        }

        JsonService.saveFileLogs(fileLogs);
        if (replicaRecordRemoved) saveFileMap();
    }

    private void notifyNodeToDeleteLocalCopy(String fileName, String targetNodeIp, String reason) {
        try {
            Node targetNode = getNodeByIp(targetNodeIp); // You need to ensure Node objects in map have httpPort
            if (targetNode != null) { // Assuming httpPort 0 is invalid/unknown
                Map<String, String> deletePayload = Map.of("fileName", fileName);
                String targetDeleteUrl = "http://" + targetNodeIp + ":8081/api/bootstrap/files/delete-local-copy"; // Corrected path

                System.out.println("NamingServer: Instructing node " + targetNodeIp +
                        " to delete local copy of '" + fileName + "' because " + reason + ". URL: " + targetDeleteUrl);
                restTemplate.postForObject(targetDeleteUrl, deletePayload, String.class);
            } else {
                System.err.println("NamingServer: Could not find target node details (or valid port) for IP: " + targetNodeIp + " to send delete instruction for '" + fileName + "'.");
            }
        } catch (Exception e) {
            System.err.println("NamingServer: Failed to instruct node " + targetNodeIp + " to delete file '" + fileName + "': " + e.getMessage());
            // Do not re-throw here to let the rest of removeFileReplica complete.
            // The error is logged.
        }
    }

    private synchronized Node getNodeByIp(String ipAddress) {
        for (Node node : nodeMap.values()) {
            if (node.getIpAddress().equals(ipAddress)) {
                return node; // Assuming Node object in map has getHttpPort()
            }
        }
        return null;
    }

    public List<Map<String, String>> getReplicatedFilesForNode(int hash) {
        List<Map<String, String>> replicatedFiles = new ArrayList<>();

        Node node = nodeMap.get(hash);
        if (node == null) return replicatedFiles;

        String ip = node.getIpAddress();
        Set<String> files = storedFiles.getOrDefault(ip, Set.of());

        for (String file : files) {
            replicatedFiles.add(Map.of(
                    "fileName", file,
                    "currentOwner", ip
            ));
        }

        return replicatedFiles;
    }

    public Map<String, FileLogEntry> getFileLogs() {
        return fileLogs;
    }

    private String getNextValidOwner(String file, String excludedIp) {
        int hash = HashingUtil.generateHash(file);

        for (Map.Entry<Integer, Node> entry : nodeMap.tailMap(hash, true).entrySet()) {
            String ip = entry.getValue().getIpAddress();
            if (!ip.equals(excludedIp)) {
                return ip;
            }
        }

        // fallback: first node that is available  ≠ excluded
        for (Node node : nodeMap.values()) {
            String ip = node.getIpAddress();
            if (!ip.equals(excludedIp)) {
                return ip;
            }
        }

        return null;
    }

    public Map<String,List<String>> getFilesOfNode(int hash) {
        Node n = nodeMap.get(hash);
        if (n == null) return Map.of("local", List.of(), "replicas", List.of());

        String ip = n.getIpAddress();

        List<String> local    = new ArrayList<>(storedFiles.getOrDefault(ip, Set.of()));
        List<String> replicas = new ArrayList<>();

        storedFiles.forEach((ownerIp, set) -> {
            if (!ownerIp.equals(ip)) {
                set.stream()
                        .filter(f -> storedFiles.getOrDefault(ip, Set.of()).contains(f))
                        .forEach(replicas::add);
            }
        });
        return Map.of("local", local, "replicas", replicas);
    }

}