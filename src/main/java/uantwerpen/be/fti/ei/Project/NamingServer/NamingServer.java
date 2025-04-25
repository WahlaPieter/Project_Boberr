package uantwerpen.be.fti.ei.Project.NamingServer;

import java.io.IOException;
import java.util.*;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveStoredFiles;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveToJson;

@Component
public class NamingServer {
    private TreeMap<Integer, Node> nodeMap = new TreeMap<>();
    private Map<String, Set<String>> storedFiles = new HashMap<>();

    @PostConstruct
    public void init() {
        startFailureDetection();
    }
    public void saveNodeMap(){
        saveToJson(nodeMap);
    }

    public void saveFileMap(){
        saveStoredFiles(storedFiles);
    }

    public Map<Integer, Node> getNodeMap() {
        return nodeMap;
    }

    public boolean addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (nodeMap.containsKey(hash)) return false;

        Node newNode = new Node();
        newNode.setNodeName(nodeName);
        newNode.setIpAddress(ipAddress);
        newNode.setCurrentID(hash);
        nodeMap.put(hash, newNode);

        saveNodeMap();
        updateRingPointers();
        storedFiles.put(ipAddress, new HashSet<>());
        redistributeFiles();
        saveFileMap();

        System.out.println("Node added: " + nodeName + " -> " + ipAddress);
        return true;
    }


    private void updateRingPointers() {
        if (nodeMap.isEmpty()) return;

        List<Integer> sortedHashes = new ArrayList<>(nodeMap.keySet());
        Collections.sort(sortedHashes);

        int n = sortedHashes.size();
        for (int i = 0; i < n; i++) {
            int hash  = sortedHashes.get(i);
            int prev  = sortedHashes.get((i - 1 + n) % n);  // wrap‐around
            int next  = sortedHashes.get((i + 1) % n);      // wrap‐around

            Node node  = nodeMap.get(hash);
            node.updatePreviousIDIntern(prev);
            node.updateNextIDIntern(next);
        }
    }

    public boolean removeNode(int hash) {
        Node nodeToRemove = nodeMap.get(hash);
        if (nodeToRemove == null) return false;

        Node prevNode = nodeMap.get(nodeToRemove.getPreviousID());
        Node nextNode = nodeMap.get(nodeToRemove.getNextID());

        if (prevNode != null) {
            prevNode.setNextID(nodeToRemove.getNextID());
            nodeMap.put(prevNode.getCurrentID(), prevNode);
        }

        if (nextNode != null) {
            nextNode.setPreviousID(nodeToRemove.getPreviousID());
            nodeMap.put(nextNode.getCurrentID(), nextNode);
        }

        nodeMap.remove(hash);
        storedFiles.remove(nodeToRemove.getIpAddress());
        saveNodeMap();
        saveFileMap();
        redistributeFiles();

        System.out.println("Node removed: " + hash);
        return true;
    }

    public String getNodeIp(int hash) {
        Node node = nodeMap.get(hash);
        if (node != null){
            return node.getIpAddress();
        }
        else {
            return null;
        }
    }

    public boolean storeFile(String fileName) {
        int fileHash = HashingUtil.generateHash(fileName);
        String responsibleIp = findResponsibleNode(fileHash);
        if (responsibleIp == null) return false;

        try {
            FileStorage.storeFile(responsibleIp, fileName, "File content: " + fileName);
            storedFiles.get(responsibleIp).add(fileName);
            saveFileMap();
            return true;
        } catch (IOException e) {
            System.err.println("Error storing file: " + e.getMessage());
            return false;
        }
    }

    public String findFileLocation(String fileName) {
        int fileHash = HashingUtil.generateHash(fileName);
        String responsibleIp = findResponsibleNode(fileHash);
        if (responsibleIp == null || !storedFiles.get(responsibleIp).contains(fileName)) {
            return null; // File not found
        }
        return responsibleIp;
    }

    // Returns the IP address of the node that is closest to the file's hash value
    private String findResponsibleNode(int fileHash) {
        if (nodeMap.isEmpty()) return null;

        Integer ceilingKey = nodeMap.ceilingKey(fileHash);
        if (ceilingKey != null) {
            return nodeMap.get(ceilingKey).getIpAddress();
        } else {
            // Wrap-around to the first node
            return nodeMap.firstEntry().getValue().getIpAddress();
        }
    }

    private void redistributeFiles() {
        Map<String, Set<String>> filesToMove = new HashMap<>();

        // Collect files that need to be moved
        for (Map.Entry<String, Set<String>> entry : storedFiles.entrySet()) {
            String nodeIp = entry.getKey();
            for (String fileName : entry.getValue()) {
                int fileHash = HashingUtil.generateHash(fileName);
                String newResponsibleIp = findResponsibleNode(fileHash);

                if (!newResponsibleIp.equals(nodeIp)) {
                    filesToMove.computeIfAbsent(nodeIp, k -> new HashSet<>()).add(fileName);
                }
            }
        }

        // Move files between nodes
        for (Map.Entry<String, Set<String>> entry : filesToMove.entrySet()) {
            String sourceIp = entry.getKey();
            for (String fileName : entry.getValue()) {
                int fileHash = HashingUtil.generateHash(fileName);
                String targetIp = findResponsibleNode(fileHash);

                try {
                    FileStorage.moveFile(sourceIp, targetIp, fileName);
                    storedFiles.get(sourceIp).remove(fileName);
                    storedFiles.get(targetIp).add(fileName);
                } catch (IOException e) {
                    System.err.println("Error moving file: " + e.getMessage());
                }
            }
        }
        saveFileMap();
    }

    public void handleNodeFailure(int failedHash, String failedIp) {

        if (!nodeMap.containsKey(failedHash)) {
            System.out.println("node not found in the map: ");
            return;
        }

        // Find the previous and next nodes
        Map.Entry<Integer, Node> previousEntry = nodeMap.lowerEntry(failedHash);
        Map.Entry<Integer, Node> nextEntry = nodeMap.higherEntry(failedHash);

        // if failed node was first or last => the circular ring
        if (previousEntry == null) {
            previousEntry = nodeMap.lastEntry();
        }
        if (nextEntry == null) {
            nextEntry = nodeMap.firstEntry();
        }

        // update neighbours
        if (previousEntry != null && nextEntry != null) {
            Node previousNode = previousEntry.getValue();
            Node nextNode = nextEntry.getValue();

            // Update previous node's next pointer
            previousNode.setNextID(nextEntry.getKey());

            // Update next node's previous pointer
            nextNode.setPreviousID(previousEntry.getKey());

            System.out.println("Updated neighbors: " + previousNode.getIpAddress() +
                    " and " + nextNode.getIpAddress() + " about failure of " + failedIp);
        }

        // the failed node needs to be removed
        nodeMap.remove(failedHash);
        storedFiles.remove(failedIp);
        saveNodeMap();
        saveFileMap();
        System.out.println("Node failure handled for IP: " + failedIp + " (Hash: " + failedHash + ")");

    }

    public boolean pingNode(String ipAddress) {
        try {
            Process process = Runtime.getRuntime().exec("ping -c 1" + ipAddress);
            int returnval = process.waitFor();
            return returnval == 0;
        }catch (Exception e) {
            return false;
        }
    }

    public void startFailureDetection(){
        Thread failureDetectionThread = new Thread(() -> {
            while (true){
                try {
                    // Make a copy to avoid concurrent modification
                    Map<Integer, Node> nodesCopy = new HashMap<>(nodeMap);
                    // this will check each node periodically
                    for (Map.Entry<Integer, Node> entry : nodesCopy.entrySet()) {
                        Node node = entry.getValue();
                        if (!pingNode(node.getIpAddress())) {
                            System.out.println("Node failure detected at IP: " + node.getNodeName());
                            handleNodeFailure(entry.getKey(), node.getIpAddress());
                        }
                    }
                    Thread.sleep(30000); // every 30 sec
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        failureDetectionThread.setDaemon(true);
        failureDetectionThread.start();
    }

}