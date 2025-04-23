package uantwerpen.be.fti.ei.Project.NamingServer;

import java.io.IOException;
import java.util.*;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveStoredFiles;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveToJson;

@Component
public class NamingServer {
    private TreeMap<Integer, Node> nodeMap = new TreeMap<>();
    private Map<String, Set<String>> storedFiles = new HashMap<>();


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



    public boolean removeNode(String nodeName) {
        int hash = HashingUtil.generateHash(nodeName);
        String ip = nodeMap.get(hash).getIpAddress();
        if (nodeMap.containsKey(hash)) {
            nodeMap.remove(hash);
            storedFiles.remove(ip);
            saveNodeMap();
            saveFileMap();
            System.out.println("Node deleted: " + nodeName);
            return true;
        } else {
            System.out.println("Node not found!");
            return false;
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
}