package uantwerpen.be.fti.ei.Project.NamingServer;

import java.io.IOException;
import java.util.*;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;
import jakarta.annotation.PostConstruct;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastReceiver;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveStoredFiles;
import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveToJson;

@Component
public class NamingServer {
    private TreeMap<Integer, String> nodeMap = new TreeMap<>();
    private Map<String, Set<String>> storedFiles = new HashMap<>();


    public void saveNodeMap(){
        saveToJson(nodeMap);
    }

    public void saveFileMap(){
        saveStoredFiles(storedFiles);
    }

    public Map<Integer, String> getNodeMap() {
        return nodeMap;
    }

    // When adding a node, we add it and then reassign files based on the new hash ring
    public boolean addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (!nodeMap.containsKey(hash)) {
            nodeMap.put(hash, ipAddress);
            saveNodeMap();
            // Initialize file storage for the new node
            storedFiles.put(ipAddress, new HashSet<>());

            // Reassign all files according to the new ring
            redistributeFiles();
            saveFileMap();

            System.out.println("Node added: " + nodeName + " -> " + ipAddress);
            return true;
        } else {
            System.out.println("Node already exists!");
            return false;
        }
    }

    public boolean removeNode(String nodeName) {
        int hash = HashingUtil.generateHash(nodeName);
        String ip = nodeMap.get(hash);
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

        String bestNodeIp = null;
        int minDifference = Integer.MAX_VALUE;

        // Iterate over all nodes in the hash ring
        for (Map.Entry<Integer, String> entry : nodeMap.entrySet()) {
            int nodeHash = entry.getKey();
            int difference = fileHash - nodeHash;

            // We select the node with the smallest non-negative difference
            if (difference >= 0 && difference < minDifference) {
                minDifference = difference;
                bestNodeIp = entry.getValue();
            }
        }

        // If no node has a hash less than or equal to the file hash, wrap-around:
        if (bestNodeIp == null) {
            return nodeMap.lastEntry().getValue();
        }
        return bestNodeIp;
    }

    // Reassign all files over the updated ring. For each file, determine its new responsible node,
    // and then place it there while ensuring it is removed from any previous node.
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