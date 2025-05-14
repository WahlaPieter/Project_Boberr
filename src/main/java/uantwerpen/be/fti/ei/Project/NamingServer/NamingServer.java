package uantwerpen.be.fti.ei.Project.NamingServer;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;
import uantwerpen.be.fti.ei.Project.storage.JsonService;

import java.io.IOException;
import java.util.*;

@Component
@Profile("namingserver")
public class NamingServer {
    private final TreeMap<Integer, Node> nodeMap;
    private final Map<String, Set<String>> storedFiles;

    public NamingServer() {
        this.nodeMap = JsonService.loadFromJson();
        this.storedFiles = JsonService.loadStoredFiles();
    }

    @PostConstruct
    public void init() {
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
        System.out.println("Node added: " + nodeName + " -> " + ipAddress);
        return true;
    }

    public synchronized boolean removeNode(int hash) {
        Node node = nodeMap.get(hash);
        if (node == null) return false;
        Node prev = nodeMap.get(node.getPreviousID());
        Node next = nodeMap.get(node.getNextID());
        if (prev != null) prev.setNextID(node.getNextID());
        if (next != null) next.setPreviousID(node.getPreviousID());
        nodeMap.remove(hash);
        storedFiles.remove(node.getIpAddress());
        saveNodeMap();
        saveFileMap();
        redistributeFiles();
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
                try {
                    FileStorage.moveFile(src, findResponsibleNode(HashingUtil.generateHash(f)), f);
                    storedFiles.get(src).remove(f);
                    storedFiles.get(findResponsibleNode(HashingUtil.generateHash(f))).add(f);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        saveFileMap();
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
        saveNodeMap();
        saveFileMap();
        System.out.println("Handled failure of " + failedIp);
    }

    private void startFailureDetection() {
        Thread t = new Thread(() -> {
            while (true) {
                for (var e : new HashMap<>(nodeMap).entrySet()) {
                    String ip = e.getValue().getIpAddress();
                    if (!ping(ip)) handleNodeFailure(e.getKey(), ip);
                }
                try { Thread.sleep(30000); } catch (InterruptedException ignored) { }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private boolean ping(String ip) {
        try {
            Process p = Runtime.getRuntime().exec("ping -c 1 " + ip);
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    // Persistence helpers
    public Map<Integer, Node> getNodeMap() { return nodeMap; }
    public void saveNodeMap() { JsonService.saveToJson(nodeMap); }
    public void saveFileMap() { JsonService.saveStoredFiles(storedFiles); }

    public synchronized String getNodeForReplication(int hash) {
        if (nodeMap.isEmpty()) return null;

        // Find the node where node.hash < file.hash
        Map.Entry<Integer, Node> lower = nodeMap.lowerEntry(hash);
        if (lower == null) {
            // Wrap around to the highest node
            lower = nodeMap.lastEntry();
        }

        // Check if this node would be the owner (edge case)
        if (lower.getKey() == hash) {
            return null; // File hash matches node hash - no replication needed
        }

        return lower.getValue().getIpAddress();
    }

    public synchronized void registerFileReplication(String fileName, String ownerIp, String replicaIp) {
        storedFiles.computeIfAbsent(ownerIp, k -> new HashSet<>()).add(fileName);
        saveFileMap();
    }

    public synchronized void removeFileReplica(String fileName, String replicaIp) {
        storedFiles.entrySet().stream()
                .filter(e -> e.getValue().contains(fileName))
                .findFirst()
                .ifPresent(entry -> {
                    entry.getValue().remove(fileName);
                    saveFileMap();
                });
    }

}