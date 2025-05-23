package uantwerpen.be.fti.ei.Project.NamingServer;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
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
                String dst = findResponsibleNode(HashingUtil.generateHash(f));
                Path sourcePath = Paths.get("nodes_storage/" + src + "/" + f);
                Path targetPath = Paths.get("nodes_storage/" + dst + "/" + f);

                if (Files.exists(sourcePath)) {
                    try {
                        Files.createDirectories(targetPath.getParent());
                        Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

                        storedFiles.get(src).remove(f);
                        storedFiles.computeIfAbsent(dst, k -> new HashSet<>()).add(f);

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

    public synchronized String getNodeForReplication(int hash) {
        if (nodeMap.isEmpty() || nodeMap.size() == 1) {
            return null;
        }

        // find owner of the file
        Map.Entry<Integer, Node> ownerEntry = nodeMap.ceilingKey(hash) != null
                ? nodeMap.ceilingEntry(hash)
                : nodeMap.firstEntry();

        Node owner = ownerEntry.getValue();
        Node replica = nodeMap.get(owner.getNextID());

        // try new node for replication
        int attempts = 0;
        while (replica.getIpAddress().equals(owner.getIpAddress())) {
            replica = nodeMap.get(replica.getNextID());
            attempts++;

            if (attempts >= nodeMap.size()) {
                System.out.println("No fitting replica: everyone is owner");
                return null;
            }
        }

        return replica.getIpAddress();
    }

    public synchronized void registerFileReplication(String fileName, String ownerIp, String replicaIp) {
        storedFiles.computeIfAbsent(ownerIp, k -> new HashSet<>()).add(fileName);

        if (!ownerIp.equals(replicaIp)) {
            storedFiles.computeIfAbsent(replicaIp, k -> new HashSet<>()).add(fileName);
        }
        saveFileMap();
    }

    public synchronized void removeFileReplica(String fileName) {
        storedFiles.entrySet().stream()
                .filter(e -> e.getValue().contains(fileName))
                .findFirst()
                .ifPresent(entry -> {
                    entry.getValue().remove(fileName);
                    saveFileMap();
                });
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