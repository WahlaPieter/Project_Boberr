package uantwerpen.be.fti.ei.Project.Bootstrap;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.Agents.FailAgent;
import uantwerpen.be.fti.ei.Project.Agents.FailureMonitor;
import uantwerpen.be.fti.ei.Project.Agents.FileEntry;
import uantwerpen.be.fti.ei.Project.Agents.SyncAgent;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastReceiver;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastSender;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.replication.FileWatcher;
import uantwerpen.be.fti.ei.Project.replication.ReplicationManager;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;


import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.util.HashMap;

@Component
@Profile("node")
public class Node {

    private int currentID;
    private int previousID;
    private int nextID;

    private String nodeName;
    private String ipAddress;

    private transient ReplicationManager replicationManager;   // lab 5
    private transient FileWatcher        fileWatcher;          // lab 5
    private transient CompletableFuture<Integer> nodeCountFuture = new CompletableFuture<>();

    @Autowired
    private transient RestTemplate rest;


    // Toegevoegd voor synchronisatie
    private Map<String, FileEntry> localFileList = new HashMap<>();

    public Map<String, FileEntry> getLocalFileList() {
        return localFileList;
    }

    public void updateFileListFromDisk() {
        String path = "nodes_storage/" + ipAddress;
        File folder = new File(path);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));

        if (files != null) {
            for (File file : files) {
                String filename = file.getName().replace(".txt", "");
                localFileList.putIfAbsent(filename, new FileEntry(filename, false, ipAddress));
            }
        }
    }

    /* ------------- config ------------- */
    @Value("${storage.path}")
    private String storagePath;

    @Value("${namingserver.url}")
    private String namingServerUrl;

    @PostConstruct
    public void init() {
        try {
            this.ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            this.ipAddress = "127.0.0.1";
        }
        this.nodeName = "Node-" + ThreadLocalRandom.current().nextInt(1, 10000);
        this.currentID = HashingUtil.generateHash(nodeName);
        this.previousID = currentID;
        this.nextID = currentID;

        // Create storage directory
        Path storagePath = Paths.get("storage").toAbsolutePath();
        try {
            if (!Files.exists(storagePath)) {
                Files.createDirectories(storagePath);
                System.out.println("Created storage directory at: " + storagePath);
            } else {
                System.out.println("Using existing storage directory at: " + storagePath);
            }
        } catch (IOException e) {
            System.err.println("Failed to create storage directory: " + e.getMessage());
            throw new RuntimeException("Storage directory initialization failed", e);
        }


        this.replicationManager = new ReplicationManager(
                nodeName, ipAddress, namingServerUrl, rest, storagePath.toString());
        this.fileWatcher = new FileWatcher(storagePath.toString(), replicationManager);

        FileReplicator.startFileReceiver(8082, storagePath.toString());

        System.out.println("Node started: " + nodeName + " (ID: " + currentID + ")");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        // Start multicast listener
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();

        // Start file watcher
        Thread watcherThread = new Thread(fileWatcher);
        watcherThread.setDaemon(true);
        watcherThread.start();

        // Start a separate thread for discovery with delay
        new Thread(() -> {
            try {
                // Wait for other nodes to potentially start
                Thread.sleep(3000);  // 3 second delay


                MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);

                System.out.println("Registered with NamingServer: " + namingServerUrl);

                nodeCountFuture.orTimeout(2, TimeUnit.SECONDS)
                        .whenComplete((cnt, ex) -> {
                            if (ex == null && cnt != null && cnt > 0) {
                                System.out.println("Ring has " + cnt + " node(s) – starting with replication...");
                                replicationManager.replicateInitialFiles();
                            } else {
                                System.out.println("No replication: ring doesn't have any nodes.");
                            }
                        });

                // Perform initial replication (keep this)
                replicationManager.replicateInitialFiles();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Bootstrap error: " + e.getMessage());
            }
        }).start();

        // Start SyncAgent
        try {
            String nextNodeIp = getIpFromNodeId(nextID);
            String nextNodeUrl = "http://" + nextNodeIp + ":8081";

            SyncAgent syncAgent = new SyncAgent(ipAddress, nextNodeUrl, "nodes_storage/" + ipAddress, rest, namingServerUrl);
            Thread syncThread = new Thread(syncAgent);
            syncThread.setDaemon(true);
            syncThread.start();

            System.out.println("[Node] SyncAgent gestart op " + ipAddress + " → synchroniseert met " + nextNodeIp);
        } catch (Exception e) {
            System.err.println("[Node] Fout bij starten van SyncAgent: " + e.getMessage());
        }
        // Start de failureagent
        FailureMonitor monitor = new FailureMonitor(this, rest);
        Thread failMonitorThread = new Thread(monitor);
        failMonitorThread.setDaemon(true);
        failMonitorThread.start();
        System.out.println("[Node] FailureMonitor gestart om next node te controleren.");

    }

    @PreDestroy
    public void onShutdown() {
        System.out.println("Graceful shutdown of node: " + nodeName);

        // update neighbors
        if (previousID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + previousID + "/next", nextID);
        }
        if (nextID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + nextID + "/previous", previousID);
        }

        rest.delete(namingServerUrl + "/api/nodes/" + currentID);
        System.out.println("Node deletet: " + currentID);
    }

    public synchronized int handleDiscovery(String newName) {
        if (newName.equals(nodeName)) return 0;          // ignore self
        int newHash = HashingUtil.generateHash(newName);

        int changed = 0;
        if (isBetween(currentID, newHash, nextID)) {  // I come before the new node
            this.nextID = newHash; // Next changed
            changed = 1;
        }
        if (isBetween(previousID, newHash, currentID)) {  // I come after the new node
            this.previousID = newHash; // Previous changed

            // If we already set nextID before, mark both changes (1 | 2 = 3)
            if (changed == 1) {
                changed = 3; // Previous and next changed
            } else {
                changed = 2;
            }
        }
        return changed;
    }

    public void setInitialCount(int count){
        if (count <= 1){
            previousID = currentID;
            nextID     = currentID;
        }
        nodeCountFuture.complete(count);
    }

    private boolean isBetween(int low, int target, int high) {
        if (low < high) return target > low && target < high;
        else return target > low || target < high;
    }

    public void sendBootstrapResponse(String destIp, int updatedField) {
        // updatedField: 1 = previous   2 = next
        Map<String, Object> payload = Map.of(
                "updatedField", updatedField,
                "nodeID",       currentID
        );
        try {
            rest.postForObject("http://" + destIp + ":8081/api/bootstrap/update",
                    payload, Void.class);
        } catch (Exception e) {
            System.err.println("bootstrap → " + destIp + " : " + e.getMessage());
        }
    }
    // Voor syncAgent
    public String getIpFromNodeId(int nodeId) {
        try {
            String url = namingServerUrl + "/api/nodes/" + nodeId;
            Map<String, Object> response = rest.getForObject(url, Map.class);
            return (String) response.get("ipAddress");
        } catch (Exception e) {
            System.err.println("[Node] Fout bij ophalen van IP voor nodeID " + nodeId + ": " + e.getMessage());
            return "127.0.0.1";
        }
    }

    public void simulateFailureDetection(int failingNodeId) {
        try {
            System.out.println("[TEST] Failure gedetecteerd bij nodeID: " + failingNodeId);

            FailAgent failAgent = new FailAgent(failingNodeId, this.getCurrentID(), this);

            String nextIp = getIpFromNodeId(nextID);
            String url = "http://" + nextIp + ":8081/api/bootstrap/agent/fail";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<FailAgent> request = new HttpEntity<>(failAgent, headers);

            RestTemplate rest = new RestTemplate();
            rest.postForEntity(url, request, String.class);

            System.out.println("[TEST] FailAgent gestart en verzonden naar: " + nextIp);
        } catch (Exception e) {
            System.err.println("[TEST] Fout bij starten van FailAgent: " + e.getMessage());
        }
    }

    public void updatePrevious(int id) { this.previousID = id; }
    public void updateNext(int id) { this.nextID = id; }

    // Getters
    public int getPreviousID() { return previousID; }

    public int getNextID() { return nextID; }

    public void setCurrentID(int currentID) {
        this.currentID = currentID;
    }

    public void setPreviousID(int previousID) {
        this.previousID = previousID;
    }

    public void setNextID(int nextID) {
        this.nextID = nextID;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getCurrentID() {return currentID;}

    public String getNodeName() {
        return nodeName;
    }
}