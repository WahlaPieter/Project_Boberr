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
import uantwerpen.be.fti.ei.Project.Discovery.MulticastReceiver;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastSender;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.replication.FileWatcher;
import uantwerpen.be.fti.ei.Project.replication.ReplicationManager;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
    }

    @PreDestroy
    public void onShutdown() {
        System.out.println("Graceful shutdown of node: " + nodeName);
        replicationManager.transferReplicasToPreviousNodeOnShutdown();
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

    public boolean deleteLocalFile(String fileName){
        Path filePath = Paths.get(storagePath, fileName);
        try {
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                Files.delete(filePath);
                System.out.println("Node '" + nodeName + "': Successfully deleted local file: " + filePath.toAbsolutePath());
                // Also remove from FileWatcher's knownFiles if it's managing that accurately
                if (fileWatcher != null) { // If fileWatcher instance is available
                    fileWatcher.removeKnownFile(fileName);
                }
                return true;
            } else {
                System.out.println("Node '" + nodeName + "': Local file to delete not found or not a file: " + filePath.toAbsolutePath());
                return false; // Or true if "not found" is considered success for deletion
            }
        } catch (IOException e) {
            System.err.println("Node '" + nodeName + "': Error deleting local file '" + fileName + "': " + e.getMessage());
            return false;
        }
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

    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    public String getStoragePath() {
        return storagePath;
    }
}