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

    /* ------------- ring info ------------- */
    private int currentID;
    private int previousID;
    private int nextID;

    /* ------------- identiteit ------------- */
    private String nodeName;
    private String ipAddress;

    /* ------------- runtime-only state (NIET serialiseren) ------------- */
    private transient ReplicationManager replicationManager;   // lab 5
    private transient FileWatcher        fileWatcher;          // lab 5
    private transient CompletableFuture<Integer> nodeCountFuture = new CompletableFuture<>();

    @Autowired
    private transient RestTemplate rest;

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

        // Create storage directory with proper path handling
        Path storagePath = Paths.get("storage").toAbsolutePath(); // Changed to relative path
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

        // Initialize components with consistent path
        this.replicationManager = new ReplicationManager(
                nodeName, ipAddress, namingServerUrl, rest, storagePath.toString());
        this.fileWatcher = new FileWatcher(storagePath.toString(), replicationManager);

        FileReplicator.startFileReceiver(8082, storagePath.toString());

        System.out.println("Node started: " + nodeName + " (ID: " + currentID + ")");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        // Start multicast listener (keep this)
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();

        // Start file watcher (keep this - fixed version)
        Thread watcherThread = new Thread(fileWatcher);
        watcherThread.setDaemon(true);
        watcherThread.start();

        // Start a separate thread for discovery with delay
        new Thread(() -> {
            try {
                // Wait for other nodes to potentially start
                Thread.sleep(3000);  // 3 second delay

                /* stap 1 – multicast zodat NamingServer ons registreert */
                MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);

                System.out.println("Registered with NamingServer: " + namingServerUrl);

                nodeCountFuture.orTimeout(2, TimeUnit.SECONDS)
                        .whenComplete((cnt, ex) -> {
                            if (ex == null && cnt != null && cnt > 0) {
                                System.out.println("Ring bevat nu " + cnt + " node(s) – starten met replicatie...");
                                replicationManager.replicateInitialFiles();
                            } else {
                                System.out.println("Geen replicatie uitgevoerd: ring bevat nog geen andere nodes.");
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
        System.out.println("Graceful shutdown van node: " + nodeName);

        // update buren in NamingServer
        if (previousID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + previousID + "/next", nextID);
        }
        if (nextID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + nextID + "/previous", previousID);
        }
        // deregistreer
        rest.delete(namingServerUrl + "/api/nodes/" + currentID);
        System.out.println("Node verwijderd: " + currentID);
    }

    public synchronized boolean handleDiscovery(String newName, String newIp) {
        if (newName.equals(this.nodeName)) return false;
        int newHash = HashingUtil.generateHash(newName);
        boolean updated = false;
        if (isBetween(previousID, newHash, currentID)) {
            this.previousID = newHash;
            updated = true;
        }
        if (isBetween(currentID, newHash, nextID)) {
            this.nextID = newHash;
            updated = true;
        }
        return updated;
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

    public void sendBootstrapResponse(String destIp, int newHash, boolean updated) {
        Map<String, Object> resp = Map.of(
                "updatedField", updated ? (isBetween(currentID, newHash, nextID) ? 1 : 2) : 0,
                "nodeID", currentID,
                "previousID", previousID,
                "nextID", nextID
        );
        rest.postForObject("http://" + destIp + ":8081/api/bootstrap/update", resp, Void.class);
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