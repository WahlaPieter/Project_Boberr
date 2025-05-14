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

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Profile("node")
public class Node {
    private int currentID;
    private int previousID;
    private int nextID;
    private String nodeName;
    private String ipAddress;
    private ReplicationManager replicationManager;
    private FileWatcher fileWatcher;

    @Value("${namingserver.url}")
    private String namingServerUrl;

    @Autowired
    private RestTemplate rest;

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

        // Initialize replication
        this.replicationManager = new ReplicationManager(
                nodeName, ipAddress, namingServerUrl, rest, "/storage");
        this.fileWatcher = new FileWatcher("/storage", replicationManager);

        FileReplicator.startFileReceiver(8081, "/storage");

        System.out.println("Node gestart: " + nodeName + " (ID: " + currentID + ")");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        // start multicast listener
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();

        // Start file watcher
        Thread watcherThread = new Thread(String.valueOf(fileWatcher));
        watcherThread.setDaemon(true);
        watcherThread.start();

        // send discovery
        MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);
        // register with naming server
        Map<String, String> payload = Map.of("nodeName", nodeName, "ipAddress", ipAddress);
        rest.postForObject(namingServerUrl + "/api/nodes", payload, Void.class);


        // Perform initial replication
        replicationManager.replicateInitialFiles();
        System.out.println(" Geregistreerd bij NamingServer: " + namingServerUrl);
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

    private boolean isBetween(int low, int target, int high) {
        if (low < high) return target > low && target < high;
        else return target > low || target < high;
    }

    public void sendBootstrapResponse(String destIp) {
        int updatedField = (isBetween(currentID, currentID, nextID) ? 2 : 1);
        Map<String, Integer> resp = Map.of(
                "updatedField", updatedField,
                "nodeID", currentID
        );
        try {
            rest.postForObject("http://" + destIp + ":8080/api/bootstrap/update", resp, Void.class);
            System.out.println(" Bootstrap info naar " + destIp);
        } catch (Exception e) {
            System.err.println(" Fout bij bootstrap naar " + destIp);
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
}