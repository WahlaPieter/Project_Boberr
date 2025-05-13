package uantwerpen.be.fti.ei.Project.Bootstrap;

import jakarta.annotation.PostConstruct;
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

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import java.io.File;
import java.util.HashMap;

import uantwerpen.be.fti.ei.Project.Replication.FileTransferClient;
import uantwerpen.be.fti.ei.Project.Replication.FileTransferServer;
import uantwerpen.be.fti.ei.Project.Replication.FolderWatcher;

@Component
@Profile("node")
public class Node {
    private int currentID;
    private int previousID;
    private int nextID;
    private String nodeName;
    private String ipAddress;

    @Value("${namingserver.url}")
    private String namingServerUrl;

    @Autowired
    private RestTemplate rest;

    public Node() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::gracefulShutdown));
    }

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
        System.out.println("Node gestart: " + nodeName + " (ID: " + currentID + ")");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        // start multicast listener
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();
        // send discovery
        MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);
        // register with naming server
        Map<String, String> payload = Map.of("nodeName", nodeName, "ipAddress", ipAddress);
        rest.postForObject(namingServerUrl + "/api/nodes", payload, Void.class);
        System.out.println(" Geregistreerd bij NamingServer: " + namingServerUrl);

        scanAndReportFiles(); // Bestanden scannen en rapporteren (Replicatie: Starting)
        new Thread(new FileTransferServer(ipAddress)).start();// Start de TCP-server voor inkomende bestandsoverdrachten
        new Thread(new FolderWatcher(this, ipAddress)).start();// Start folder watcher om nieuwe/verwijderde bestanden op te sporen
    }

    private void gracefulShutdown() {
        System.out.println(" Shutting down node: " + nodeName);
        if (previousID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + previousID + "/next", nextID);
        }
        if (nextID != currentID) {
            rest.put(namingServerUrl + "/api/nodes/" + nextID + "/previous", previousID);
        }
        rest.delete(namingServerUrl + "/api/nodes/" + currentID);
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
// For replication: Starting
    private void scanAndReportFiles() {
        // Pad lokale bestandsmap voor deze node
        String folderPath = "nodes_storage/" + ipAddress;
        File folder = new File(folderPath);
        // Als de folder niet bestaat of geen map is → niks doen
        if (!folder.exists() || !folder.isDirectory()) return;

        // Map om bestandsnaam → hash op te slaan
        Map<String, Integer> fileHashes = new HashMap<>();

        // Loop over alle bestanden in de map
        for (File file : folder.listFiles()) {
            if (file.isFile()) {
                String name = file.getName().replace(".txt", "");
                int hash = HashingUtil.generateHash(name);
                fileHashes.put(name, hash);
            }
        }

        try {
            String url = namingServerUrl + "/api/files/report/" + ipAddress; // Endpoint van de NamingServer
            Map<String, String> replicationTargets = rest.postForObject(url, fileHashes, Map.class);// Verstuur POST-request met bestandsnamen en hashes
            // Als er bestanden zijn die gerepliceerd moeten worden
            if (replicationTargets != null && !replicationTargets.isEmpty()) {
                for (Map.Entry<String, String> entry : replicationTargets.entrySet()) {
                    System.out.println("Bestand " + entry.getKey() + " moet gerepliceerd worden naar " + entry.getValue());
                    // Bestand echt versturen
                    FileTransferClient.sendFile(ipAddress, entry.getValue(), entry.getKey());
                }
            }
        } catch (Exception e) { // Indien fout bij rapporteren
            System.err.println("Fout bij rapporteren aan NamingServer");
            e.printStackTrace();
        }
    }

    // Replication updating
    public void handleNewFile(String fileName) {
        Map<String, Integer> singleFile = Map.of(fileName, HashingUtil.generateHash(fileName));
        try {
            String url = namingServerUrl + "/api/files/report/" + ipAddress;
            Map<String, String> response = rest.postForObject(url, singleFile, Map.class);
            if (response != null && response.containsKey(fileName)) {
                String targetIp = response.get(fileName);
                FileTransferClient.sendFile(ipAddress, targetIp, fileName);
            }
        } catch (Exception e) {
            System.err.println("Fout bij verwerken van nieuw bestand");
            e.printStackTrace();
        }
    }
    // Replication updating
    public void handleRemovedFile(String fileName) {
        try {
            String url = namingServerUrl + "/api/files/remove/" + ipAddress + "/" + fileName;
            rest.delete(url);
            System.out.println("Verwijderingsaanvraag verstuurd voor: " + fileName);
        } catch (Exception e) {
            System.err.println("Fout bij verwerken van verwijderd bestand");
            e.printStackTrace();
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