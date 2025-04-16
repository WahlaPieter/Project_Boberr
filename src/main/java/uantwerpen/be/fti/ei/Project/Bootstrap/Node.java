package uantwerpen.be.fti.ei.Project.Bootstrap;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastSender;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastReceiver;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;

@Component
public class Node {

    private int currentID;
    private int previousID;
    private int nextID;
    private String nodeName;
    private String ipAddress;

    @PostConstruct
    public void init() {
        this.nodeName = "Node-" + System.currentTimeMillis(); // unieke naam
        this.ipAddress = "127.0.0.1"; // tijdelijk hardcoded, kan verbeterd worden

        this.currentID = HashingUtil.generateHash(nodeName);
        this.previousID = currentID;
        this.nextID = currentID;

        System.out.println("🟢 Node gestart: " + nodeName + " (ID: " + currentID + ")");

        // Start Multicast listener in achtergrond
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();

        // Stuur discovery
        MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);
    }

    // ===== GETTERS & SETTERS =====

    public int getCurrentID() {
        return currentID;
    }

    public int getPreviousID() {
        return previousID;
    }

    public int getNextID() {
        return nextID;
    }

    public void setPreviousID(int previousID) {
        this.previousID = previousID;
        System.out.println("🔁 Updated previousID → " + previousID);
    }

    public void setNextID(int nextID) {
        this.nextID = nextID;
        System.out.println("🔁 Updated nextID → " + nextID);
    }
}
