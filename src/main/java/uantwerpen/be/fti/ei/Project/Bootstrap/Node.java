package uantwerpen.be.fti.ei.Project.Bootstrap;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastSender;
import uantwerpen.be.fti.ei.Project.Discovery.MulticastReceiver;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;
import uantwerpen.be.fti.ei.Project.REST.NodeController;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


@Component
public class Node {

    private int currentID;
    private int previousID;
    private int nextID;
    private String nodeName;
    private String ipAddress;

    public Node(){
    }

    @PostConstruct
    public void init() {
        int randomId = ThreadLocalRandom.current().nextInt(1, 1000);
        this.nodeName = "Node-" + randomId;
        this.ipAddress = "127.0.0.1";

        this.currentID = HashingUtil.generateHash(nodeName);
        this.previousID = currentID;
        this.nextID = currentID;

        System.out.println("🟢 Node gestart: " + nodeName + " (ID: " + currentID + ")");
    }

    //We have to listen for the naming server to see if it is running
    @EventListener(ApplicationReadyEvent.class)
    public void registerWithNamingServer() {
        Thread listener = new Thread(new MulticastReceiver(this));
        listener.setDaemon(true);
        listener.start();
        MulticastSender.sendDiscoveryMessage(nodeName, ipAddress);
        RestTemplate rest = new RestTemplate();
        Map<String,Object> payload = Map.of(
                "nodeName", nodeName,
                "ipAddress", ipAddress
        );
        rest.postForObject("http://localhost:8080/api/nodes", payload, Void.class);

        System.out.println("✅ Geregistreerd bij NamingServer: " + nodeName);
    }

    public String getNodeName() {
        return nodeName;
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

    public int getCurrentID() {
        return currentID;
    }

    public int getPreviousID() {
        return previousID;
    }

    public int getNextID() {
        return nextID;
    }

    public void updateNextIDIntern(int nextID) { this.nextID = nextID; }

    public void updatePreviousIDIntern(int previousID) { this.previousID = previousID; }

    public void setNextID(int nextID) {
        if (this.nextID != nextID) {
            updateNextIDIntern(nextID);
            notifyNamingServer();
        }
    }

    public void setPreviousID(int previousID) {
        if (this.previousID != previousID) {
            updatePreviousIDIntern(previousID);
            notifyNamingServer();
        }
    }

    public void setCurrentID(int currentID) {
        this.currentID = currentID;
    }

    private void notifyNamingServer() {
        RestTemplate rest = new RestTemplate();
        NodeController.NodeUpdateRequest payload = new NodeController.NodeUpdateRequest();
        payload.setPreviousID(this.previousID);
        payload.setNextID(this.nextID);
        rest.put("http://localhost:8080/api/nodes/" + this.currentID, payload);
    }
}
