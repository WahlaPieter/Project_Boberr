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
}
