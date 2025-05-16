package uantwerpen.be.fti.ei.Project.Discovery;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Map;

@Component
@Profile("namingserver")
public class NamingMulticastReceiver implements Runnable {

    @Autowired
    private NamingServer namingServer;

    @PostConstruct
    public void start() {
        Thread t = new Thread(this); t.setDaemon(true); t.start();
    }
    @Override
    public void run() {
        try (MulticastSocket sock = new MulticastSocket(MulticastConfig.MULTICAST_PORT)) {
            sock.joinGroup(InetAddress.getByName(MulticastConfig.MULTICAST_ADDRESS));
            byte[] buf = new byte[256];
            while (true) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                sock.receive(p);
                String[] parts = new String(p.getData(),0,p.getLength()).split(";");
                if (parts.length != 2) continue;
                String nodeName = parts[0], ip = parts[1];

                // 1. addNode() geeft true als nieuw
                boolean added = namingServer.addNode(nodeName, ip);

                // 2. stuur #nodes terug via unicast HTTP
                int count = namingServer.getNodeMap().size() - 1;
                try {
                    String url = "http://" + ip + ":8081/api/bootstrap/info";
                    Map<String,Integer> body = Map.of("count", count);
                    new RestTemplate().postForObject(url, body, Void.class);
                } catch (Exception ignore) {}
            }
        } catch (IOException e){ e.printStackTrace(); }
    }
}