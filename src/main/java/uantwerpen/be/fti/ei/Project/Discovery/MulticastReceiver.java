package uantwerpen.be.fti.ei.Project.Discovery;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

public class MulticastReceiver implements Runnable {

    private final Node node;
    public MulticastReceiver(Node node) { this.node = node; }

    @Override
    public void run() {
        try (MulticastSocket socket = new MulticastSocket(MulticastConfig.MULTICAST_PORT)) {
            socket.joinGroup(InetAddress.getByName(MulticastConfig.MULTICAST_ADDRESS));
            System.out.println("listening for discovery …");

            byte[] buffer = new byte[256];

            while (true) {
                DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
                socket.receive(pkt);

                String[] parts = new String(pkt.getData(), 0, pkt.getLength()).split(";");
                if (parts.length != 2) continue;

                String name = parts[0], ip = parts[1];


                int mask = node.handleDiscovery(name);

                if ((mask & 1) != 0) node.sendBootstrapResponse(ip, 1); // nextID updated so I am your previous
                if ((mask & 2) != 0) node.sendBootstrapResponse(ip, 2); // PreviousID updated so I am your next
            }
        } catch (Exception e) {
            System.err.println("discovery-receiver: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
