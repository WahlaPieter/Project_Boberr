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
            System.out.println("📡 Listening for discovery messages...");
            byte[] buffer = new byte[256];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String received = new String(packet.getData(), 0, packet.getLength());
                String[] parts = received.split(";");
                if (parts.length == 2) {
                    String name = parts[0], ip = parts[1];
                    if (node.handleDiscovery(name, ip)) {
                        node.sendBootstrapResponse(ip);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}