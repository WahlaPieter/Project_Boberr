package uantwerpen.be.fti.ei.Project.Discovery;

import java.io.IOException;
import java.net.*;

import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;


public class MulticastReceiver implements Runnable {
    private final NamingServer namingServer;

    public MulticastReceiver(NamingServer namingServer) {
        this.namingServer = namingServer;
    }

    @Override
    public void run() {
        try (MulticastSocket socket = new MulticastSocket(MulticastConfig.MULTICAST_PORT)) {
            InetAddress group = InetAddress.getByName(MulticastConfig.MULTICAST_ADDRESS);
            socket.joinGroup(group);
            System.out.println("📡 Listening for discovery messages...");

            while (true) {
                byte[] buffer = new byte[256];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String received = new String(packet.getData(), 0, packet.getLength());
                String[] parts = received.split(";");
                if (parts.length == 2) {
                    String nodeName = parts[0];
                    String ipAddress = parts[1];

                    System.out.println("📨 Received discovery from: " + nodeName + " at " + ipAddress);
                    namingServer.addNode(nodeName, ipAddress); // Verwerk binnen bestaande flow
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
