package uantwerpen.be.fti.ei.Project.Discovery;
import java.net.*;

public class MulticastSender {
    public static void sendDiscoveryMessage(String nodeName, String ipAddress) {
        try (MulticastSocket socket = new MulticastSocket()) {
            String message = nodeName + ";" + ipAddress;
            byte[] buffer = message.getBytes();
            InetAddress group = InetAddress.getByName(MulticastConfig.MULTICAST_ADDRESS);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, MulticastConfig.MULTICAST_PORT);
            socket.send(packet);
            System.out.println(" Discovery message sent: " + message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}