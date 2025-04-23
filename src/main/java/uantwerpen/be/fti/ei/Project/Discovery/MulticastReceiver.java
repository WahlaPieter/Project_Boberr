package uantwerpen.be.fti.ei.Project.Discovery;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;

public class MulticastReceiver implements Runnable {

    private final Node node;

    public MulticastReceiver(Node node) {
        this.node = node;
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
                    if (nodeName.equals(this.node.getNodeName())) {
                        continue;
                    }
                    int newNodeHash = HashingUtil.generateHash(nodeName);

                    int currentID = node.getCurrentID();
                    int prevID = node.getPreviousID();
                    int nextID = node.getNextID();

                    boolean updated = false;

                    Map<String, Integer> response = new HashMap<>();
                    response.put("newNodeID", newNodeHash);
                    response.put("currentID", currentID);

                    if (isBetween(prevID, newNodeHash, currentID)) {
                        if (node.getPreviousID() != newNodeHash) {
                            node.setPreviousID(newNodeHash);
                            response.put("updatedField", 1);
                            updated = true;
                        }
                    }

                    if (isBetween(currentID, newNodeHash, nextID)) {
                        if (node.getNextID() != newNodeHash) {
                            node.setNextID(newNodeHash);
                            response.put("updatedField", 2);
                            updated = true;
                        }
                    }

                    if (updated) {
                        sendUnicastResponse(ipAddress, response);
                        System.out.println("🔁 Sent unicast bootstrap info to: " + ipAddress);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isBetween(int low, int target, int high) {
        if (low < high)
            return (target > low && target < high);
        else
            return (target > low || target < high); // ring wrap-around
    }

    private void sendUnicastResponse(String ip, Map<String, Integer> data) {
        try {
            URL url = new URL("http://" + ip + ":8080/api/bootstrap/update");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");

            String json = String.format("{\"updatedField\":%d,\"nodeID\":%d}",
                    data.get("updatedField"), data.get("currentID"));

            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes());
                os.flush();
            }

            con.getResponseCode();
        } catch (IOException e) {
            System.err.println("⚠️ Failed to send unicast response to " + ip);
        }
    }
}
