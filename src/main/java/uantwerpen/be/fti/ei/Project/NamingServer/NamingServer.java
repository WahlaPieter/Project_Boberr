package uantwerpen.be.fti.ei.Project.NamingServer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class NamingServer {
    private Map<Integer, String> nodeMap;
    private static final String DATA_FILE = "nodes.json";

    public NamingServer() {
        this.nodeMap = new TreeMap<>(); // Gesorteerde map voor efficiënte zoekopdrachten
        loadNodeMap(); // to load any existing node data from JSON file

    }





    // Voeg een node toe met unieke naam en IP-adres
    public void addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (!nodeMap.containsKey(hash)) {
            nodeMap.put(hash, ipAddress);
            saveNodeMap();
            System.out.println("Node toegevoegd: " + nodeName + " -> " + ipAddress);
        } else {
            System.out.println("Node met deze hash bestaat al!");
        }

    }



    // Verwijder een node
    public void removeNode(String nodeName) {
        int hash = HashingUtil.generateHash(nodeName);
        if (nodeMap.containsKey(hash)) {
            nodeMap.remove(hash);
            System.out.println("Node verwijderd: " + nodeName);
        } else {
            System.out.println("Node niet gevonden!");
        }
    }

    // Zoek de juiste node voor een bestand
    public String findFileLocation(String filename) {
        int fileHash = HashingUtil.generateHash(filename);
        return FileLocator.locateFile(fileHash, nodeMap);
    }

    public Map<Integer, String> getNodeMap() {
        return nodeMap;
    }

    private void loadNodeMap() {
        if (!Files.exists(Paths.get(DATA_FILE))) {
            return;
        }
        try {
            String json = new String(Files.readAllBytes(Paths.get(DATA_FILE)));
            if (json.startsWith("{\"nodes\":[")){
                String nodesPart = json.substring(json.indexOf("[") + 1, json.lastIndexOf("]"));
                String[] nodeEntries = nodesPart.split("\\},\\{}");
                for (String entry : nodeEntries) {
                    entry = entry.replaceAll("[{}\"]","");
                    String[] parts = entry.split(",");

                    int hash = 0;
                    String ip = "";
                    for (String part : parts) {
                        String[] keyValue = part.split(":");
                        if (keyValue[0].equals("hash")) {
                            hash = Integer.parseInt(keyValue[1]);
                        } else if (keyValue[0].equals("ip")) {
                            ip = keyValue[1];

                        }
                    }
                    if (hash != 0 && !ip.isEmpty()) {
                        nodeMap.put(hash, ip);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void saveNodeMap() {
        try {
            StringBuilder json = new StringBuilder("{\"nodes\":[");
            boolean first = true;

            for (Map.Entry<Integer, String> entry : nodeMap.entrySet()) {
                if (!first){
                    json.append(",");
                }
                json.append(String.format("\"%s\":\"%s\"", entry.getKey(), entry.getValue()));
                first = false;

            }
            json.append("]}");
            Files.write(Paths.get(DATA_FILE), json.toString().getBytes());
        } catch (Exception e) {
            System.err.println("Failed so save node map:" + e.getMessage());
        }

    }

}
