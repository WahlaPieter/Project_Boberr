package uantwerpen.be.fti.ei.Project.NamingServer;


import java.io.*;
import java.util.*;
import org.springframework.stereotype.Component;

import static uantwerpen.be.fti.ei.Project.storage.JsonService.saveToJson;

@Component
public class NamingServer {
    private Map<Integer, String> nodeMap;
    private static final String DATA_FILE = "nodes.json";

    public NamingServer() {
        this.nodeMap = new TreeMap<>(); // Gesorteerde map voor efficiënte zoekopdrachten

    }
    public void saveNodeMap(){
        saveToJson(nodeMap);
    }

    public Map<Integer, String> getNodeMap() {
        return nodeMap;
    }

    // Voeg een node toe met unieke naam en IP-adres
    public boolean addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (!nodeMap.containsKey(hash)) {
            nodeMap.put(hash, ipAddress);
            saveNodeMap();
            System.out.println("Node toegevoegd: " + nodeName + " -> " + ipAddress);
            return true;
        } else {
            System.out.println("Node met deze hash bestaat al!");
            return false;
        }
    }

    // Verwijder een node
    public boolean removeNode(String nodeName) {
        int hash = HashingUtil.generateHash(nodeName);
        if (nodeMap.containsKey(hash)) {
            nodeMap.remove(hash);
            saveNodeMap();
            System.out.println("Node verwijderd: " + nodeName);
            return true;
        } else {
            System.out.println("Node niet gevonden!");
            return false;
        }
    }


    // Zoek de juiste node voor een bestand
    public String findFileLocation(String filename) {
        int fileHash = HashingUtil.generateHash(filename);
        return FileLocator.locateFile(fileHash, nodeMap);
    }

}
