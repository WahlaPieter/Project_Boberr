package uantwerpen.be.fti.ei.Project.NamingServer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class NamingServer {
    private Map<Integer, String> nodeMap;

    public NamingServer() {
        this.nodeMap = new TreeMap<>(); // Gesorteerde map voor efficiënte zoekopdrachten

    }





    // Voeg een node toe met unieke naam en IP-adres
    public void addNode(String nodeName, String ipAddress) {
        int hash = HashingUtil.generateHash(nodeName);
        if (!nodeMap.containsKey(hash)) {
            nodeMap.put(hash, ipAddress);
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



}
