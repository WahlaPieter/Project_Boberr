package uantwerpen.be.fti.ei.Project;

import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;
import uantwerpen.be.fti.ei.Project.storage.FileStorage;


public class SystemTest {
    public static void main(String[] args) {
        // Initialize naming server
        NamingServer ns = new NamingServer();

        // Add initial nodes
        ns.addNode("node1", "192.168.0.2");
        ns.addNode("node2", "192.168.0.3");

        // Store test file
        ns.storeFile("testfile");

        // Verify initial placement
        String initialLocation = ns.findFileLocation("testfile");
        System.out.println("Initial location: " + initialLocation);
        System.out.println("File exists: " +
                FileStorage.fileExists(initialLocation, "testfile"));

        // Add new node
        ns.addNode("node3", "192.168.0.4");

        // Verify new location
        String newLocation = ns.findFileLocation("testfile");
        System.out.println("New location: " + newLocation);
        System.out.println("File exists: " +
                FileStorage.fileExists(newLocation, "testfile"));
        System.out.println("Old location cleared: " +
                !FileStorage.fileExists(initialLocation, "testfile"));
    }
}