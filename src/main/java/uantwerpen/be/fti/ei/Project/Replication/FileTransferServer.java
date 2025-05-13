package uantwerpen.be.fti.ei.Project.Replication;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
/**
 * Deze klasse start een server op poort 9000 die inkomende bestanden ontvangt via TCP.
 * Elke node draait deze server om bestanden van andere nodes te kunnen ontvangen.
 */

public class FileTransferServer implements Runnable {
    private final String ipAddress;
    private static final int PORT = 9000;

    public FileTransferServer(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    // Methode die wordt uitgevoerd wanneer deze klasse in een Thread wordt gestart
    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("FileTransferServer gestart op poort " + PORT);
            while (true) {  // Blijf voor altijd luisteren naar inkomende connecties
                Socket socket = serverSocket.accept();
                new Thread(() -> handleClient(socket)).start();// Behandel elke client(node) in aparte thread => clients moeten niet wachten
            }
        } catch (IOException e) {
            System.err.println("Fout bij starten FileTransferServer");
            e.printStackTrace();
        }
    }

    /**
     * Methode om de inkomende verbinding af te handelen:
     * 1. Ontvang bestandsnaam
     * 2. Ontvang bestandsgrootte
     * 3. Lees en sla bestand op
     */
    private void handleClient(Socket socket) {
        try (DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            String fileName = dis.readUTF();// Ontvang bestandsnaam
            long fileSize = dis.readLong(); // Ontvang bestandsgrootte

            Path dir = Paths.get("nodes_storage/" + ipAddress); // Maak opslagmap aan voor deze node
            Files.createDirectories(dir);

            File file = dir.resolve(fileName + ".txt").toFile(); // Bestemming van het bestand (inclusief extensie)

            // Schrijf het inkomende bestand byte-per-byte weg
            try (FileOutputStream fos = new FileOutputStream(file)) {
                byte[] buffer = new byte[4096];
                long bytesReceived = 0;
                while (bytesReceived < fileSize) {
                    int read = dis.read(buffer, 0, (int) Math.min(buffer.length, fileSize - bytesReceived));
                    fos.write(buffer, 0, read);
                    bytesReceived += read;
                }
            }

            System.out.println("Bestand ontvangen: " + fileName);
        } catch (IOException e) {
            System.err.println("Fout bij ontvangen bestand");
            e.printStackTrace();
        }
    }

}
