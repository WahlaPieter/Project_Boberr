package uantwerpen.be.fti.ei.Project.Replication;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// Deze klasse stuurt een lokaal bestand via TCP naar een andere node (de 'server')
public class FileTransferClient {
    private static final int PORT = 9000;

    public static void sendFile(String sourceIp, String targetIp, String fileName) {
        Path filePath = Paths.get("nodes_storage/" + sourceIp + "/" + fileName + ".txt");

        // Controleer of het bestand bestaat
        if (!Files.exists(filePath)) {
            System.err.println("Bestand niet gevonden: " + filePath);
            return;
        }

        try (Socket socket = new Socket(targetIp, PORT); // Verbind met de TCP-server van de doelnote (target)
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) // Voor het schrijven van primitieve types zoals UTF-strings en bytes
         {
            byte[] fileData = Files.readAllBytes(filePath); // Lees het bestand volledig in het geheugen
            dos.writeUTF(fileName); // Stuur eerst de bestandsnaam
            dos.writeLong(fileData.length); // Stuur dan de bestandsgrootte
            dos.write(fileData); // Stuur de eigenlijke bytes van het bestand

            System.out.println("Bestand verzonden: " + fileName + " naar " + targetIp);
        } catch (IOException e) {
            System.err.println("Fout bij verzenden bestand naar " + targetIp);
            e.printStackTrace();
        }
    }
}
