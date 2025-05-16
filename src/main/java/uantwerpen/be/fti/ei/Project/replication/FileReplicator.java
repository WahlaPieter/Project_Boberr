package uantwerpen.be.fti.ei.Project.replication;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileReplicator {
    // this class will handle the actual file transfers
    public static void transferFile(String sourceIp, String targetIp, String fileName, byte[] fileData) throws IOException {
        try (Socket socket = new Socket(targetIp, 8082);
             OutputStream out = socket.getOutputStream();
             DataOutputStream dos = new DataOutputStream(out)) {

            // Send filemane and size
            dos.writeUTF(fileName);
            dos.writeInt(fileData.length);

            // Send file data
            dos.write(fileData);
        }
    }

    public static byte[] receiveFile(String fileName, InputStream in) throws IOException {
        try (DataInputStream dis = new DataInputStream(in);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            String receivedFileName = dis.readUTF();
            int fileSize = dis.readInt();

            byte[] buffer = new byte[4096];
            int read = 0;
            int remaining = fileSize;

            while ((read = dis.read(buffer, 0, Math.min(buffer.length, remaining))) > 0) {
                baos.write(buffer, 0, read);
                remaining -= read;
            }

            return baos.toByteArray();
        }
    }
    public static void startFileReceiver(int port, String storagePath) {
        System.out.println("Starting file receiver on port " + port + " for: " + storagePath);
        new Thread(() -> {
            try {
                Path storageDir = Paths.get(storagePath);
                if (!Files.exists(storageDir)) {
                    Files.createDirectories(storageDir);
                    System.out.println("Created receiver storage: " + storageDir);
                }

                try (ServerSocket serverSocket = new ServerSocket(port)) {
                    System.out.println("File receiver listening on port " + port);
                    while (!Thread.currentThread().isInterrupted()) {
                        try (Socket socket = serverSocket.accept();
                             InputStream in = socket.getInputStream()) {

                            byte[] fileData = receiveFile("temp", in);
                            String fileName = new String(fileData, 0, fileData.length).split("\n")[0];
                            Files.write(Paths.get(storagePath, fileName),
                                    fileData, StandardOpenOption.CREATE);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("File receiver error: " + e.getMessage());
            }
        }).start();
    }


}
