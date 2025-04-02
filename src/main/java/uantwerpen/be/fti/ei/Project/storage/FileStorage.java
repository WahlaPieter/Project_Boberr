package uantwerpen.be.fti.ei.Project.storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class FileStorage {
    private static final String BASE_DIR = "nodes_storage/";

    public static void storeFile(String nodeIp, String fileName, String content) throws IOException {
        Path nodeDir = Paths.get(BASE_DIR + nodeIp);
        if (!Files.exists(nodeDir)) {
            Files.createDirectories(nodeDir);
        }
        Path filePath = nodeDir.resolve(fileName + ".txt");
        Files.write(filePath, content.getBytes());
    }

    public static void moveFile(String sourceNodeIp, String targetNodeIp, String fileName) throws IOException {
        Path source = Paths.get(BASE_DIR + sourceNodeIp + "/" + fileName + ".txt");
        Path targetDir = Paths.get(BASE_DIR + targetNodeIp);

        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        Files.move(source, targetDir.resolve(fileName + ".txt"), StandardCopyOption.REPLACE_EXISTING);
    }

    public static boolean fileExists(String nodeIp, String fileName) {
        return Files.exists(Paths.get(BASE_DIR + nodeIp + "/" + fileName + ".txt"));
    }
}