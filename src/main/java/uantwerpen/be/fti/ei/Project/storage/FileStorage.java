package uantwerpen.be.fti.ei.Project.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.Set;

public class FileStorage {
    private static final String BASE = "nodes_storage/";

    public static void storeFile(String ip, String fileName, String content) throws IOException {
        Path dir = Paths.get(BASE + ip);
        if (!Files.exists(dir)) Files.createDirectories(dir);
        Files.writeString(dir.resolve(fileName + ".txt"), content);
    }

    public static void moveFile(String srcIp, String dstIp, String fileName) throws IOException {
        Path src = Paths.get(BASE + srcIp + "/" + fileName + ".txt");
        Path dstDir = Paths.get(BASE + dstIp);
        if (!Files.exists(dstDir)) Files.createDirectories(dstDir);
        Files.move(src, dstDir.resolve(fileName + ".txt"), StandardCopyOption.REPLACE_EXISTING);
    }

    public static boolean fileExists(String ip, String fileName) {
        return Files.exists(Paths.get(BASE + ip + "/" + fileName + ".txt"));
    }

    public static byte[] readFileBytes(String ip, String fileName) throws IOException {
        Path filePath = Paths.get(BASE + ip + "/" + fileName + ".txt");
        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("File not found: " + filePath.toString());
        }
        return Files.readAllBytes(filePath);
    }
}