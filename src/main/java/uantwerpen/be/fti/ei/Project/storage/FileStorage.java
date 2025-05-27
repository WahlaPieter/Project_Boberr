package uantwerpen.be.fti.ei.Project.storage;

import java.io.IOException;
import java.nio.file.*;

public class FileStorage {
    private static final String BASE = "nodes_storage/";

    public static void storeFile(String ip, String fileName, String content) throws IOException {
        Path dir = Paths.get(BASE + ip);
        if (!Files.exists(dir)) Files.createDirectories(dir);
        Files.writeString(dir.resolve(fileName + ".txt"), content);
    }
}