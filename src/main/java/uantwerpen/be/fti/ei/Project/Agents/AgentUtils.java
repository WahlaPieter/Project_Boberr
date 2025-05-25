package uantwerpen.be.fti.ei.Project.Agents;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class AgentUtils {


    public static Map<String, FileEntry> scanLocalFiles(String storagePath, String nodeIp) {
        Map<String, FileEntry> fileList = new HashMap<>();

        File folder = new File(storagePath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));

        if (files != null) {
            for (File file : files) {
                String filename = file.getName().replace(".txt", "");
                FileEntry entry = new FileEntry(filename, false, nodeIp);
                fileList.put(filename, entry);
            }
        }

        return fileList;
    }
}
