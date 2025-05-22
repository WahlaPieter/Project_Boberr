package uantwerpen.be.fti.ei.Project.Agents;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class AgentUtils {

    /**
     * Leest alle bestanden in de lokale opslagfolder en maakt er FileEntry-objecten van.
     *
     * @param storagePath pad naar de opslagfolder (bijv. "nodes_storage/192.168.1.101")
     * @param nodeIp IP-adres van de huidige node (gebruikt als eigenaar)
     * @return een map van bestandsnamen naar FileEntry objecten
     */

    /**
     * Scant de lokale opslagfolder van een node op alle aanwezige .txt-bestanden
     * en bouwt een map op van bestandsnamen naar FileEntry-objecten.
     *
     * Elk FileEntry bevat metadata zoals de bestandsnaam, lockstatus (standaard false)
     * en het IP-adres van de node (als eigenaar).
     *
     * @param storagePath het pad naar de opslagfolder van de node
     * @param nodeIp het IP-adres van de huidige node
     * @return een map van bestandsnamen naar FileEntry-objecten
     */
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
