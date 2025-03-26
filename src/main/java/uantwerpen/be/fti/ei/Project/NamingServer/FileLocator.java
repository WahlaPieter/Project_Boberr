package uantwerpen.be.fti.ei.Project.NamingServer;

import java.util.Map;
import java.util.TreeMap;

public class FileLocator {
    public static String locateFile(int fileHash, Map<Integer, String> nodeMap) {
        if (nodeMap.isEmpty()) return "Geen nodes beschikbaar";

        Integer bestMatch = null;
        for (Integer nodeHash : nodeMap.keySet()) {
            if (nodeHash >= fileHash) {
                bestMatch = nodeHash;
                break;
            }
        }

        if (bestMatch == null) {
            bestMatch = ((TreeMap<Integer, String>) nodeMap).firstKey();
        }
        return nodeMap.get(bestMatch);
    }
}
