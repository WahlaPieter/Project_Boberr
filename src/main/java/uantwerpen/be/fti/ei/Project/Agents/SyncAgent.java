package uantwerpen.be.fti.ei.Project.Agents;

import uantwerpen.be.fti.ei.Project.Agents.FileEntry;
import uantwerpen.be.fti.ei.Project.Agents.LockRequest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * SyncAgent draait continu op een node en synchroniseert periodiek
 * de bestanden en lock-status met de volgende node in de ring.
 * Daarnaast kan hij locks verspreiden naar alle nodes via REST.
 */
public class SyncAgent implements Runnable, Serializable {

    private final String currentNodeIp;
    private final String nextNodeUrl;
    private final String namingServerUrl;
    private final String storagePath;
    private final RestTemplate restTemplate;

    private final Map<String, FileEntry> agentFileList;

    /**
     * Constructor voor SyncAgent.
     *
     * @param currentNodeIp     IP van deze node
     * @param nextNodeUrl       URL van de volgende node (bv. http://192.168.1.2:8081)
     * @param storagePath       Pad naar de lokale opslagfolder
     * @param restTemplate      RestTemplate voor HTTP-verkeer
     * @param namingServerUrl   URL van de naming server om IP’s op te halen
     */
    public SyncAgent(String currentNodeIp, String nextNodeUrl, String storagePath, RestTemplate restTemplate, String namingServerUrl) {
        this.currentNodeIp = currentNodeIp;
        this.nextNodeUrl = nextNodeUrl;
        this.storagePath = storagePath;
        this.restTemplate = restTemplate;
        this.namingServerUrl = namingServerUrl;
        this.agentFileList = new HashMap<>();
    }

    /**
     * Deze methode wordt continu uitgevoerd:
     * - Lokale bestanden scannen
     * - File list ophalen van volgende node
     * - Synchroniseren van metadata
     * - Locks controleren en eventueel aanvragen
     */
    @Override
    public void run() {
        while (true) {
            try {
                System.out.println("[SyncAgent] Synchronisatie gestart...");

                // 1. Lokale bestanden detecteren
                File folder = new File(storagePath);
                File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));
                if (files != null) {
                    for (File file : files) {
                        String name = file.getName().replace(".txt", "");
                        agentFileList.putIfAbsent(name, new FileEntry(name, false, currentNodeIp));
                    }
                }

                // 2. File list ophalen van volgende node
                ResponseEntity<Map<String, FileEntry>> response = restTemplate.exchange(
                        nextNodeUrl + "/api/agent/filelist",
                        HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<Map<String, FileEntry>>() {}
                );
                Map<String, FileEntry> remoteList = response.getBody();

                // 3. Lijsten vergelijken
                for (Map.Entry<String, FileEntry> entry : remoteList.entrySet()) {
                    agentFileList.putIfAbsent(entry.getKey(), entry.getValue());
                }

                // 4. Lock-check en update agentFileList indien nodig
                for (Map.Entry<String, FileEntry> entry : agentFileList.entrySet()) {
                    FileEntry entryInAgentList = entry.getValue();
                    String filename = entryInAgentList.getFilename();
                    File localFile = new File(storagePath + "/" + filename + ".txt");

                    if (localFile.exists()) {
                        boolean shouldBeLocked = filename.contains("lock_me");

                        if (shouldBeLocked && !entryInAgentList.isLocked()) {
                            entryInAgentList.setLocked(true);
                            System.out.println("[SyncAgent] LOCK toegevoegd aan agentlijst voor: " + filename);
                        }

                        if (entryInAgentList.isLocked()) {
                            System.out.println("[SyncAgent] Bestand is gelockt: " + filename + " → geen bewerking toegestaan.");
                        }
                    }
                }

                // 5. Simulatie: lock aanvragen voor "rapport" als het niet gelockt is
                String targetFile = "rapport";
                File testFile = new File(storagePath + "/" + targetFile + ".txt");
                if (testFile.exists() && !agentFileList.getOrDefault(targetFile, new FileEntry(targetFile, false, currentNodeIp)).isLocked()) {
                    List<String> allIps = fetchAllNodeIps();
                    requestLock(targetFile, allIps);
                }

                Thread.sleep(5000);

            } catch (Exception e) {
                System.err.println("[SyncAgent] Fout tijdens synchronisatie: " + e.getMessage());
            }
        }
    }

    /**
     * Haalt alle node-IP’s op van de naming server, exclusief deze node zelf.
     */
    public List<String> fetchAllNodeIps() {
        try {
            ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                    namingServerUrl + "/api/nodes",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<Map<String, Object>>>() {}
            );

            List<Map<String, Object>> nodes = response.getBody();
            List<String> ips = new ArrayList<>();
            for (Map<String, Object> node : nodes) {
                String ip = (String) node.get("ipAddress");
                if (!ip.equals(currentNodeIp)) {
                    ips.add(ip);
                }
            }
            return ips;

        } catch (Exception e) {
            System.err.println("[SyncAgent] Fout bij ophalen IP’s: " + e.getMessage());
            return List.of();
        }
    }

    /**
     * Vraagt een lock aan bij alle nodes voor het opgegeven bestand.
     *
     * @param filename    de bestandsnaam waarvoor een lock wordt aangevraagd
     * @param allNodeIps  lijst van IP’s van alle andere nodes
     */
    public void requestLock(String filename, List<String> allNodeIps) {
        LockRequest req = new LockRequest(filename, currentNodeIp);

        for (String ip : allNodeIps) {
            try {
                String url = "http://" + ip + ":8081/api/agent/lock";

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<LockRequest> request = new HttpEntity<>(req, headers);

                ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
                System.out.println("[LockRequest] Lock gevraagd aan " + ip + ": " + response.getStatusCode());

            } catch (Exception e) {
                System.err.println("[LockRequest] Fout bij verzenden naar " + ip + ": " + e.getMessage());
            }
        }
    }

    public Map<String, FileEntry> getAgentFileList() {
        return agentFileList;
    }
}
