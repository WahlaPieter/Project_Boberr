package uantwerpen.be.fti.ei.Project.Agents;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpMethod;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * SyncAgent draait continu op een node en houdt een gedeelde file list bij,
 * door periodiek de bestanden van de eigen node te synchroniseren met die van de volgende node.
 */
public class SyncAgent implements Runnable, Serializable {

    private final String currentNodeIp;
    private final String nextNodeUrl;
    private final String storagePath;
    private final RestTemplate restTemplate;

    private final Map<String, FileEntry> agentFileList;

    /**
     * Constructor voor SyncAgent.
     *
     * @param currentNodeIp IP-adres van de huidige node
     * @param nextNodeUrl   Volledig URL van de volgende node (bijv. http://192.168.0.2:8081)
     * @param storagePath   Pad naar lokale opslagfolder van de node
     * @param restTemplate  Spring RestTemplate voor HTTP-verkeer
     */
    public SyncAgent(String currentNodeIp, String nextNodeUrl, String storagePath, RestTemplate restTemplate) {
        this.currentNodeIp = currentNodeIp;
        this.nextNodeUrl = nextNodeUrl;
        this.storagePath = storagePath;
        this.restTemplate = restTemplate;
        this.agentFileList = new HashMap<>();
    }

    /**
     * Wordt automatisch gestart in een aparte thread.
     * Deze methode draait in een oneindige lus:
     * 1. Detecteert lokale bestanden
     * 2. Vraagt file list op van volgende node
     * 3. Synchroniseert beide lijsten
     */
    @Override
    public void run() {
        while (true) {
            try {
                System.out.println("[SyncAgent] Synchronisatie gestart...");

                // 📁 1. Lokale bestanden detecteren en toevoegen aan file list
                File folder = new File(storagePath);
                File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));
                if (files != null) {
                    for (File file : files) {
                        String name = file.getName().replace(".txt", "");
                        agentFileList.putIfAbsent(name, new FileEntry(name, false, currentNodeIp));
                    }
                }

                // 🌐 2. Ophalen van file list van de volgende node
                ResponseEntity<Map<String, FileEntry>> response = restTemplate.exchange(
                        nextNodeUrl + "/api/agent/filelist",
                        org.springframework.http.HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<Map<String, FileEntry>>() {}
                );

                Map<String, FileEntry> remoteList = response.getBody();

                // 🔄 3. Synchroniseren: aanvullen met bestanden van andere node
                for (Map.Entry<String, FileEntry> entry : remoteList.entrySet()) {
                    agentFileList.putIfAbsent(entry.getKey(), entry.getValue());
                }

                // 🔐 4. Lock check (optioneel – zie volgende stappen)
                for (Map.Entry<String, FileEntry> entry : agentFileList.entrySet()) {
                    FileEntry entryInAgentList = entry.getValue();
                    String filename = entryInAgentList.getFilename();

                    File localFile = new File(storagePath + "/" + filename + ".txt");

                    if (localFile.exists()) {
                        // ⛔ Simulatie: als bestandsnaam "lock_me" bevat → beschouwen we het als gelockt
                        boolean shouldBeLocked = filename.contains("lock_me");

                        // Als het lokaal gelockt is maar nog niet in de agentlist → update agentlist
                        if (shouldBeLocked && !entryInAgentList.isLocked()) {
                            entryInAgentList.setLocked(true);
                            System.out.println("[SyncAgent] LOCK toegevoegd aan agentlijst voor: " + filename);
                        }

                        // Als het bestand in de agentlist gelockt is → toon waarschuwing (geen bewerking uitvoeren)
                        if (entryInAgentList.isLocked()) {
                            System.out.println("[SyncAgent] Bestand gelockt volgens agentlijst: " + filename + " → schrijf geblokkeerd.");
                            // Hier zou je lokaal schrijven kunnen weigeren
                        }
                    }
                }

                Thread.sleep(5000); // Wacht 5 seconden voor volgende synchronisatie

            } catch (Exception e) {
                System.err.println("[SyncAgent] Fout bij synchronisatie: " + e.getMessage());
            }
        }
    }

    /**
     * Geeft de huidige gesynchroniseerde lijst van FileEntry-objecten.
     * Wordt gebruikt door de node of REST om de status op te vragen.
     *
     * @return Map met bestandsnamen en hun FileEntry info
     */
    public Map<String, FileEntry> getAgentFileList() {
        return agentFileList;
    }
    public void requestLock(String filename, List<String> allNodeIps) {
        RestTemplate rest = new RestTemplate();
        LockRequest req = new LockRequest(filename, currentNodeIp);

        for (String ip : allNodeIps) {
            try {
                String url = "http://" + ip + ":8081/api/agent/lock";

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<LockRequest> request = new HttpEntity<>(req, headers);

                ResponseEntity<String> response = rest.postForEntity(url, request, String.class);
                System.out.println("[LockRequest] Lock gevraagd aan " + ip + ": " + response.getStatusCode());

            } catch (Exception e) {
                System.err.println("[LockRequest] Fout bij versturen naar " + ip + ": " + e.getMessage());
            }
        }
    }

    public List<String> fetchAllNodeIps(String namingServerUrl) {
        try {
            RestTemplate rest = new RestTemplate();
            String url = namingServerUrl + "/api/nodes";
            ResponseEntity<List<Map<String, Object>>> response = rest.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {}
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
            System.err.println("[SyncAgent] Fout bij ophalen van IP-adressen: " + e.getMessage());
            return List.of();
        }
    }




}