package uantwerpen.be.fti.ei.Project.Agents;

import uantwerpen.be.fti.ei.Project.Agents.FileEntry;
import uantwerpen.be.fti.ei.Project.Agents.LockRequest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class SyncAgent implements Runnable, Serializable {

    private final String currentNodeIp;
    private final String nextNodeUrl;
    private final String namingServerUrl;
    private final String storagePath;
    private final RestTemplate restTemplate;

    private final Map<String, FileEntry> agentFileList;

    public SyncAgent(String currentNodeIp, String nextNodeUrl, String storagePath, RestTemplate restTemplate, String namingServerUrl) {
        this.currentNodeIp = currentNodeIp;
        this.nextNodeUrl = nextNodeUrl;
        this.storagePath = storagePath;
        this.restTemplate = restTemplate;
        this.namingServerUrl = namingServerUrl;
        this.agentFileList = new HashMap<>();
    }

    /**
     * This method runs continuously:
     * - Scan local files via AgentUtils
     * - Retrieve file list from next node
     * - Synchronising metadata
     * - Checking locks and requesting them if necessary
     */
    @Override
    public void run() {
        while (true) {
            try {
                System.out.println("[SyncAgent] Synchronisation started...");

                // Detecting local files via AgentUtils
                Map<String, FileEntry> localFiles = AgentUtils.scanLocalFiles(storagePath, currentNodeIp);
                for (Map.Entry<String, FileEntry> entry : localFiles.entrySet()) {
                    agentFileList.putIfAbsent(entry.getKey(), entry.getValue());
                }

                // Retrieve file list from next node
                ResponseEntity<Map<String, FileEntry>> response = restTemplate.exchange(
                        nextNodeUrl + "/api/agent/filelist",
                        HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<Map<String, FileEntry>>() {}
                );
                Map<String, FileEntry> remoteList = response.getBody();

                // Compare lists
                for (Map.Entry<String, FileEntry> entry : remoteList.entrySet()) {
                    agentFileList.putIfAbsent(entry.getKey(), entry.getValue());
                }

                // Lock-check and update agentFileList if necessary
                for (Map.Entry<String, FileEntry> entry : agentFileList.entrySet()) {
                    FileEntry entryInAgentList = entry.getValue();
                    String filename = entryInAgentList.getFilename();
                    File localFile = new File(storagePath + "/" + filename + ".txt");

                    if (localFile.exists()) {
                        boolean shouldLock = filename.contains("lock_me"); // Simulate writing action

                        if (shouldLock && !entryInAgentList.isLocked()) {
                            System.out.println("[SyncAgent] Lock required for: " + filename);

                            // Apply a LOCK to all other nodes
                            List<String> allIps = fetchAllNodeIps();
                            requestLock(filename, allIps);

                            // Also set the lock locally
                            entryInAgentList.setLocked(true);
                            System.out.println("[SyncAgent] LOCK assigned for local file: " + filename);
                        }

                        if (entryInAgentList.isLocked()) {
                            System.out.println("[SyncAgent] File is locked: " + filename + " → no editing allowed.");
                        }


                    }
                }


                Thread.sleep(5000);

            } catch (Exception e) {
                System.err.println("[SyncAgent] Error during synchronisation: " + e.getMessage());
            }
        }
    }

    /**
     * Retrieves all node IPs from the naming server, excluding this node itself.
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
            System.err.println("[SyncAgent] Error retrieving IPs: " + e.getMessage());
            return List.of();
        }
    }

    /**
     * Requests a lock on all nodes for the specified file.
     *
     * @param filename    the file name for which a lock is requested
     * @param allNodeIps  list of IPs of all other nodes
     */
    public void requestLock(String filename, List<String> allNodeIps) {
        LockRequest req = new LockRequest(filename, currentNodeIp, "LOCK");
        req.setAction("LOCK");

        for (String ip : allNodeIps) {
            try {
                String url = "http://" + ip + ":8081/api/agent/lock";

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<LockRequest> request = new HttpEntity<>(req, headers);

                ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
                System.out.println("[LockRequest] Lock requested to " + ip + ": " + response.getStatusCode());

            } catch (Exception e) {
                System.err.println("[LockRequest] Error sending to " + ip + ": " + e.getMessage());
            }
        }
    }


    public Map<String, FileEntry> getAgentFileList() {
        return agentFileList;
    }
}
