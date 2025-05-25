package uantwerpen.be.fti.ei.Project.Agents;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;

import java.nio.file.Files;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.RestTemplate;
import uantwerpen.be.fti.ei.Project.replication.FileTransferRequest;
import org.springframework.http.ResponseEntity;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

/**
 * Failure Agent is started when a node fails.
 * It travels around the ring and handles files from the failing node:
 * - If the next node does not have the file: send file
 * - If the next node already has it: update metadata only
 */
public class FailAgent implements Runnable, Serializable {
    private final int failingNodeId;
    private final int originNodeId;
    private final Node node;

    public FailAgent(int failingNodeId, int originNodeId, Node node) {
        this.failingNodeId = failingNodeId;
        this.originNodeId = originNodeId;
        this.node = node;
    }

    /**
     * Principal agent logic. Runs when it arrives on a node:
     * - Scans files locally
     * - Redistributes files whose owner is the failing node
     * - Forwards agent to next node unless we are back at startup
     */
    @Override
    public void run() {
        System.out.println("[FailAgent] Active on node: " + node.getNodeName());

        // Retrieve local filelist
        node.updateFileListFromDisk();
        Map<String, FileEntry> fileList = node.getLocalFileList();

        for (Map.Entry<String, FileEntry> entry : fileList.entrySet()) {
            FileEntry file = entry.getValue();

            // Is this file owned by the failing node?
            if (file.getOwnerIp().equals(node.getIpFromNodeId(failingNodeId))) {

                System.out.println("[FailAgent] File found from failing node: " + file.getFilename());

                // Option 1 / Option 2, (file transfer vs log update)
                String filename = file.getFilename();
                String fullPath = "nodes_storage/" + node.getIpAddress() + "/" + filename + ".txt";
                File localFile = new File(fullPath);

                if (localFile.exists()) {
                    // Check whether the next node already has this file
                    String nextIp = node.getIpFromNodeId(node.getNextID());
                    String checkUrl = "http://" + nextIp + ":8081/api/bootstrap/agent/filelist";

                    try {
                        RestTemplate rest = new RestTemplate();
                        ResponseEntity<Map<String, FileEntry>> response = rest.exchange(
                                checkUrl,
                                HttpMethod.GET,
                                null,
                                new ParameterizedTypeReference<Map<String, FileEntry>>() {}
                        );

                        Map<String, FileEntry> nextFileList = response.getBody();

                        if (nextFileList != null && !nextFileList.containsKey(filename)) {
                            // Option 1: file does NOT exist → send it
                            System.out.println("[FailAgent] Bestand wordt verstuurd naar: " + nextIp);

                            byte[] fileBytes = Files.readAllBytes(localFile.toPath());

                            FileTransferRequest transfer = new FileTransferRequest(filename + ".txt", fileBytes);

                            HttpHeaders headers = new HttpHeaders();
                            headers.setContentType(MediaType.APPLICATION_JSON);
                            HttpEntity<FileTransferRequest> entity = new HttpEntity<>(transfer, headers);

                            rest.postForEntity("http://" + nextIp + ":8081/api/bootstrap/files/receive", entity, String.class);

                        } else {
                            // Option 2: file already exists → update metadata only
                            System.out.println("[FailAgent] File already exists on next node → only log updated");
                        }

                        // In both cases: update local metadata (new owner)
                        file.setOwnerIp(node.getIpAddress());

                    } catch (Exception e) {
                        System.err.println("[FailAgent] Error on file redistribution: " + e.getMessage());
                    }
                }
                // For now, just log:
                System.out.println("[FailAgent] Log: file '" + file.getFilename() + "' had owner " + file.getOwnerIp());
            }
        }

        // stop when we are back at the startnode
        if (node.getCurrentID() == originNodeId) {
            System.out.println("[FailAgent] Back on original node → stop agent.");
            return;
        }

        // Forward agent to next node
        try {
            String nextIp = node.getIpFromNodeId(node.getNextID());
            String url = "http://" + nextIp + ":8081/api/agent/fail";

            RestTemplate rest = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<FailAgent> request = new HttpEntity<>(this, headers);
            rest.postForEntity(url, request, Void.class);

            System.out.println("[FailAgent] Forwarded to next node: " + nextIp);

        } catch (Exception e) {
            System.err.println("[FailAgent] Error on forwarding to next node: " + e.getMessage());
        }

    }

    public int getOriginNodeId() {
        return originNodeId;
    }

    public int getFailingNodeId() {
        return failingNodeId;
    }

}
