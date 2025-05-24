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
 * Failure Agent wordt opgestart wanneer een node faalt.
 * Hij reist rond in de ring en handelt bestanden van de falende node af:
 * - Als de volgende node het bestand niet heeft: verstuur bestand
 * - Als de volgende node het al heeft: update alleen metadata
 */
public class FailAgent implements Runnable, Serializable {
    private final int failingNodeId;
    private final int originNodeId;
    private final Node node;

    /**
     * Constructor voor FailAgent.
     * @param failingNodeId ID van de node die is uitgevallen
     * @param originNodeId ID van de node die de agent heeft gestart
     * @param node Referentie naar de huidige actieve node
     */
    public FailAgent(int failingNodeId, int originNodeId, Node node) {
        this.failingNodeId = failingNodeId;
        this.originNodeId = originNodeId;
        this.node = node;
    }

    /**
     * Hoofdlogica van de agent. Wordt uitgevoerd wanneer hij op een node toekomt:
     * - Scant bestanden lokaal
     * - Herverdeelt bestanden waarvan de eigenaar de falende node is
     * - Stuurt agent door naar volgende node tenzij we terug op start zijn
     */
    @Override
    public void run() {
        System.out.println("[FailAgent] Actief op node: " + node.getNodeName());

        // Stap 1: haal lokale filelist op
        node.updateFileListFromDisk();
        Map<String, FileEntry> fileList = node.getLocalFileList();

        for (Map.Entry<String, FileEntry> entry : fileList.entrySet()) {
            FileEntry file = entry.getValue();

            // Stap 2: is dit bestand eigendom van de falende node?
            if (file.getOwnerIp().equals(getIpOfNodeId(failingNodeId))) {

                System.out.println("[FailAgent] Bestand gevonden van falende node: " + file.getFilename());

                // Option 1 / Option 2 moeten hier nog uitgewerkt worden (file transfer vs log update)
                // TODO: check of de volgende eigenaar het bestand al heeft
                String filename = file.getFilename();
                String fullPath = "nodes_storage/" + node.getIpAddress() + "/" + filename + ".txt";
                File localFile = new File(fullPath);

                if (localFile.exists()) {
                    // 📨 Controleer of de volgende node dit bestand al heeft
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
                            // 🔄 Option 1: bestand bestaat NIET → verstuur het
                            System.out.println("[FailAgent] Bestand wordt verstuurd naar: " + nextIp);

                            byte[] fileBytes = Files.readAllBytes(localFile.toPath());

                            FileTransferRequest transfer = new FileTransferRequest(filename + ".txt", fileBytes);

                            HttpHeaders headers = new HttpHeaders();
                            headers.setContentType(MediaType.APPLICATION_JSON);
                            HttpEntity<FileTransferRequest> entity = new HttpEntity<>(transfer, headers);

                            rest.postForEntity("http://" + nextIp + ":8081/api/bootstrap/files/receive", entity, String.class);

                        } else {
                            // 📝 Option 2: bestand bestaat al → alleen metadata bijwerken
                            System.out.println("[FailAgent] Bestand bestaat al op volgende node → enkel log bijgewerkt");
                        }

                        // 🔄 In beide gevallen: update local metadata (nieuwe eigenaar)
                        file.setOwnerIp(node.getIpAddress());

                    } catch (Exception e) {
                        System.err.println("[FailAgent] Fout bij herverdeling bestand: " + e.getMessage());
                    }
                }
                // Voor nu gewoon log:
                System.out.println("[FailAgent] Log: bestand '" + file.getFilename() + "' had eigenaar " + file.getOwnerIp());
            }
        }

        // Stap 3: stoppen als we terug zijn op de startnode
        if (node.getCurrentID() == originNodeId) {
            System.out.println("[FailAgent] Terug op oorspronkelijke node → stop agent.");
            return;
        }

        // Stap 4: Agent doorsturen naar volgende node
        try {
            String nextIp = node.getIpFromNodeId(node.getNextID());
            String url = "http://" + nextIp + ":8081/api/agent/fail";

            RestTemplate rest = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<FailAgent> request = new HttpEntity<>(this, headers);
            rest.postForEntity(url, request, Void.class);

            System.out.println("[FailAgent] Doorgestuurd naar volgende node: " + nextIp);

        } catch (Exception e) {
            System.err.println("[FailAgent] Fout bij doorsturen naar volgende node: " + e.getMessage());
        }


    }

    /**
     * Simulatie van IP lookup van een node ID
     * (wordt later vervangen door echte lookup via naming server)
     */
    private String getIpOfNodeId(int nodeId) {
        // Simuleer mapping: kan vervangen worden met echte lookup
        return "192.168.0." + (nodeId % 256); // enkel tijdelijk voor tests
    }


    public int getOriginNodeId() {
        return originNodeId;
    }

    public int getFailingNodeId() {
        return failingNodeId;
    }

}
