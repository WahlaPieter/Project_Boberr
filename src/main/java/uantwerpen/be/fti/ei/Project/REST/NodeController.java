package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Agents.AgentUtils;
import uantwerpen.be.fti.ei.Project.Agents.LockRequest;
import uantwerpen.be.fti.ei.Project.Agents.SyncAgent;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.replication.FileTransferRequest;
import uantwerpen.be.fti.ei.Project.Agents.FileEntry;
import org.springframework.http.HttpStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api/bootstrap")
@Profile("node")
public class NodeController {

    @Autowired
    private Node node;

    @PostMapping("/update")
    public ResponseEntity<?> update(@RequestBody Map<String,Object> p){
        int field = (int) p.get("updatedField");
        if (field == 1)      node.updatePrevious((int) p.get("nodeID"));
        else if (field == 2) node.updateNext((int) p.get("nodeID"));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/files/receive")
    public ResponseEntity<?> receiveFile(@RequestBody FileTransferRequest request) {
        try (InputStream in = new ByteArrayInputStream(request.getData())) {
            byte[] receivedData = FileReplicator.receiveFile(in);
            Files.write(Paths.get("/storage", request.getFileName()), receivedData);
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @PostMapping("/info")
    public ResponseEntity<Void> receiveCount(@RequestBody Map<String,Integer> m){
        node.setInitialCount(m.get("count"));
        return ResponseEntity.ok().build();
    }

    @GetMapping("/state")
    public Map<String, Object> nodeState() {
        return Map.of(
                "nodeName",  node.getNodeName(),
                "ipAddress", node.getIpAddress(),
                "currentID", node.getCurrentID(),
                "previousID", node.getPreviousID(),
                "nextID",     node.getNextID()
        );
    }

    /**
     * REST-endpoint dat een lijst teruggeeft van alle bestanden
     * die lokaal op deze node aanwezig zijn.
     *
     * Deze lijst wordt automatisch opgebouwd op basis van fysieke .txt-bestanden
     * in de opslagmap van de node, en gebruikt door SyncAgents voor synchronisatie.
     *
     * @return Map van bestandsnamen naar FileEntry-objecten (JSON)
     */
    @GetMapping("/agent/filelist")
    public ResponseEntity<Map<String, FileEntry>> getFileList() {
        String nodeIp = node.getIpAddress(); // Verkrijg IP van deze node
        String path = "nodes_storage/" + nodeIp; // Correct pad naar opslag

        Map<String, FileEntry> scannedList = AgentUtils.scanLocalFiles(path, nodeIp);
        return ResponseEntity.ok(scannedList);
    }

    @PostMapping("/agent/lock")
    public ResponseEntity<String> lockFile(@RequestBody LockRequest request) {
        String filename = request.getFilename();

        // Scan lokale bestanden
        Map<String, FileEntry> fileList = AgentUtils.scanLocalFiles("nodes_storage/" + node.getIpAddress(), node.getIpAddress());

        if (fileList.containsKey(filename)) {
            FileEntry entry = fileList.get(filename);
            if (!entry.isLocked()) {
                entry.setLocked(true);
                System.out.println("[LOCK] Bestand gelockt: " + filename + " op verzoek van " + request.getRequesterIp());
                return ResponseEntity.ok("Lock geaccepteerd");
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body("Bestand is al gelockt");
            }
        }

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Bestand niet gevonden");
    }

}