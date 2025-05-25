package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Agents.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.replication.FileTransferRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

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
     * Deze lijst wordt opgebouwd vanuit de 'nodes_storage/<ip>' folder.
     * Alleen nieuwe bestanden worden toegevoegd — bestaande blijven met lockstatus behouden.
     *
     * @return JSON: Map<String, FileEntry>
     */
    @GetMapping("/agent/filelist")
    public ResponseEntity<Map<String, FileEntry>> getFileList() {
        node.updateFileListFromDisk();
        return ResponseEntity.ok(node.getLocalFileList());
    }

    /**
     * Ontvangt een lock-aanvraag van een andere node.
     * Als het bestand nog niet gelockt is → zet lock.
     *
     * @param request JSON met 'filename' en 'requesterIp'
     * @return HTTP 200 OK bij succes, 409 als al gelockt, 404 als niet gevonden
     */
    @PostMapping("/agent/lock")
    public ResponseEntity<String> handleLockAction(@RequestBody LockRequest request) {
        String filename = request.getFilename();
        String action = request.getAction();

        node.updateFileListFromDisk();
        Map<String, FileEntry> fileList = node.getLocalFileList();

        FileEntry entry = fileList.get(filename);
        if (entry == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Bestand niet gevonden");
        }

        if ("LOCK".equalsIgnoreCase(action)) {
            if (!entry.isLocked()) {
                entry.setLocked(true);
                System.out.println("[LOCK] Bestand gelockt: " + filename + " op verzoek van " + request.getRequesterIp());
                return ResponseEntity.ok("Lock geaccepteerd");
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body("Bestand is al gelockt");
            }
        } else if ("UNLOCK".equalsIgnoreCase(action)) {
            if (entry.isLocked()) {
                entry.setLocked(false);
                System.out.println("[UNLOCK] Lock vrijgegeven voor: " + filename);
                return ResponseEntity.ok("Unlock geaccepteerd");
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body("Bestand was al niet gelockt");
            }
        }

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Ongeldige actie");
    }

        @PostMapping("/agent/fail")
    public ResponseEntity<Void> receiveFailAgent(@RequestBody FailAgent agent) {
        new Thread(agent).start();
        return ResponseEntity.ok().build();
    }




}