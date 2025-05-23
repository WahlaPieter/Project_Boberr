package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;
import uantwerpen.be.fti.ei.Project.replication.FileTransferRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

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

    @PostMapping("/files/delete-local-copy")
    public ResponseEntity<String> deleteLocalFileCopy(@RequestBody Map<String, String> payload) {

        String fileNameToDelete = payload.get("fileName");
        if (fileNameToDelete == null || fileNameToDelete.isEmpty()) {
            return ResponseEntity.badRequest().body("fileName not provided.");
        }

        System.out.println("Node '" + node.getNodeName() + "': Received instruction to delete local copy of: " + fileNameToDelete);
        boolean success = node.deleteLocalFile(fileNameToDelete); // Implement this method in Node.java

        if (success) {
            return ResponseEntity.ok("Local file " + fileNameToDelete + " deleted.");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to delete local file " + fileNameToDelete);
        }
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

    @GetMapping("/gui/state")              // één node (Home-details popup)
    public Map<String,Object> guiState() {
        return Map.of(
                "name",  node.getNodeName(),
                "hash",  node.getCurrentID(),
                "ip",    node.getIpAddress(),
                "prev",  node.getPreviousID(),
                "next",  node.getNextID()
        );
    }
}