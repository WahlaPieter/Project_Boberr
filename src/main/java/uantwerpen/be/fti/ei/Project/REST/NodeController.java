package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.replication.FileReplicator;

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
    public ResponseEntity<?> update(@RequestBody Map<String, Integer> payload) {
        int field = payload.get("updatedField");
        int id = payload.get("nodeID");
        if (field == 1) node.updatePrevious(id);
        else if (field == 2) node.updateNext(id);
        return ResponseEntity.ok(Map.of("status","updated"));
    }
    @PostMapping("/files/receive")
    public ResponseEntity<?> receiveFile(@RequestBody byte[] fileData) {
        try (InputStream in = new ByteArrayInputStream(fileData)) {
            byte[] receivedData = FileReplicator.receiveFile("received_file", in);
            // Save the file to storage
            Files.write(Paths.get("/storage/received_file"), receivedData);
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }
}