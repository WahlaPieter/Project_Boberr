package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;


import java.util.Map;

@RestController
@RequestMapping("/api")
public class NodeController {
    private final NamingServer namingServer;

    @Autowired
    public NodeController(NamingServer namingServer) {
        this.namingServer = namingServer;
    }

    // Add a node to the system
    @PostMapping("/nodes")
    public ResponseEntity<?> addNode(@RequestBody NodeRegistrationRequest request) {
        boolean success = namingServer.addNode(request.getNodeName(), request.getIpAddress());

        if (success) {
            return ResponseEntity.ok().body(Map.of(
                    "status", "Node added successfully",
                    "nodeName", request.getNodeName(),
                    "ip", request.getIpAddress()
            ));
        } else {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "Node name already exists",
                    "nodeName", request.getNodeName()
            ));
        }
    }

    // Remove a node from the system
    @DeleteMapping("/nodes/{nodeId}")
    public ResponseEntity<?> removeNode(@PathVariable String nodeId) {
        boolean success = namingServer.removeNode(nodeId);

        if (success) {
            return ResponseEntity.ok().body(Map.of(
                    "status", "Node removed",
                    "nodeId", nodeId
            ));
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "error", "Node not found",
                    "nodeId", nodeId
            ));
        }
    }

    // Store a file (hash-based assignment)
    @PostMapping("/files/{fileName}")
    public ResponseEntity<?> storeFile(@PathVariable String fileName) {
        boolean success = namingServer.storeFile(fileName);
        if (success) {
            String ip = namingServer.findFileLocation(fileName);
            return ResponseEntity.ok(Map.of(
                    "fileName", fileName,
                    "ipAddress", ip,
                    "status", "Stored on node"
            ));
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "error", "No nodes available"
            ));
        }
    }

    // Lookup a file's location
    @GetMapping("/files/{fileName}")
    public ResponseEntity<?> findFileLocation(@PathVariable String fileName) {
        String ip = namingServer.findFileLocation(fileName);
        if (ip != null) {
            return ResponseEntity.ok(Map.of(
                    "fileName", fileName,
                    "ipAddress", ip
            ));
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "error", "File not found"
            ));
        }
    }

    @GetMapping("/nodemap")
    public ResponseEntity<?> getNodeMap() {
        return ResponseEntity.ok(namingServer.getNodeMap());
    }

    public static class NodeRegistrationRequest {
        private String nodeName;
        private String ipAddress;

        public String getNodeName() {
            return nodeName;
        }

        public void setNodeName(String nodeName) {
            this.nodeName = nodeName;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public void setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
        }
    }
}
