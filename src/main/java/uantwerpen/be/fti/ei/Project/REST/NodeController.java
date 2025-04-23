package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class NodeController {
    private final NamingServer namingServer;
    private final Node node;

    @Autowired
    public NodeController(NamingServer namingServer, Node node) {
        this.namingServer = namingServer;
        this.node = node;
    }

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

    @PutMapping("/nodes/{hash}")
    public ResponseEntity<?> updateNode(
            @PathVariable int hash,
            @RequestBody NodeUpdateRequest request
    ) {
        Node node = namingServer.getNodeMap().get(hash);
        if (node != null) {
            node.setPreviousID(request.getPreviousID());
            node.setNextID(request.getNextID());
            namingServer.saveNodeMap();
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    public static class NodeUpdateRequest {
        private int previousID;
        private int nextID;

        public int getNextID() {
            return nextID;
        }

        public void setNextID(int nextID) {
            this.nextID = nextID;
        }

        public int getPreviousID() {
            return previousID;
        }

        public void setPreviousID(int previousID) {
            this.previousID = previousID;
        }
    }

    public static class NodeRegistrationRequest {
        private String nodeName;
        private String ipAddress;

        public String getNodeName() { return nodeName; }
        public void setNodeName(String nodeName) { this.nodeName = nodeName; }

        public String getIpAddress() { return ipAddress; }
        public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
    }

    @PostMapping("/bootstrap/update")
    public ResponseEntity<?> updateRingInfo(@RequestBody Map<String, Integer> payload) {
        int updatedField = payload.get("updatedField");
        int nodeID = payload.get("nodeID");

        if (updatedField == 1) {
            node.setPreviousID(nodeID);
        } else if (updatedField == 2) {
            node.setNextID(nodeID);
        }

        String statusMessage;
        if (updatedField == 1) {
            statusMessage = "Updated previousID";
        } else {
            statusMessage = "Updated nextID";
        }

        return ResponseEntity.ok(Map.of(
                "status", statusMessage,
                "with", nodeID
        ));

    }

}
