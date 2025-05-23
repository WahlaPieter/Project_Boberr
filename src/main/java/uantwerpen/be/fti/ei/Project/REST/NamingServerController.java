package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;
import uantwerpen.be.fti.ei.Project.NamingServer.HashingUtil;
import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;
import uantwerpen.be.fti.ei.Project.replication.FileLogEntry;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@Profile("namingserver")
public class NamingServerController {
    @Autowired
    private NamingServer namingServer;

    @PostMapping("/nodes")
    public ResponseEntity<?> addNode(@RequestBody Map<String, String> req) {
        boolean ok = namingServer.addNode(req.get("nodeName"), req.get("ipAddress"));
        return ok ? ResponseEntity.ok(Map.of("status","ok")) : ResponseEntity.status(409).body(Map.of("error","exists"));
    }

    @DeleteMapping("/nodes/{hash}")
    public ResponseEntity<?> removeNode(@PathVariable int hash) {
        return namingServer.removeNode(hash) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @PutMapping("/nodes/{hash}/previous")
    public ResponseEntity<?> updatePrev(@PathVariable int hash, @RequestBody int prev) {
        var node = namingServer.getNodeMap().get(hash);
        if (node==null) return ResponseEntity.notFound().build();
        node.setPreviousID(prev);
        namingServer.saveNodeMap();
        return ResponseEntity.ok().build();
    }

    @PutMapping("/nodes/{hash}/next")
    public ResponseEntity<?> updateNext(@PathVariable int hash, @RequestBody int next) {
        var node = namingServer.getNodeMap().get(hash);
        if (node==null) return ResponseEntity.notFound().build();
        node.setNextID(next);
        namingServer.saveNodeMap();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/files/{fileName}")
    public ResponseEntity<?> storeFile(@PathVariable String fileName) {
        boolean ok = namingServer.storeFile(fileName);
        if (!ok) return ResponseEntity.status(503).body(Map.of("error","no nodes"));
        String ip = namingServer.findFileLocation(fileName);
        return ResponseEntity.ok(Map.of("fileName",fileName,"ipAddress",ip));
    }

    @GetMapping("/files/{fileName}")
    public ResponseEntity<?> findFile(@PathVariable String fileName) {
        String ip = namingServer.findFileLocation(fileName);
        return ip != null ? ResponseEntity.ok(Map.of("fileName",fileName,"ipAddress",ip)) : ResponseEntity.notFound().build();
    }

    @GetMapping("/nodemap")
    public Map<Integer, ?> getMap() {
        return namingServer.getNodeMap();
    }

    @GetMapping("/replicate")
    public ResponseEntity<?> getReplicationTarget(@RequestParam int hash) {
        Map<String, Object> targetInfo = namingServer.getNodeForReplication(hash);
        return targetInfo.get("ip") != null ? ResponseEntity.ok(Map.of("ip", targetInfo.get("ip"))) : ResponseEntity.notFound().build();
    }

    @PostMapping("/files/replicate")
    public ResponseEntity<?> registerReplication(
            @RequestBody Map<String, String> payload) {
        namingServer.registerFileReplication(
                payload.get("fileName"),
                payload.get("ownerIp"),
                payload.get("replicaIp"));
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/files/{fileName}/replicas/{replicaIp}")
    public ResponseEntity<?> removeReplica(
            @PathVariable String fileName, @PathVariable String replicaIp) {
        namingServer.removeFileReplica(fileName, replicaIp);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/nodes/{hash}/replicated")
    public ResponseEntity<?> getReplicatedFiles(@PathVariable int hash) {
        var replicas = namingServer.getReplicatedFilesForNode(hash);
        return ResponseEntity.ok(replicas);
    }

    @GetMapping("/nodes/ip/{shuttingDownNodeIp}/held-replicas")
    public ResponseEntity<List<Map<String, String>>> getFilesHeldAsReplicas(
            @PathVariable String shuttingDownNodeIp) {
        List<Map<String, String>> heldReplicas = namingServer.getFilesHeldAsReplicasByNode(shuttingDownNodeIp);
        if (heldReplicas == null) { // NamingServer service might return null if nodeIp not found
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(heldReplicas);
    }

    @GetMapping("/nodes/ip/{currentNodeIp}/previous-node-contact")
    public ResponseEntity<Map<String, String>> getPreviousNodeContactDetails(
            @PathVariable String currentNodeIp) {
        Map<String, String> contactInfo = namingServer.getPreviousNodeContact(currentNodeIp);
        if (contactInfo == null || !contactInfo.containsKey("ip")) {
            return ResponseEntity.notFound().build(); // No previous node or info incomplete
        }
        return ResponseEntity.ok(contactInfo);
    }

    @PostMapping("/files/replicas/move")
    public ResponseEntity<Void> updateMovedReplicaLocation(@RequestBody Map<String, String> payload) {
        String fileName = payload.get("fileName");
        String newReplicaHolderIp = payload.get("newReplicaHolderIp"); // The previous node that received the file
        String oldReplicaHolderIp = payload.get("oldReplicaHolderIp"); // The shutting down node

        if (fileName == null || newReplicaHolderIp == null || oldReplicaHolderIp == null) {
            return ResponseEntity.badRequest().build();
        }
        namingServer.moveReplicaLocation(fileName, newReplicaHolderIp, oldReplicaHolderIp);
        return ResponseEntity.ok().build();
    }

    // Your existing /nodes/{hash}/shouldReplicate - seems fine for querying
    @GetMapping("/nodes/{hash}/shouldReplicate")
    public ResponseEntity<?> shouldReplicate(@PathVariable int hash,
                                             @RequestParam String file) {
        int fileHash = HashingUtil.generateHash(file);
        // Assuming getNodeForReplication now returns a map with IP and filePort
        Map<String, Object> targetInfo = namingServer.getNodeForReplication(fileHash);
        if (targetInfo != null) {
            return ResponseEntity.ok(Map.of(
                    "file", file,
                    "fileHash", fileHash,
                    "targetNodeIp", targetInfo
            ));
        }
        return ResponseEntity.notFound().build(); // Or appropriate response if no target
    }

    @GetMapping("/filelogs")
    public ResponseEntity<Map<String, FileLogEntry>> getFileLogs() {
        Map<String, FileLogEntry> logs = namingServer.getFileLogs(); // Assuming NamingServer has this getter
        if (logs == null || logs.isEmpty()) {
            return ResponseEntity.noContent().build(); // Or OK with empty map
        }
        return ResponseEntity.ok(logs);
    }

    @GetMapping("/nodes/{hash}/files")
    public ResponseEntity<?> listFiles(@PathVariable int hash) {
        return ResponseEntity.ok(namingServer.getFilesOfNode(hash));
    }

    @GetMapping("/gui/nodes")
    public List<Map<String,Object>> guiNodes() {
        return namingServer.getNodeMap().values().stream()
                .map(n -> Map.<String,Object>of(      //  ← let op   <String,Object>
                        "name", n.getNodeName(),
                        "hash", n.getCurrentID(),
                        "ip",   n.getIpAddress(),
                        "prev", n.getPreviousID(),
                        "next", n.getNextID()))
                .toList();
    }

    @GetMapping("/gui/files/{hash}")       // local+replica lijst (Files tab)
    public Map<String,List<String>> guiFiles(@PathVariable int hash) {
        return namingServer.getFilesOfNode(hash);
    }

    /**
     * Geeft het IP-adres van deze naming-server terug aan de GUI.
     */
    @GetMapping("/get-IP-server")
    public ResponseEntity<Map<String,String>> getNamingServerIp() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "127.0.0.1";
        }
        return ResponseEntity.ok(Map.of("ip", ip));
    }

}