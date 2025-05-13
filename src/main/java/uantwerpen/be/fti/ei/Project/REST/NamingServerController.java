package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.NamingServer.NamingServer;

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

    @PostMapping("/files/report/{ip}")
    public ResponseEntity<Map<String, String>> reportFiles(@PathVariable String ip, @RequestBody Map<String, Integer> files) {
        Map<String, String> targets = namingServer.handleFileReport(ip, files); // Toegevoegd in namingserver
        return ResponseEntity.ok(targets);
    }

    @DeleteMapping("/files/remove/{ip}/{fileName}")
    public ResponseEntity<?> removeFile(@PathVariable String ip, @PathVariable String fileName) {
        namingServer.removeFile(ip, fileName);
        return ResponseEntity.ok().build();
    }

}