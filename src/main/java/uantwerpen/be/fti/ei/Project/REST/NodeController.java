package uantwerpen.be.fti.ei.Project.REST;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

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
}