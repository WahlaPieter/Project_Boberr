package uantwerpen.be.fti.ei.Project.Agents;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

import java.io.Serializable;
import java.util.Map;

public class FailureMonitor implements Runnable, Serializable {
    private final Node node;
    private final RestTemplate rest;

    public FailureMonitor(Node node, RestTemplate rest) {
        this.node = node;
        this.rest = rest;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String nextIp = node.getIpFromNodeId(node.getNextID());
                String url = "http://" + nextIp + ":8081/api/bootstrap/state";

                ResponseEntity<Map> response = rest.getForEntity(url, Map.class);
                if (response.getStatusCode().is2xxSuccessful()) {
                    System.out.println("[FailMonitor] Next node reachable: " + nextIp);
                }

            } catch (Exception e) {
                System.err.println("[FailMonitor] Next node appears offline: " + node.getNextID());

                // Starting FailAgent on current node
                node.simulateFailureDetection(node.getNextID());

                // Skip temporary wait time so multiple FailAgents are not started
                try {
                    Thread.sleep(15000); // cooldown
                } catch (InterruptedException ignored) {}

                continue;
            }

            try {
                Thread.sleep(5000); // check each 5 sec
            } catch (InterruptedException ignored) {}
        }
    }


}
