package uantwerpen.be.fti.ei.Project.Agents;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import uantwerpen.be.fti.ei.Project.Bootstrap.Node;

import java.io.Serializable;
import java.util.Map;

/**
 * Monitor die periodiek de volgende node controleert.
 * Bij detectie van failure wordt automatisch een FailAgent gestart.
 */

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
                    System.out.println("[FailMonitor] Next node bereikbaar: " + nextIp);
                }

            } catch (Exception e) {
                System.err.println("[FailMonitor] ❗ Next node lijkt offline: " + node.getNextID());

                // FailAgent starten op huidige node
                node.simulateFailureDetection(node.getNextID());

                // Sla tijdelijk wachttijd over zodat niet meerdere FailAgents worden gestart
                try {
                    Thread.sleep(15000); // cooldown
                } catch (InterruptedException ignored) {}

                continue;
            }

            try {
                Thread.sleep(5000); // check elke 5 seconden
            } catch (InterruptedException ignored) {}
        }
    }


}
