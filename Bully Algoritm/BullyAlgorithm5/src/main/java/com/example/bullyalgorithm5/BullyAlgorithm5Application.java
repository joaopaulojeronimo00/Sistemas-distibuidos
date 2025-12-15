package com.example.bullyalgorithm5;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class BullyAlgorithm5Application {

    public static void main(String[] args) {
        SpringApplication.run(BullyAlgorithm5Application.class, args);
    }

    // ===== MODELOS =====

    record Message(int fromProcess, String payload) {
    }

    record Ack(int fromProcess) {
    }

    // ===== SERVIÇO =====

    @Service
    static class BullyService {

        private final int processId;
        private volatile int leaderId;
        private final int totalProcesses;
        private final Map<Integer, String> peers = new HashMap<>();
        private final RestTemplate restTemplate = new RestTemplate();

        private volatile boolean electionInProgress = false;

        BullyService(
                @Value("${process.id}") int processId,
                @Value("${process.total}") int totalProcesses,
                @Value("${process.leader}") int leaderId,
                @Value("${process.peers}") String peersRaw
        ) {
            this.processId = processId;
            this.totalProcesses = totalProcesses;
            this.leaderId = leaderId;

            String[] split = peersRaw.split(",");
            for (int i = 0; i < split.length; i++) {
                peers.put(i + 1, split[i]);
            }

            System.out.println("[INIT] Process " + processId + " | Leader=" + leaderId);
        }

        // ===== ENVIO DE MENSAGEM AO LÍDER =====

        public void sendToLeader(String payload) {
            if (leaderId == processId) {
                System.out.println("[LEADER] Message received: " + payload);
                return;
            }

            try {
                restTemplate.postForEntity(
                        peers.get(leaderId) + "/leader/message",
                        new Message(processId, payload),
                        Void.class
                );
            } catch (Exception e) {
                System.out.println("[FAIL] Leader not responding. Starting election...");
                startElection();
            }
        }

        // ===== INÍCIO DA ELEIÇÃO =====

        synchronized void startElection() {
            if (electionInProgress) return;
            electionInProgress = true;

            boolean higherProcessAlive = false;

            System.out.println("[ELECTION] Started by process " + processId);

            for (int i = processId + 1; i <= totalProcesses; i++) {
                try {
                    restTemplate.postForEntity(
                            peers.get(i) + "/election",
                            new Message(processId, "ELECTION"),
                            Void.class
                    );
                    higherProcessAlive = true;
                } catch (Exception ignored) {
                }
            }

            if (!higherProcessAlive) {
                becomeLeader();
            }
        }

        // ===== RECEPÇÃO DE ELEIÇÃO =====

        public void onElection(Message msg) {
            System.out.println("[ELECTION] Received from " + msg.fromProcess());

            restTemplate.postForEntity(
                    peers.get(msg.fromProcess()) + "/ack",
                    new Ack(processId),
                    Void.class
            );

            startElection();
        }

        // ===== RECEPÇÃO DE ACK =====

        public void onAck(Ack ack) {
            System.out.println("[ACK] From process " + ack.fromProcess());
        }

        // ===== TORNA-SE LÍDER =====

        synchronized void becomeLeader() {
            leaderId = processId;
            electionInProgress = false;

            System.out.println("[NEW LEADER] Process " + processId);

            for (Map.Entry<Integer, String> entry : peers.entrySet()) {
                if (entry.getKey() != processId) {
                    try {
                        restTemplate.postForEntity(
                                entry.getValue() + "/leader/new",
                                new Message(processId, "NEW_LEADER"),
                                Void.class
                        );
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        // ===== RECEPÇÃO DE NOVO LÍDER =====

        public void onNewLeader(Message msg) {
            leaderId = msg.fromProcess();
            electionInProgress = false;
            System.out.println("[UPDATE] New leader is " + leaderId);
        }

        // ===== SIMULAÇÃO DE FALHA DO LÍDER =====

        public void simulateFailure() throws InterruptedException {
            if (processId == leaderId) {
                System.out.println("[LEADER] Simulating failure...");
                Thread.sleep(30000);
            }
        }
    }

    // ===== CONTROLLER =====

    @RestController
    static class BullyController {

        private final BullyService service;

        BullyController(BullyService service) {
            this.service = service;
        }

        @PostMapping("/leader/message")
        void leaderMessage(@RequestBody Message msg) {
            service.sendToLeader(msg.payload());
        }

        @PostMapping("/send")
        void send(@RequestBody String payload) {
            service.sendToLeader(payload);
        }

        @PostMapping("/election")
        void election(@RequestBody Message msg) {
            service.onElection(msg);
        }

        @PostMapping("/ack")
        void ack(@RequestBody Ack ack) {
            service.onAck(ack);
        }

        @PostMapping("/leader/new")
        void newLeader(@RequestBody Message msg) {
            service.onNewLeader(msg);
        }

        @PostMapping("/simulate-failure")
        void simulateFailure() throws InterruptedException {
            service.simulateFailure();
        }
    }
}




