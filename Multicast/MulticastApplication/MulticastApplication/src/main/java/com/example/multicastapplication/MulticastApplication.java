package com.example.multicastapplication;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@SpringBootApplication
public class MulticastApplication {

    public static void main(String[] args) {
        SpringApplication.run(MulticastApplication.class, args);
    }

    // ===== MODELOS =====

    record MessageId(long timestamp, int senderId)
            implements Comparable<MessageId> {

        @Override
        public int compareTo(MessageId o) {
            int c = Long.compare(this.timestamp, o.timestamp);
            return (c != 0) ? c : Integer.compare(this.senderId, o.senderId);
        }
    }

    record MulticastMessage(MessageId id, String payload) {}
    record AckMessage(MessageId messageId, int fromProcess) {}

    // ===== SERVIÇO =====

    @Service
    static class MulticastService {

        private final int processId;
        private final int totalProcesses;
        private final List<String> peers;
        private final RestTemplate restTemplate = new RestTemplate();
        private final Random random = new Random();

        private Long lamportClock = random.nextLong(20);

        private final PriorityQueue<MulticastMessage> pendingQueue =
                new PriorityQueue<>(Comparator.comparing(MulticastMessage::id));

        private final NavigableMap<MessageId, Set<Integer>> acks = new TreeMap<>();

        private final Object stateLock = new Object();

        MulticastService(
                @Value("${process.id}") int processId,
                @Value("${process.total}") int totalProcesses,
                @Value("${process.peers}") List<String> peers,
                @Value("${server.port}") int serverPort) {

            this.processId = processId;
            this.totalProcesses = totalProcesses;
            this.peers = peers.stream()
                    .filter(p -> !p.endsWith(":" + serverPort))
                    .toList();
        }

        // ===== MULTICAST =====

        void multicast(String payload) throws InterruptedException {
            MessageId id;
            MulticastMessage msg;

            synchronized (stateLock) {
                lamportClock++;
                id = new MessageId(lamportClock, processId);
                msg = new MulticastMessage(id, payload);

                pendingQueue.add(msg);
                acks.computeIfAbsent(id, k -> new HashSet<>())
                        .add(processId);
            }


            for (String peer : peers) {
                Thread.sleep(random.nextLong(1000,10000));

                System.out.println("Sending message: " + msg + " to " + peer);

                restTemplate.postForEntity(
                        peer + "/multicast/message",
                        msg,
                        Void.class
                );

                tryDeliver();
            }
        }

        // ===== RECEPÇÃO DE MENSAGEM =====

        void onMessage(MulticastMessage msg) {
            System.out.println("Receiving message: " + msg);

            synchronized (stateLock) {
                lamportClock = Math.max(lamportClock, msg.id.timestamp()) + 1;

                pendingQueue.removeIf(m -> m.id.equals(msg.id));
                pendingQueue.add(msg);

                acks.computeIfAbsent(msg.id(), k -> new HashSet<>())
                        .add(processId);
                acks.computeIfAbsent(msg.id(), k -> new HashSet<>()).add(msg.id.senderId);
            }

            AckMessage ack = new AckMessage(msg.id(), processId);

            for (String peer : peers) {
                System.out.println("Acking for message: " + msg + " to: " + peer);

                restTemplate.postForEntity(
                        peer + "/multicast/ack",
                        ack,
                        Void.class
                );

                tryDeliver();
            }
        }

        // ===== RECEPÇÃO DE ACK =====

        void onAck(AckMessage ack) {
            System.out.println("Receiving ack for message: " + ack);

            synchronized (stateLock) {
                acks.computeIfAbsent(ack.messageId(), k -> new HashSet<>())
                        .add(ack.fromProcess());
            }

            tryDeliver();
        }

        // ===== ENTREGA =====

        private void tryDeliver() {
            while (true) {
                MulticastMessage head;

                synchronized (stateLock) {
                    if (pendingQueue.isEmpty()) {
                        return;
                    }

                    head = pendingQueue.peek();
                    Set<Integer> s = acks.get(head.id());
                    MessageId smallest = acks.isEmpty() ? null : acks.firstKey();

                    if (s == null || s.size() < totalProcesses || (smallest != null && !head.id().equals(smallest)) ) {
                        return;
                    }

                    pendingQueue.poll();
                    acks.remove(head.id());
                }

                deliver(head);
            }
        }

        private void deliver(MulticastMessage msg) {
            System.out.println(
                    "[Process " + processId + "] DELIVER -> " + msg
            );
        }
    }

    // ===== CONTROLLER =====

    @RestController
    @RequestMapping("/multicast")
    static class MulticastController {

        private final MulticastService service;

        MulticastController(MulticastService service) {
            this.service = service;
        }

        @PostMapping("/send")
        public void send(@RequestBody String payload) throws InterruptedException {
            service.multicast(payload);
        }

        @PostMapping("/message")
        public void receiveMessage(@RequestBody MulticastMessage msg) {
            service.onMessage(msg);
        }

        @PostMapping("/ack")
        public void receiveAck(@RequestBody AckMessage ack) {
            service.onAck(ack);
        }
    }
}
