package dev.streamforge;

import dev.streamforge.broker.BrokerServer;
import dev.streamforge.client.Consumer;
import dev.streamforge.client.Producer;
import dev.streamforge.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public final class AllTests {
    private AllTests() {
    }

    public static void main(String[] args) throws Exception {
        testPartitionLogRecovery();
        testBrokerClientsAndConcurrentWrites();
        System.out.println("All StreamForge tests passed.");
    }

    private static void testPartitionLogRecovery() throws Exception {
        Path directory = Files.createTempDirectory("streamforge-log-test-");
        Path logPath = directory.resolve("partition-0.log");
        try {
            try (PartitionLog log = new PartitionLog(logPath)) {
                equal(0L, log.append("first"), "first offset");
                equal(1L, log.append("second"), "second offset");
            }
            Files.write(logPath, new byte[]{0, 1, 2}, StandardOpenOption.APPEND);
            try (PartitionLog log = new PartitionLog(logPath)) {
                List<PartitionLog.LogRecord> records = log.fetch(0, 10);
                equal(2, records.size(), "recovered record count");
                equal("first", records.get(0).value(), "first recovered message");
                equal("second", records.get(1).value(), "second recovered message");
                equal(2L, log.append("after-recovery"), "offset after recovery");
            }
            System.out.println("PASS partition log recovery");
        } finally {
            deleteTree(directory);
        }
    }

    private static void testBrokerClientsAndConcurrentWrites() throws Exception {
        Path directory = Files.createTempDirectory("streamforge-broker-test-");
        try (BrokerServer broker = new BrokerServer("127.0.0.1", 0, directory)) {
            broker.start();
            Producer producer = new Producer("127.0.0.1", broker.port());
            Consumer consumer = new Consumer("127.0.0.1", broker.port());

            equal(0L, producer.send("orders", 0, "one"), "first broker offset");
            equal(1L, producer.send("orders", 0, "two"), "second broker offset");
            List<Consumer.Message> initial = consumer.fetch("orders", 0, 0, 10);
            equal(2, initial.size(), "initial fetch size");
            equal("two", initial.get(1).value(), "second fetched value");
            equal(0, consumer.fetch("orders", 0, 2, 10).size(), "empty fetch at end");

            int workerCount = 6;
            int messagesPerWorker = 20;
            ExecutorService workers = Executors.newFixedThreadPool(workerCount);
            try {
                List<Callable<Long>> writes = new ArrayList<>();
                for (int worker = 0; worker < workerCount; worker++) {
                    int workerId = worker;
                    for (int message = 0; message < messagesPerWorker; message++) {
                        int messageId = message;
                        writes.add(() -> producer.send("orders", 0,
                                "worker-" + workerId + "-message-" + messageId));
                    }
                }
                List<Future<Long>> futures = workers.invokeAll(writes);
                Set<Long> offsets = new HashSet<>();
                for (Future<Long> future : futures) {
                    offsets.add(future.get());
                }
                equal(workerCount * messagesPerWorker, offsets.size(), "unique concurrent offsets");
                for (long offset = 2; offset < 2L + workerCount * messagesPerWorker; offset++) {
                    check(offsets.contains(offset), "missing concurrent offset " + offset);
                }
                equal(workerCount * messagesPerWorker + 2,
                        consumer.fetch("orders", 0, 0, 500).size(), "total fetched record count");
            } finally {
                workers.shutdownNow();
            }
            System.out.println("PASS broker/client integration and concurrent writes");
        } finally {
            deleteTree(directory);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void equal(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message + ": expected " + expected + ", got " + actual);
        }
    }
}
