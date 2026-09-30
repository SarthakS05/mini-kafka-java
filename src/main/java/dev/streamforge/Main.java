package dev.streamforge;

import dev.streamforge.broker.BrokerServer;
import dev.streamforge.client.Consumer;
import dev.streamforge.client.Producer;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception exception) {
            System.err.println("Error: " + exception.getMessage());
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "broker" -> runBroker(args);
            case "produce" -> runProducer(args);
            case "consume" -> runConsumer(args);
            default -> printUsage();
        }
    }

    private static void runBroker(String[] args) throws Exception {
        if (args.length > 3) {
            printUsage();
            return;
        }
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 9092;
        Path dataDirectory = args.length >= 3 ? Path.of(args[2]) : Path.of("data");
        BrokerServer broker = new BrokerServer("0.0.0.0", port, dataDirectory);
        broker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                broker.close();
            } catch (Exception exception) {
                System.err.println("Broker shutdown failed: " + exception.getMessage());
            }
        }, "streamforge-shutdown"));
        System.out.printf("StreamForge broker listening on port %d; data directory: %s%n",
                broker.port(), dataDirectory.toAbsolutePath());
        new CountDownLatch(1).await();
    }

    private static void runProducer(String[] args) throws Exception {
        if (args.length < 6) {
            printUsage();
            return;
        }
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String topic = args[3];
        int partition = Integer.parseInt(args[4]);
        String value = String.join(" ", List.of(args).subList(5, args.length));
        long offset = new Producer(host, port).send(topic, partition, value);
        System.out.println(offset);
    }

    private static void runConsumer(String[] args) throws Exception {
        if (args.length != 7) {
            printUsage();
            return;
        }
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String topic = args[3];
        int partition = Integer.parseInt(args[4]);
        long offset = Long.parseLong(args[5]);
        int limit = Integer.parseInt(args[6]);
        for (Consumer.Message message : new Consumer(host, port).fetch(topic, partition, offset, limit)) {
            System.out.printf("%d\t%d\t%s%n", message.offset(), message.timestampMillis(), message.value());
        }
    }

    private static void printUsage() {
        System.out.println("StreamForge Mini Broker\n"
                + "  broker [port] [data-directory]\n"
                + "  produce <host> <port> <topic> <partition> <message>\n"
                + "  consume <host> <port> <topic> <partition> <offset> <max-records>");
    }
}
