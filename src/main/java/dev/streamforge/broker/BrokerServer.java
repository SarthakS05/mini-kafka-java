package dev.streamforge.broker;

import dev.streamforge.protocol.Protocol;
import dev.streamforge.storage.PartitionLog;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class BrokerServer implements AutoCloseable {
    private final String bindHost;
    private final int requestedPort;
    private final Path dataDirectory;
    private final Map<PartitionKey, PartitionLog> logs = new ConcurrentHashMap<>();
    private final ExecutorService handlers = Executors.newFixedThreadPool(16);

    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public BrokerServer(String bindHost, int port, Path dataDirectory) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }
        this.bindHost = bindHost;
        this.requestedPort = port;
        this.dataDirectory = dataDirectory;
    }

    public synchronized void start() throws IOException {
        if (running) {
            throw new IllegalStateException("Broker is already running");
        }
        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(bindHost, requestedPort));
        serverSocket = socket;
        running = true;
        acceptThread = new Thread(this::acceptLoop, "streamforge-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public int port() {
        if (serverSocket == null) {
            throw new IllegalStateException("Broker has not been started");
        }
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                handlers.execute(() -> handle(client));
            } catch (IOException exception) {
                if (running) {
                    System.err.println("Broker accept failed: " + exception.getMessage());
                }
                break;
            }
        }
    }

    private void handle(Socket client) {
        try (Socket socket = client) {
            socket.setSoTimeout(Protocol.SOCKET_TIMEOUT_MILLIS);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            try {
                int operation = input.readUnsignedByte();
                if (operation == Protocol.OP_PRODUCE) {
                    handleProduce(input, output);
                } else if (operation == Protocol.OP_FETCH) {
                    handleFetch(input, output);
                } else {
                    throw new IOException("Unknown operation: " + operation);
                }
            } catch (IOException | IllegalArgumentException exception) {
                writeError(output, exception.getMessage());
            }
        } catch (IOException ignored) {
            // The peer may disconnect before a complete request or response.
        }
    }

    private void handleProduce(DataInputStream input, DataOutputStream output) throws IOException {
        String topic = input.readUTF();
        int partition = input.readInt();
        int payloadLength = input.readInt();
        Protocol.validateTopic(topic);
        Protocol.validatePartition(partition);
        Protocol.validatePayloadLength(payloadLength);
        byte[] payload = new byte[payloadLength];
        input.readFully(payload);
        String value = new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        long offset = logFor(topic, partition).append(value);
        output.writeByte(Protocol.STATUS_OK);
        output.writeLong(offset);
        output.flush();
    }

    private void handleFetch(DataInputStream input, DataOutputStream output) throws IOException {
        String topic = input.readUTF();
        int partition = input.readInt();
        long offset = input.readLong();
        int limit = input.readInt();
        Protocol.validateTopic(topic);
        Protocol.validatePartition(partition);
        Protocol.validateFetch(offset, limit);
        List<PartitionLog.LogRecord> records = logFor(topic, partition).fetch(offset, limit);
        output.writeByte(Protocol.STATUS_OK);
        output.writeInt(records.size());
        for (PartitionLog.LogRecord record : records) {
            byte[] payload = record.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            output.writeLong(record.offset());
            output.writeLong(record.timestampMillis());
            output.writeInt(payload.length);
            output.write(payload);
        }
        output.flush();
    }

    private PartitionLog logFor(String topic, int partition) throws IOException {
        PartitionKey key = new PartitionKey(topic, partition);
        try {
            return logs.computeIfAbsent(key, ignored -> {
                Path path = dataDirectory.resolve(topic).resolve("partition-" + partition + ".log");
                try {
                    return new PartitionLog(path);
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            });
        } catch (UncheckedIOException exception) {
            throw exception.getCause();
        }
    }

    private static void writeError(DataOutputStream output, String message) throws IOException {
        String safeMessage = message == null ? "Request failed" : message;
        if (safeMessage.length() > 1_000) {
            safeMessage = safeMessage.substring(0, 1_000);
        }
        output.writeByte(Protocol.STATUS_ERROR);
        output.writeUTF(safeMessage);
        output.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        handlers.shutdown();
        try {
            if (!handlers.awaitTermination(5, TimeUnit.SECONDS)) {
                handlers.shutdownNow();
            }
        } catch (InterruptedException exception) {
            handlers.shutdownNow();
            Thread.currentThread().interrupt();
        }

        IOException closeFailure = null;
        for (PartitionLog log : logs.values()) {
            try {
                log.close();
            } catch (IOException exception) {
                if (closeFailure == null) {
                    closeFailure = exception;
                } else {
                    closeFailure.addSuppressed(exception);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private record PartitionKey(String topic, int partition) {
    }
}
