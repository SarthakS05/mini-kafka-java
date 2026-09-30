package dev.streamforge.client;

import dev.streamforge.protocol.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class Producer {
    private final String host;
    private final int port;

    public Producer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public long send(String topic, int partition, String value) throws IOException {
        Protocol.validateTopic(topic);
        Protocol.validatePartition(partition);
        if (value == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        Protocol.validatePayloadLength(payload.length);

        try (Socket socket = connect();
             DataOutputStream output = new DataOutputStream(socket.getOutputStream());
             DataInputStream input = new DataInputStream(socket.getInputStream())) {
            output.writeByte(Protocol.OP_PRODUCE);
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeInt(payload.length);
            output.write(payload);
            output.flush();
            ensureSuccess(input);
            return input.readLong();
        }
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), Protocol.SOCKET_TIMEOUT_MILLIS);
        socket.setSoTimeout(Protocol.SOCKET_TIMEOUT_MILLIS);
        return socket;
    }

    static void ensureSuccess(DataInputStream input) throws IOException {
        int status = input.readUnsignedByte();
        if (status == Protocol.STATUS_OK) {
            return;
        }
        String message = input.readUTF();
        throw new IOException(message);
    }
}
