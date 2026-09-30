package dev.streamforge.client;

import dev.streamforge.protocol.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class Consumer {
    private final String host;
    private final int port;

    public Consumer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public List<Message> fetch(String topic, int partition, long offset, int limit) throws IOException {
        Protocol.validateTopic(topic);
        Protocol.validatePartition(partition);
        Protocol.validateFetch(offset, limit);

        try (Socket socket = connect();
             DataOutputStream output = new DataOutputStream(socket.getOutputStream());
             DataInputStream input = new DataInputStream(socket.getInputStream())) {
            output.writeByte(Protocol.OP_FETCH);
            output.writeUTF(topic);
            output.writeInt(partition);
            output.writeLong(offset);
            output.writeInt(limit);
            output.flush();

            Producer.ensureSuccess(input);
            int count = input.readInt();
            if (count < 0 || count > limit) {
                throw new IOException("Broker returned an invalid record count");
            }
            List<Message> messages = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                long recordOffset = input.readLong();
                long timestampMillis = input.readLong();
                int payloadLength = input.readInt();
                Protocol.validatePayloadLength(payloadLength);
                byte[] payload = new byte[payloadLength];
                input.readFully(payload);
                messages.add(new Message(recordOffset, timestampMillis,
                        new String(payload, StandardCharsets.UTF_8)));
            }
            return List.copyOf(messages);
        }
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), Protocol.SOCKET_TIMEOUT_MILLIS);
        socket.setSoTimeout(Protocol.SOCKET_TIMEOUT_MILLIS);
        return socket;
    }

    public record Message(long offset, long timestampMillis, String value) {
    }
}
