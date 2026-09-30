package dev.streamforge.protocol;

import java.util.regex.Pattern;

public final class Protocol {
    public static final int OP_PRODUCE = 1;
    public static final int OP_FETCH = 2;
    public static final int STATUS_OK = 0;
    public static final int STATUS_ERROR = 1;
    public static final int MAX_PAYLOAD_BYTES = 1_048_576;
    public static final int MAX_FETCH_RECORDS = 500;
    public static final int SOCKET_TIMEOUT_MILLIS = 10_000;

    private static final Pattern TOPIC_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private Protocol() {
    }

    public static void validateTopic(String topic) {
        if (topic == null || !TOPIC_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException("Topic must be 1-128 ASCII letters, digits, dots, underscores, or hyphens");
        }
    }

    public static void validatePartition(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must be non-negative");
        }
    }

    public static void validatePayloadLength(int length) {
        if (length < 0 || length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Payload exceeds the 1 MiB limit");
        }
    }

    public static void validateFetch(long offset, int limit) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative");
        }
        if (limit < 1 || limit > MAX_FETCH_RECORDS) {
            throw new IllegalArgumentException("Fetch limit must be between 1 and 500");
        }
    }
}
