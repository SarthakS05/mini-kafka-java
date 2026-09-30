package dev.streamforge.storage;

import dev.streamforge.protocol.Protocol;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class PartitionLog implements AutoCloseable {
    private static final int TIMESTAMP_BYTES = Long.BYTES;

    private final RandomAccessFile file;
    private final List<Long> recordPositions = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public PartitionLog(Path filePath) throws IOException {
        Path parent = filePath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        file = new RandomAccessFile(filePath.toFile(), "rw");
        recover();
    }

    public long append(String value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        byte[] payload = value.getBytes(StandardCharsets.UTF_8);
        Protocol.validatePayloadLength(payload.length);

        lock.writeLock().lock();
        try {
            long position = file.length();
            long offset = recordPositions.size();
            try {
                file.seek(position);
                file.writeInt(TIMESTAMP_BYTES + payload.length);
                file.writeLong(Instant.now().toEpochMilli());
                file.write(payload);
                file.getFD().sync();
                recordPositions.add(position);
                return offset;
            } catch (IOException exception) {
                file.setLength(position);
                throw exception;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<LogRecord> fetch(long offset, int limit) throws IOException {
        Protocol.validateFetch(offset, limit);
        lock.readLock().lock();
        try {
            int start = (int) Math.min(offset, recordPositions.size());
            int end = Math.min(recordPositions.size(), start + limit);
            List<LogRecord> records = new ArrayList<>(end - start);
            for (int index = start; index < end; index++) {
                records.add(readRecord(index));
            }
            return List.copyOf(records);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recover() throws IOException {
        lock.writeLock().lock();
        try {
            long fileLength = file.length();
            long position = 0;
            while (position < fileLength) {
                if (fileLength - position < Integer.BYTES) {
                    break;
                }
                file.seek(position);
                int recordLength = file.readInt();
                if (recordLength < TIMESTAMP_BYTES
                        || recordLength > TIMESTAMP_BYTES + Protocol.MAX_PAYLOAD_BYTES
                        || fileLength - position - Integer.BYTES < recordLength) {
                    break;
                }
                recordPositions.add(position);
                position += Integer.BYTES + (long) recordLength;
            }
            if (position != fileLength) {
                file.setLength(position);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private LogRecord readRecord(int offset) throws IOException {
        file.seek(recordPositions.get(offset));
        int recordLength = file.readInt();
        long timestampMillis = file.readLong();
        byte[] payload = new byte[recordLength - TIMESTAMP_BYTES];
        file.readFully(payload);
        return new LogRecord(offset, timestampMillis, new String(payload, StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            file.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public record LogRecord(long offset, long timestampMillis, String value) {
    }
}
