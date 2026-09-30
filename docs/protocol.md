# Wire and Storage Protocol

StreamForge uses one TCP connection per request. Integer values use Java `DataInputStream`/`DataOutputStream` big-endian encoding. Topic names use `writeUTF`/`readUTF`; message payloads are UTF-8. A response begins with one status byte: `0` for success or `1` for an error.

## Produce

Request fields, in order:

1. Operation byte `1`.
2. Topic name (`writeUTF`).
3. Partition number (`int`).
4. Payload byte length (`int`), followed by that many payload bytes.

A successful response contains the status byte and the assigned offset (`long`). An error response contains the status byte and a UTF error message.

## Fetch

Request fields, in order:

1. Operation byte `2`.
2. Topic name (`writeUTF`).
3. Partition number (`int`).
4. Starting offset (`long`), inclusive.
5. Maximum record count (`int`), from 1 through 500.

A successful response contains the status byte, record count (`int`), then each record's offset (`long`), timestamp in Unix milliseconds (`long`), payload length (`int`), and payload bytes. Fetching from an offset at or past the current end returns an empty batch.

## On-disk records

Each partition is one append-only file. A record is stored as a four-byte record length, an eight-byte timestamp, and the UTF-8 payload. The record length includes the timestamp and payload, but not its own four-byte prefix. Offsets are zero-based record sequence numbers and are rebuilt by scanning the file at startup. An incomplete or invalid trailing record is truncated during recovery.

## Limits

Topic names are 1-128 ASCII letters, digits, dots, underscores, or hyphens. Partition numbers must be non-negative. Payloads are limited to 1 MiB, and a fetch returns at most 500 records.
