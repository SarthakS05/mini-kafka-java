# StreamForge Mini Broker

StreamForge is an original, compact Java project for learning the moving parts of a Kafka-like message log. It provides a TCP broker, durable topic partitions, sequential offsets, and small producer/consumer clients. It uses its own binary protocol and is not compatible with Kafka clients or brokers.

## Features

- Append-only, file-backed logs organized by topic and partition.
- Sequential offsets that survive broker restarts.
- Recovery that discards an incomplete trailing record after an interrupted write.
- Concurrent client connections and synchronized appends per partition.
- A small producer and consumer API plus a command-line interface.
- No third-party runtime dependencies.

## Requirements

Java 17 or later. Maven is optional for packaging; on Windows, the included PowerShell test runner uses `javac` directly.

## Build and test

With Maven:

```bash
mvn package
```

On Windows without Maven:

```powershell
.\scripts\test.ps1
```

The test runner compiles the project and runs storage-recovery and broker/client integration checks.

## Run locally

Start the broker in one terminal:

```bash
java -jar target/mini-kafka-java-1.0.0.jar broker 9092 ./data
```

Publish a message from another terminal:

```bash
java -jar target/mini-kafka-java-1.0.0.jar produce 127.0.0.1 9092 orders 0 "shipment packed"
```

Read records from offset 0:

```bash
java -jar target/mini-kafka-java-1.0.0.jar consume 127.0.0.1 9092 orders 0 0 20
```

The command forms are `broker [port] [data-directory]`, `produce <host> <port> <topic> <partition> <message>`, and `consume <host> <port> <topic> <partition> <offset> <max-records>`.

## Layout

- `storage/PartitionLog` owns a partition's append-only file and offset index.
- `broker/BrokerServer` accepts requests and routes them to partition logs.
- `protocol/Protocol` defines operation codes, validation, and frame limits.
- `client/Producer` and `client/Consumer` are thin TCP clients.
- `docs/protocol.md` describes the wire and on-disk formats.

## Scope

This is a learning project, not a production broker. It intentionally omits replication, leader election, consumer groups, transactions, authentication, and Kafka wire compatibility. Topic and partition files are local to one broker instance.
