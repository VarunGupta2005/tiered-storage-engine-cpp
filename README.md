# High-Performance Tiered Storage Key-Value Engine

A robust, multi-tiered key-value storage engine implemented in C++17. This system is designed to demonstrate advanced concepts in storage architecture, including exclusive tiering, concurrency control, and durability.

## Core Architecture

This project implements a sophisticated storage engine with the following pillars:

1.  **Exclusive Tiering**: Data exists exclusively in **RAM** OR **Disk**. This optimizes for both performance (hot data in RAM) and cost/capacity (cold data on Disk).
2.  **Concurrency**: Utilizes **Reader-Writer Locks** (`std::shared_mutex`) to enable high-throughput parallel reads while ensuring data consistency during writes.
3.  **Durability (WAL)**: All operations are logged to a **Write-Ahead Log** before in-memory execution. This ensures data persists even after crashes. The log includes Atomic Runtime Compaction to manage file size.
4.  **Optimization**: Integrates **Bloom Filters** to probabilistically determine key existence, preventing 95% of unnecessary expensive disk I/O operations for non-existent keys.

## Features

- **Operations**: Support for standard `PUT`, `GET`, and `APPEND` operations.
- **Crash Recovery**: Automatically rebuilds in-memory state from the Write-Ahead Log upon startup.
- **System Observability**: A built-in metrics dashboard tracks detailed stats including:
    - Request counts (Read/Write/Modify)
    - RAM vs. Disk Hit Rates
    - Bloom Filter efficiency (Disk I/O saved)
    - Log compaction events

## Getting Started

### Prerequisites

- A C++17 compliant compiler (GCC, Clang, or MSVC).
- Make or CMake (optional, direct compilation is simple).

### Compilation

You can compile the project using `g++`:

```bash
g++ -std=c++17 -O3 src/main.cpp -o kv_store -pthread
```

### Usage

Run the compiled executable:

```bash
./kv_store
```

The application provides two modes:

1.  **Performance Benchmark**: Automatically runs a workload to test throughput and latency.
2.  **Interactive CLI**: A REPL environment to issue commands manually.

#### Interactive Commands

When in interactive mode, the store is initialized with a small RAM capacity to demonstrate tiering mechanisms.

| Command | Usage | Description |
| :--- | :--- | :--- |
| **PUT** | `PUT <key> <value>` | Insert or update a key-value pair. |
| **GET** | `GET <key>` | Retrieve the value for a key. |
| **APPEND** | `APPEND <key> <value>` | Append string data to an existing value. |
| **STATS** | `STATS` | Print the current system metrics dashboard. |
| **CRASH** | `CRASH` | Simulate a system crash |
| **EXIT** | `EXIT` | Close the application. |

## Project Structure

- `src/main.cpp`: Complete implementation including the KV Store class, Bloom Filter, WAL manager, and CLI interface.
