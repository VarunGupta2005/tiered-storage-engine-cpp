# 🚀 Hybrid Event-Streaming KV Store: Benchmarks

Welcome to the official performance benchmarks for the Hybrid Event-Streaming Key-Value Store. 

These tests were executed using `10,000` operations compiled with strict `g++ -O3` optimizations. The goal was to prove the mechanical sympathy of **Append-Only Logs** and the extreme read efficiency of **CQRS Read Models** backed by **Bloom Filters**.

---

## 📊 1. Executive Summary

| Operation | Methodology | Time (10k ops) | Throughput (ops/sec) | Big-O Complexity |
| :--- | :--- | :--- | :--- | :--- |
| **Write** | Append-Only Log | 0.033s | **295,482** | `O(1)` (Sequential) |
| **Write** | Random File I/O | 8.097s | 1,234 | `O(1)` (Random Seek) |
| **Read (Hit)** | Hash Map (`unordered_map` + Bloom) | 0.002s | 3,927,729 | `O(1)` |
| **Read (Hit)** | Binary Tree (`std::map` raw) | 0.001s | **8,218,277** | `O(log N)` |
| **Read (Miss)**| Bloom Filter Rejection | 0.001s | **5,523,031** | `O(1)` |

---

## 💾 2. The Storage Layer (Writes)

### The 239x Speedup: Mechanical Sympathy
The most staggering result is the difference between the **Append-Only Log** (295k ops/sec) and **Random File I/O** (1.2k ops/sec). 

* **Random File I/O:** When creating a separate file for every key (or seeking to random offsets in a giant file), the Operating System incurs massive overhead. It must allocate file descriptors, update inodes, and physically move the disk head (on HDDs) or trigger random Flash Translation Layer blocks (on SSDs).
* **Append-Only Log:** By opening a single `events.log` file with `std::ios::app`, we achieve **Mechanical Sympathy**. The disk writes continuously to the end of the file. The OS aggressively batches these writes in the Page Cache and flushes them sequentially. This is the exact mechanism that allows distributed systems like Apache Kafka to achieve millions of writes per second on standard hardware.

---

## ⚡ 3. In-Memory Lookups (Reads)

### The `O(1)` vs `O(log N)` Anomaly
In computer science, `O(1)` Hash Maps are theoretically faster than `O(log N)` Binary Search Trees. However, in our benchmark, the raw `std::map` outperformed the `KVCachePlugin`'s Hash Map approach by a factor of 2x. 

**Why did this happen?**
1. **The Bloom Filter Overhead**: Our Hash Map approach is not a raw `std::unordered_map`. Every time `kvCache->get(key)` is called, the system mathematically calculates **three separate string hashes** (two for the Bloom Filter, one for the Hash Map). String hashing is CPU-intensive.
2. **CPU Cache Locality**: At N = 10,000, the entire `std::map` easily fits inside the CPU's L2/L3 cache. Traversing the red-black tree (which just involves pointer jumping and fast string prefix comparisons) is mechanically faster for the CPU than calculating three complex mathematical hashes.
3. **The Crossover Point**: If we scaled this benchmark to N = 100,000,000, the `std::map` tree would become too large for the CPU cache. Memory fetches from RAM would cause cache misses, and the `O(log N)` traversal would slow down drastically. At that scale, the `O(1)` Hash Map would effortlessly overtake the Binary Tree.

---

## 🛡️ 4. The Bloom Filter (Cache Misses)

The true power of the `KVCachePlugin` shines in Test 5. When a user requests a key that does not exist (`invalid_user_X`), the Bloom Filter intercepts the request.

Instead of navigating the complex buckets of the `std::unordered_map` (or God forbid, querying the disk), the Bloom Filter calculates two quick hashes, checks a bit-array, and instantly returns `false`. This achieves **5.5 Million operations per second**, effectively shielding the core database from being bogged down by invalid requests or malicious DDOS attacks trying to exhaust database resources.

---

## 🏗️ 5. Architectural Conclusion

The benchmarks prove that separating concerns using **CQRS** is highly effective:
1. **The Command Side** leverages Append-Only logs to maximize raw disk throughput.
2. **The Query Side** flattens complex event streams into fast in-memory Hash Maps.
3. **Optimizations** like Bloom Filters selectively trade CPU cycles (hashing overhead) for safety and massive speedups on cache misses.
