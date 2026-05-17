# I Built Kafka's Storage Engine From Scratch. Here's Every Decision I Made

I came in knowing Kafka uses an append-only log. I didn't know why.

That gap — using something without understanding it — is what started this project. As a backend developer making the jump into FinTech and low-latency systems, I realized that 'knowing the API' is no longer enough. When you are building a high-throughput system like an order matching engine, you can't just throw a message broker at the architecture and hope it scales. You have to know what happens to the hardware when 50 concurrent threads try to write to the same file at the exact same microsecond.

So, rather than just reading the documentation, I decided to build one from scratch. First principles. No external libraries. Just Java 21, memory-mapped files (MappedByteBuffer), AtomicLong, and some brutal lessons in CPU architecture.

What started as a simple file-writer quickly turned into a war against the Operating System scheduler. Here is how I built a concurrent, lock-free segmented log, the terrifying race conditions that brought it to its knees, and the architecture required to fix it.

---

## Section 1: The Illusion of In-Place Updates (Why Append-Only?)

The question that finally cracked it open for me wasn't about performance. It was about failure: **What happens if the JVM crashes halfway through a write?**

### The Frankenstein Record

Imagine you are building a storage engine that allows **in-place mutation**. You want to update a 100-byte record sitting in the middle of a file.

The disk spins up, the head aligns, and the Operating System starts overwriting bytes 500 through 600. It gets to byte 550, and then someone trips over the server's power cord.

When the server reboots, what exactly is sitting at byte 500?

- The first 50 bytes belong to the **new** record.
- The last 50 bytes belong to the **old** record.

You are left with a Frankenstein record—half-old, half-new data. Worse, unless you have meticulously placed checksums on every single byte block, the storage engine has absolutely no way to tell which half is which. The structural boundary of the file looks completely normal, but the data inside is silently corrupted.

### The Beauty of the Tail

Now look at the **append-only** model. You never modify existing bytes. You only ever write to the absolute end of the file.

If the power cord gets tripped halfway through an append, you still have a corrupted record. But the corruption is *physically isolated to the tail*.

When the broker reboots, crash recovery is mathematically trivial. The engine scans the file sequentially. The moment it hits a record at the end of the file that is missing bytes or fails its CRC32 checksum, it stops. It simply truncates the file exactly at that boundary, throwing away the torn write. In a fraction of a second, the storage engine is back to a perfectly healthy, 100% consistent state.

### The "Aha!" Moment

Understanding this led me down a rabbit hole regarding traditional relational databases. Postgres and MySQL allow in-place mutations using B-Trees. So how do they survive power failures without corrupting their data pages?

The answer is the **Write-Ahead Log (WAL)**. Before Postgres is allowed to mutate a B-Tree page on the hard drive, it must first write the intent to do so into the WAL. And what data structure is the WAL?

It is an append-only log.

That was the "aha!" moment that completely shifted my perspective: **Every database that supports safe in-place writes secretly depends on an append-only log underneath.** The append-only log isn't just a quirky Kafka optimization for streaming. It is the fundamental recovery primitive of all durable software.

### The Unanswered Question

By strictly appending, I knew I could guarantee structural integrity. If a write tore, I could detect it and chop off the tail.

But as I began sketching out the multi-threaded Java implementation, a darker question emerged: *Append-only solves structural corruption. But what if the structure is perfectly intact, but the bytes contain the wrong data?* What if the OS pauses a thread right after it reserves its space in the file, but right before it writes its payload? The bytes are there. The length is valid. But the payload is just a ghost-town of uninitialized zeros.

Append-only couldn't save me from the Operating System's thread scheduler. That realization forced me into the deepest and most painful part of this project—but we'll get to the "Hole Problem" in Section 4.

---

## Section 2: Segmentation — When One File Isn't Enough

The elegance of the append-only log felt like a revelation, but clarity is usually just a bridge to a brand new kind of confusion.

I had my core primitive: a single, indestructible file where bytes are only ever added to the tail. But as I started sketching out the memory management for the broker, the reality of a single, infinite file hit me. It creates three massive, concrete engineering problems:

1. **Deletion is impossible:** Disks aren't infinite. Eventually, you have to delete old messages. But how do you delete the first 100 bytes of a 500GB file? You have to shift the remaining 499.99GB of data backward to fill the gap. That is an in-place mutation, which completely violates the append-only rule we just established.
2. **Memory constraints:** To get zero-copy reads and high-performance writes, you have to map the file directly into RAM using a `MappedByteBuffer`. You cannot realistically map an infinitely growing, multi-terabyte file into memory.
3. **Unbounded crash recovery:** If the server crashes, I knew I could recover by scanning the tail. But if the file is a terabyte long, how long does it take the broker to boot up and verify the entire structure? The recovery time grows unbounded as the system ages.

The solution wasn't to change how the log worked, but to change how the file system viewed it. The infinite log is just an abstraction. Physically, you have to slice it up.

### The Base Offset and the 20-Digit Filename

I decided to break the log into manageable chunks called **Segments** (in my case, 1GB each).

But how do you name them so the broker can instantly find message number `8,492,011`? You name the physical file after its **Base Offset**—the logical ID of the very first message contained inside it.

When I first looked at Kafka's internals, I noticed the files looked like this: `00000000000000000000.log`. I originally thought the 20-digit zero-padding was just an aesthetic choice to make the logs look aligned in the terminal.

It isn't aesthetic at all. It is highly functional.

By zero-padding the filenames to a fixed length, **lexicographic sorting is perfectly identical to chronological sorting.** When a consumer asks for offset `500`, the broker doesn't have to open and read a bunch of files. It can just ask the Operating System for the directory listing and perform a binary search directly on the filenames themselves to find exactly which file contains that offset. You are using the OS file tree as your first-level index.

### The Lifecycle of Immutability

This segmentation architecture fundamentally changes how you think about the data.

When a segment file hits its size threshold (e.g., 1GB), it triggers a rotation. A new active segment is created. But more importantly, the old segment is permanently "sealed." It transitions into a read-only state.

This was a major paradigm shift for me: **When a file is full, it transforms into a read-only archive. The append-only property doesn't just apply to the bytes within a segment—it applies to the lifecycle of the segment itself.**

### O(1) Deletion

Suddenly, the impossible problem of deleting old data became trivial.

If my FinTech order matching engine has a retention policy of "delete data older than 7 days," I don't have to run a complex garbage collection process that scrubs through files and rewrites data. The broker just looks at the array of sealed segments, finds the ones that are too old, and issues a standard `File.delete()` command to the Operating System.

It is an $O(1)$ operation. The append-only guarantee is flawlessly preserved because I am never touching or modifying the bytes inside any file. I am just dropping entire sealed files off the edge of the earth.

With the physical storage sliced, sealed, and searchable, the disk architecture was solid. But knowing *which* file a message is in is only half the battle. Next, I had to figure out how to find a 100-byte message inside a 1 Gigabyte file without scanning from the beginning every single time.

---

## Section 3: The Sparse Index and the Architecture of Zero-Allocation

With the 1 Gigabyte `MappedByteBuffer` fully implemented and the rotation logic handling the files, the storage engine could theoretically handle unbounded data. But a log isn't a black hole; it's a pipe. Data has to come out as fast as it goes in.

This introduced the biggest routing problem of the architecture. A consumer holds a logical offset—say, `1,750,000`. You have twenty 1GB files on disk. How do you find the exact bytes of that message without reading the entire system from the beginning?

### The Index Format

In my previous project, an "offset" was just the raw byte position in a single file. But with multiple segments, that concept broke down. I needed a way to map a virtual, sequential ID (the **logical offset**) to an exact physical location on the disk (the **byte position**).

The solution is an index file (`.index`) that sits alongside every `.log` file.

I designed the index to use fixed-width, 16-byte entries:

- `[8 bytes logical offset]`
- `[8 bytes physical position]`

Making the entries strictly fixed-width wasn't just a formatting choice. It enables $O(1)$ math. If you want the 50th entry in the index, you don't have to scan the file; you simply seek directly to byte `50 * 16`.

### Dense vs. Sparse (The RAM Cost)

The immediate instinct is to create a **dense index**: every single message gets a 16-byte entry in the index file.

But when I ran the numbers, the math fell apart. If a broker processes a billion messages a day, a dense index grows by 16 Gigabytes a day. In a high-performance system, the index must be kept in RAM so readers can binary search it instantly. A dense index completely defeats the purpose of memory-mapping because the index itself becomes too large to fit in memory.

The alternative is the **sparse index**.

Instead of tracking every message, the sparse index only drops a pin every 4 Kilobytes of data. It is a periodic anchor.

If a consumer asks for offset `1,750,000`, the binary search inside the sparse index might only find an anchor for offset `1,749,980`.

1. The broker jumps to the physical byte position of the anchor.
2. It reads the length prefix of that message (`4 bytes`).
3. It skips ahead by that length to find the next message (`1,749,981`).
4. It repeats this jump 20 times until it lands on the exact bytes for `1,750,000`.

### The Synergy of Primitives

While writing the forward scan logic, I hit a temporary wall: *As I jump forward, how do I know the logical offset of the record I am looking at? Do I need to store the 8-byte logical offset inside the message itself?*

That felt redundant. And it was. It led me to one of the most satisfying "click" moments of the project. I wrote down this exact definition:

> *"The sparse index physically stores only periodic anchor mappings of a logical sequence number to its exact physical byte, and derives the identity of all intermediate messages dynamically by jumping to the nearest anchor and counting forward through the unbroken sequence of length-prefixed records."*

The logical offset is entirely implicit. This design only works because of three decisions that are utterly dependent on each other:

1. **Append-only:** Records are never reordered. Counting forward always yields the correct mathematical ID.
2. **Length-prefixing:** You don't have to scan the bytes looking for a delimiter. You read 4 bytes, and you instantly know exactly how far to jump.
3. **The Sparse Index:** Provides the $O(1)$ entry point.

These aren't three separate features. They are a single, cohesive architecture.

### The Hot Path and Zero-Allocation

The final piece of the routing puzzle was determining *which* segment file to look at. I had a list of `Segment` objects in memory. The standard Java approach to finding the right segment is `Collections.binarySearch()`.

I started writing it, but stopped. To use Java's built-in binary search, I would have to instantiate a dummy `Segment` object just to pass the target offset into the comparator.

Consumers call `read()` millions of times per second. That is the ultimate "hot path." If I create a dummy object on the heap for every single read, the Java Garbage Collector will eventually have to clean up millions of useless objects. That triggers a "Stop The World" GC pause. In a low-latency trading system, a 50-millisecond GC pause means the price of the stock moved before you processed the trade.

I rejected the built-in library and wrote a custom primitive `while` loop to search the array without allocating a single object.

This is the rule of ultra-low-latency systems like Aeron and the LMAX Disruptor: **Zero allocation on the hot path.** If you rely on the garbage collector to clean up your routing logic, your system isn't actually fast—it's just deferring the cost of its slowness.

---

## Section 4: CRC32 — What Append-Only Cannot Catch

By the time I finished the sparse index, I felt invincible. The architecture was coming together beautifully. Between the zero-allocation routing and the append-only durability, I thought I had built an indestructible storage primitive.

If the server crashed, the append-only design would save me. If a write was interrupted, the file would simply end abruptly (EOF). I would read the 4-byte length prefix, realize there weren't enough payload bytes left in the file to satisfy it, and instantly detect the torn write. Structural corruption was solved.

Then I deliberately broke the concurrency model, and my confidence evaporated.

### The Ghost Bytes

In what I called "Phase 4" of the project, I deliberately ran four producer threads with no synchronization to observe what corruption actually looked like. Everything seemed to run perfectly, until the consumer verification loop threw a bizarre error.

A consumer asked for a specific offset and received a perfectly sized byte array. The length prefix was completely intact. The file structure was completely unbroken. There was no EOF error.

The problem? The payload was just 10,000 uninitialized zeros.

The structure looked perfectly valid, but the data was a ghost town. Because of a microscopic race condition in my memory-mapped CAS loop, a thread had reserved the space, updated the length prefix, and then the Operating System put it to sleep before it could write the actual data.

Append-only had absolutely no way to catch this. To the append-only parser, a 10,000-byte array of zeros is just as valid as a 10,000-byte array of JSON.

### The Precise Gap

I realized I had conflated two entirely different types of data integrity. I wrote down this exact distinction to force myself to separate them:

**Append-only tells you where to look. CRC32 tells you whether what you found is valid. They solve different problems and you need both.**

The append-only structure protects the *container*. But to protect the *contents*, the record must be able to cryptographically prove its own validity. I needed a checksum.

### The Format Decision

I went back to the drawing board and finalized the exact physical anatomy of a record on disk. Every single message written to the log follows this strict, unchangeable format:

`[4 bytes length] [N bytes payload] [4 bytes CRC32]`

Notice what is missing? The logical offset.

As I realized in Section 3, the logical offset lives exclusively in the index and the implicit sequence. The physical record on disk carries absolutely no routing metadata. It carries only the exact mathematical properties required to parse and validate itself.

When the broker reads a record, it hashes the payload using the CRC32 algorithm and compares the result to the final 4 bytes. If they don't match, the reader instantly throws a `CorruptRecordException`.

### The Dual-Purpose Primitive

Adding CRC32 felt like a necessary tax to pay for read-time safety. But the deeper I got, the more I realized its true value was in crash recovery.

When a database crashes and reboots, it has to figure out what data is safe and what data is corrupted. With this architecture, crash recovery is remarkably elegant. The broker boots up, finds the highest valid anchor in the sparse index, and just starts scanning forward through the raw `.log` file.

It reads the length, reads the payload, and calculates the CRC32.

- Match. Safe.
- Match. Safe.
- CRC32 mismatch. **Stop.**

The moment that checksum fails, the broker knows with mathematical certainty that it has found the exact microsecond where the power cord was pulled or the server panicked. It drops an axe right at that byte boundary, truncates the file, and the engine is healthy again.

The checksum that detects silent memory corruption at read time is the exact same mechanism that finds the recovery boundary after a total system failure. One feature, two critical purposes.

---

## Section 5: The Concurrency Design (Or, How I Learned to Stop Worrying and Love CAS)

With the sparse index mapping my logical offsets and CRC32 protecting the payloads, I had a bulletproof storage engine—as long as exactly one thread was using it.

But message brokers don’t live in a single-threaded vacuum. In the real world, 50 different producer threads are going to hammer the `append()` method at the exact same microsecond. My goal was to make this engine lock-free on the hot path. No `synchronized` blocks stalling the CPU while threads wait in line to write to the file.

Moving from a single-threaded mental model to a highly concurrent one was a masterclass in surprising complexity. It broke almost every assumption I had.

### The Two Counters Insight

My first major bug happened before I even ran a stress test. I originally tried to derive the logical message offset directly from the `writePosition`.

The flaw in that logic became obvious the moment I introduced variable-length payloads. `writePosition` tracks *bytes*, not *messages*. If Thread A writes 100 bytes and Thread B writes 50 bytes, you cannot mathematically divide the byte position to figure out what message ID you are on. Conflating the two is a fatal math error.

I needed two entirely separate tracking mechanisms:

1. `AtomicLong writePosition`: Tracks physical bytes to prevent threads from overwriting each other.
2. `AtomicLong messageCount`: Tracks the logical sequence ID.

### The CAS Reservation Pattern

To keep the hot path lock-free, I used the same Compare-And-Swap (CAS) pattern I had experimented with in earlier iterations.

When a thread enters `append()`, it doesn't lock the file. Instead, it calculates how many bytes it needs, looks at the current `writePosition`, and attempts a CAS: `writePosition.compareAndSet(expected, expected + myBytes)`.

If it wins the CAS, that thread officially owns that specific byte range. No other thread can touch it. Because of this exclusive mathematical ownership, the thread can safely call `logBuffer.duplicate()` to create a thread-local pointer to the physical memory map and write its payload. 50 threads can be actively copying bytes into 50 different regions of the exact same file simultaneously, with zero locking.

### The Index Sampling Race

While the payloads were writing safely, my sparse index started acting completely erratically. I wanted to drop an index anchor every 4,000 bytes. My code looked like this:

```java
if (bytePos - lastIndexedBytePos >= 4000) {
    writeIndexEntry(logicalOffset, bytePos);
    lastIndexedBytePos = bytePos;
}
```

This is a classic "check-then-act" race condition.

Thread A passes the `if` check. Before it can update `lastIndexedBytePos`, the OS pauses it. Thread B comes along, also passes the `if` check, and both threads write an index entry. I was getting duplicate anchors right next to each other.

The fix was to collapse the check and the act into a single atomic operation. I changed it so the thread attempts a CAS on `lastIndexedBytePos`. The thread that wins the CAS gets the privilege of writing the index entry. The loser? It just moves on. In a sparse index, dropping an anchor a few bytes late isn't an error—it's perfectly correct behavior.

### The Order of Immutability (Sealing the File)

When a segment file hits 1GB, it has to be "sealed" and transformed into a read-only archive. But the order in which you execute the seal is the difference between data survival and catastrophic loss.

I had to reason through a server crash scenario to get the order right:

1. `logBuffer.force()` (Flush data to disk)
2. `indexBuffer.force()` (Flush index to disk)
3. `isSealed = true` (Update in-memory state)

Why this specific order? **You must make durable state safe before updating in-memory state.** If I set `isSealed = true` first, and the JVM crashed before `force()` executed, the system would reboot, look at the file, and the tail messages would be gone forever. By forcing the disk first, if a crash happens midway, the data is physically safe. On reboot, the `SegmentedLog` simply reads the filesystem, realizes the file is full, and correctly rebuilds the in-memory `isSealed` state. It is the exact same principle as Write-Ahead Logging.

### The Volatile Decisions

Speaking of `isSealed`, it had to be declared as a `volatile boolean`.

In Java, threads can cache variables in their local CPU registers. If a background thread seals the segment and sets `isSealed = true`, but my producer thread has `isSealed = false` cached in its L1 cache, the producer will walk straight past the validation checks and try to write to a closed file, throwing a massive `ClosedChannelException`.

The same rule applied to `currentSegment` inside the `SegmentedLog`. When a rotation happens, the active segment pointer is updated. Making it `volatile` guarantees that all 50 producers instantly see the new file reference and route their traffic accordingly.

### Double-Checked Locking on the Cold Path

Which brings us to the rotation itself. The hot path (`append`) is lock-free, but when the segment fills up and throws an `IllegalStateException`, someone has to create the new 1GB file.

I couldn't use CAS for this because creating a file and allocating a `MappedByteBuffer` involves heavy I/O. So, I implemented **Double-Checked Locking**.

- **Check outside the lock:** A cheap, volatile read to see if rotation is needed.
- **Check inside the lock:** To ensure another thread didn't already rotate the file while you were waiting for the lock.

Only one thread ever actually calls `rotate()`. The other 49 threads hit the lock, wait, enter the block, realize the `currentSegment` has already been updated by the winner, and immediately retry their `append()` on the brand new file.

### ReentrantLock(false) over Synchronized

For the lock itself, I specifically chose `new ReentrantLock(false)` (unfair mode) over a standard `synchronized` block.

File creation and memory mapping are slow operations. Blocking threads that can't rotate is the correct architectural choice because they literally have nowhere to write until the new file exists. But why unfair mode? Because when the new file is finally ready, the ordering of which thread wakes up first to write its payload absolutely doesn't matter. Forcing the OS to maintain a strict FIFO queue for waking up threads just adds context-switching overhead. An unfair lock minimizes latency and lets the CPU tear through the backlog as efficiently as possible.

With the threads perfectly orchestrated, the math holding steady, and the disk safely flushing, I was finally ready to face the absolute hardest bug of the entire project.

---

## Closing: What This Taught Me

When I started this project, I just wanted to understand the mechanics of an append-only log. What I ended up with was a completely rewired mental model of how data actually moves through a computer.

Looking back at the architecture, three distinct lessons stand out.

### 1. Architecture is Synergy, Not a Checklist

The append-only log, the sparse index, and length-prefixing are not three separate features. They are a single, indivisible mechanism. If you remove the length prefix, the sparse index can't jump forward. If you remove the append-only rule, the math of counting forward shatters. Remove any one of them, and the other two stop working entirely. That is the kind of structural interdependence you only truly understand by building it from scratch.

### 2. Design-by-Failure > Design-by-Intuition

Every major design decision I made was forced by a failure mode I either observed or reasoned through. The CRC32 checksum wasn't a proactive best practice; it came from watching 10,000 uninitialized zeros pass as a valid message. The two-counter design (separating byte positions from logical offsets) came from a math error I caught on a whiteboard before it became a catastrophic bug. Building this engine taught me that designing by failure is significantly faster and more robust than designing by intuition.

### 3. The Gap Between Using and Understanding

I started this journey with a surface-level confidence. I could name Kafka's features in an interview. Now, I can explain *why* Kafka uses a segmented append-only log in a way that traces directly back to OS-level I/O, the Java Memory Model, and hardware-level atomics. That is the exact gap between an engineer who has used a system, and an engineer who actually understands it.

### What's Next?

With the architecture complete and the memory-mapped concurrency seemingly flawless, I thought I was done.

If you want to go deeper or see how these ideas translate into actual Java code, the storage engine implementation is available here: [mini-kafaka storage engine source code](https://github.com/Gowtham-beep/mini-kafaka/tree/main/src/main/java/com/minibroker/log).

Then I stress-tested it with 100 threads and 1 million messages.

It passed.

Then I ran it again. It failed at offset `970,898`.

The third run failed at a different offset. A flaky test is the most frightening thing in concurrent programming, because it means the bug exists but won't show itself consistently.

That bug had a name. Finding it is Part 2.

---

## Reference

- [mini-kafaka storage engine source code](https://github.com/Gowtham-beep/mini-kafaka/tree/main/src/main/java/com/minibroker/log)
