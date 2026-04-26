# I built a message broker from scratch. Here's what broke me.

![Bare Metal Engineering](https://images.unsplash.com/photo-1558494949-ef010cbdcc31?q=80&w=2000&auto=format&fit=crop)

I predicted that standard **FileOutputStream** would be much slower than memory-mapped I/O. It was the logical guess—stream I/O involves crossing OS boundaries, right? Then I ran the benchmark. The console spit back a staggering **953 MB/s** for FileOutputStream, completely dwarfing the **18.70 MB/s** I had just recorded for my MappedByteBuffer.

I felt like I knew absolutely nothing. The result violently contradicted the theory I had just learned about zero-copy architecture. But it forced me to realize I wasn't comparing apples to apples: I was comparing a memory-mapped buffer that forced a synchronous, blocking disk flush on every single write against a standard stream that was just happily dumping bytes into the OS page cache and returning immediately. FileOutputStream wasn't writing to disk at all at that point — it was writing to RAM. That's why it was fast.

---

I was trying to learn the way most juniors do: watching countless tutorial videos, then trying to rebuild whatever the video taught. The issue was that I could understand it perfectly while watching, but the minute I stepped away, it was gone. I couldn't recall the concepts, and I definitely couldn't reconstruct them or apply them correctly. It felt like I was lacking the necessary depth—I was consuming knowledge without anchoring it to anything real. That's why I finally decided to stop watching and start building.

The specific moment it hit me was right after I finished the first two chapters of Martin Kleppmann's *Designing Data-Intensive Applications*. I was reading these incredible explanations of storage engines and log-structured merge trees, but within a day, I could feel the understanding already slipping away. I could repeat the vocabulary, but I couldn't actually tell you how the bytes moved. I realized that if I didn't stop consuming tutorials and start writing raw code, I was going to lose the knowledge entirely. I needed to build it to make it permanent.

---

I spent the last three weeks building the bare-metal storage layer of a message broker from scratch in Java. Instead of using a traditional database that safely organizes and updates records, I built an append-only log. It’s essentially a massive, raw file on disk where messages are just appended to the end as fast as the physical hardware allows, using memory-mapped I/O. It’s designed so that multiple producers can write data concurrently without blocking each other, and consumers can read that data at the exact same time without slowing down the incoming firehose.

When I ran that test, seeing only 128 messages come back out of the 4,000 I wrote was a terrifying reality check. What made it so unsettling wasn't just that the data was destroyed, but that the system appeared to be working perfectly right up until the moment I actually inspected the data. **It taught me a lesson no tutorial ever could: the most dangerous bugs in production don't crash your servers—they cause silent data corruption.**

---

When I first ran four threads concurrently without synchronization and saw the data get destroyed, my immediate instinct was to reach for a lock. It’s what almost every tutorial teaches you: if multiple threads are fighting over a shared resource, wrap it in a synchronized block. Force them to stand in line. But forcing threads to stand in line completely destroys the massive throughput you get from memory-mapped files. We'd be burning precious time on context switching instead of actually writing data.

The breakthrough moment was discovering **Compare-And-Swap (CAS)** and using an **AtomicLong**. Instead of a pessimistic lock, CAS is an optimistic approach: *"I am going to read the current write position, calculate my new position, and tell the CPU to update it only if the value hasn't changed since I last looked."* What surprised me most was learning that CAS isn't just a clever Java software trick—it translates down to a single, indivisible hardware-level CPU instruction.

---

When I deliberately simulated a torn write—crashing the system halfway through an operation—I fully expected the consumer to blow up and throw a massive error. Instead, it cheerfully returned an array of 10,000 empty zeros and confidently handed it over as a perfectly valid message. The disk doesn't know what your data is supposed to mean; it just blindly returns the bytes that are physically sitting in that memory address. It proved that real systems fail by silently poisoning your application state, which is exactly why append-only architecture is useless if you don't have a cryptographic checksum to mathematically prove what the data actually is.

If I'm being honest, looking at all those missing pieces used to paralyze me. The fear of being an entry-level engineer trying to build complex systems without a mentor is something nobody talks about. You look at a production-ready system, then you look at your broken 1GB log file, and the gap feels insurmountable. You sit alone with your failing tests, wondering if you're fundamentally incapable of understanding this tier of engineering. But wrestling with this broken, isolated storage engine taught me why those missing pieces actually exist. I'm okay with what's broken because for the first time, I'm not just blindly copying a tutorial's architecture—I actually understand the bare-metal physics of what happens when a system lacks it.

---

### The Reality Check

If you look at the codebase right now, it’s missing almost everything that makes Kafka actually Kafka.

There is no network layer—producers and consumers have to run on the exact same machine. There is no segmentation, meaning once that single 1GB memory-mapped file fills up, the system just hits a wall and dies. And as my torn-write experiment proved, there is no CRC32 checksum, meaning a sudden power loss will leave the log full of silent, corrupted garbage. If you put this in production today, it would be a disaster.

**But I am completely okay with that. Why?**

Because you cannot build a reliable distributed system if you don't understand the physics of how a single machine actually writes to disk. Phase 1 wasn't about building a production-ready broker. It was about touching the bare metal. It was about wrestling with CPU caches, lock-free concurrency, and the operating system's page cache.

I essentially built the engine block. Adding the safety sensors (CRC32), the transmission (Segmentation), and the chassis (the Network Layer) is what comes next in Project 2 and 3. But for the first time in my career, I actually know why those pieces need to exist, rather than just blindly copying them from a tutorial. I know exactly what fails without them.

---

The feeling that you are completely out of your depth is exactly where you are supposed to be. When you are pushing through a strict ten-week sprint to be ready for the Q3 hiring window, every hour spent stuck on a single memory-mapped benchmark feels like you are falling fatally behind. The clock is ticking, and it is so incredibly tempting to just look up the answers so you can pass an Amazon Bar Raiser interview.

But that silent, agonizing struggle is the actual work. There is no mentor there to look at your corrupted data, laugh, and say, "Yeah, that happens to all of us, here is what to look at." You just sit there in the dark, doubting yourself. But wrestling with those broken threads entirely on your own is the only way you build deep, unshakable intuition. The frustration isn't blocking your progress; it is your progress. Sit with the broken code, trust the grind, and don't rush the realization.

This isn't finished. Project 2 starts next week — segmentation, crash recovery, CRC32. Then I'm building an order matching engine on top of it. I'm writing this not because I'm done, but because building in public keeps me honest.

---

**Here is the code link:** [https://github.com/Gowtham-beep/mini-kafaka.git](https://github.com/Gowtham-beep/mini-kafaka.git)
