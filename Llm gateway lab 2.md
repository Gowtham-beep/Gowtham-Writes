# My Rate Limiter Told 25 Users to Wait 6.7 Minutes. The Real Wait Was 90 Seconds.

![Token Bucket Rate Limiter for AI LLM Gateway](./assets/llm_gateway_hero.jpg)

*AI/MLOps Experiment Lab #2 — building a token-bucket rate limiter for a local LLM gateway, and finding out why LLM token budgeting is a harder version of a "solved" problem.*

---

## The idea

Stage 1 of this lab series proved that bounding worker concurrency doesn't protect you from a slow downstream dependency — Ollama serialized requests regardless of what BullMQ thought it was doing. Stage 2 asks the next question every production LLM system has to answer: **what happens when you cap how much of that dependency any single burst of traffic is allowed to consume?**

This is the same question Groq, OpenAI, and Anthropic answer every time they return a 429. I wanted to build the client-side mirror of what they run — not to reinvent rate limiting, but to hit the same edges they've clearly already engineered around, and understand exactly why those edges exist.

I did not expect the "why" to be as interesting as it turned out to be.

---

## What I built

A Redis-backed token bucket sitting between the API and the BullMQ queue from Stage 1:

```
Client → Fastify API → [TOKEN BUCKET CHECK] → BullMQ Queue → Worker → Ollama
```

The bucket enforces a TPM (tokens-per-minute) budget using the same algorithm Anthropic documents using for Claude's API: capacity that replenishes continuously up to a maximum, rather than resetting at fixed intervals. Every check-and-deduct happens atomically inside a single Redis Lua script — the same class of read-modify-write race condition I'd already hit once in Stage 1 (BullMQ's `state`/`returnvalue` bug), so I wasn't going to risk it again with hand-rolled `GET`/`SET` pairs.

Two settlement phases, mirroring how OpenAI's API actually works: OpenAI counts tokens toward your limit based on the maximum you allow — the sum of input tokens plus `max_tokens` — as the potential cost of a request, then settles against real usage after.

1. **Pre-flight**: reserve `estimatedInputTokens + maxOutputTokens` before the job ever reaches the queue. If the bucket can't cover it, reject with an HTTP 429 immediately — the request never touches Ollama.
2. **Post-flight**: after the LLM responds, refund whatever was reserved but not actually used.

That sounds simple. It was not simple to get right, and every wrong turn taught me something I wouldn't have learned from a clean implementation.

---

## Finding #1: static token estimation is fundamentally the wrong tool

My first pre-flight estimator guessed output cost from input word count: `words × 1.3`. It was wrong by an average of **670%**. A 5-word prompt ("Count from 1 to 5") reserved 7 tokens and actually consumed 55-94. The estimator wasn't miscalibrated — it was measuring something with almost no relationship to what it needed to predict. Input length tells you almost nothing about how many tokens a model will choose to generate.

**The fix wasn't a better formula. It was removing the guess entirely**, using Ollama's `num_predict` parameter — the same mechanism as OpenAI's `max_tokens` — to cap generation at a hard ceiling, then reserve that ceiling instead of estimating it.

```
if a request must be capped at 150 output tokens:
  reservedCost = estimatedInputTokens + 150   ← not a guess, a guarantee
```

Worst case, actual usage equals the reservation exactly. Best case, you refund a large surplus. **The bucket can never be surprised by more consumption than it planned for** — that's the entire value of a hard cap over an estimate.

I verified `num_predict` actually holds as a real ceiling, not just a hint, against multiple prompts — including one that would naturally have generated 1300+ tokens uncapped. It stopped at exactly 150 every time.

---

## Finding #2: fixing output estimation doesn't fix input estimation — they're separate bugs

Once output was capped, I still saw the bucket going net-negative on individual jobs. Turned out the *input* side had the exact same problem I'd just fixed on the output side: word-count-based token estimation doesn't map linearly to actual tokenization.

A 10-word prompt — `"Write a REST API for a todo app using Node.js"` — tokenized to **40 actual input tokens**, a 4x ratio, driven by the model's chat template injecting structural tokens (`<|im_start|>user...`) around every request regardless of content length.

My first instinct was to find "the right multiplier." I tried 2.5x. Then 4.0x, which happened to match that one prompt exactly. That should have been a warning sign, not a victory — a multiplier tuned to fit a single sample isn't a model, it's overfitting. The real relationship isn't `words × ratio`, it's closer to `fixed_template_overhead + (words × ratio)`, and no single multiplier can represent a fixed cost as if it scaled with input length.

The actual fix: stop trying to predict precisely, and over-reserve generously instead — `Math.max(50, words × 2)` — the same philosophy as the output-side fix. Verified across five prompt lengths (5, 10, 20, 40, and 107 words): the estimate stayed above actual consumption in every case, and the surplus got refunded cleanly every time.

---

## Finding #3: raising a ceiling to fix one problem quietly tightens a different one

Generously over-reserving input tokens meant a 107-word prompt now required 364 reserved tokens — comfortably inside a 500-token bucket, but structurally impossible in the 350-token bucket I'd started Stage 2 with. Raising the bucket ceiling to fix that fixed the long-prompt case immediately.

It also, without me planning it, cut my burst tolerance from 5 concurrent requests down to 2 — because the *ratio* between per-request cost and bucket size had shifted. Sizing a bucket for the worst-case single request and sizing it for interesting concurrent-burst behavior are two different design goals, and they pull the same number in different directions. Nobody warns you about this tradeoff in a token bucket tutorial, because most tutorials use small, fixed request costs where it never comes up.

The fix that actually mattered here wasn't the bucket size — it was recognizing that **a request whose reservation exceeds the bucket's absolute maximum capacity is a different kind of failure than a request that's merely rate-limited.** The first can never succeed, no matter how long the client waits. The second just needs time. My gateway was treating both identically — as 429s with a retry delay — which meant an oversized request would loop forever waiting for a refill that could mathematically never reach the required threshold.

The fix: a request whose cost exceeds `maxTokens` gets an immediate **HTTP 400**, not a 429. "This will never work" and "this will work in N seconds" are different messages, and conflating them is a real, easy-to-miss bug in naive rate limiter designs.

---

## The benchmark: what the rate limiter actually does under load

Final configuration: 1000-token bucket, 30 TPM refill, sized to mirror the structural ratio of Groq's 6000 TPM free tier scaled down for local hardware (roughly 5 requests of average cost before throttling, same shape as Groq's real-world ceiling relative to typical request size).

30 concurrent requests fired at the gateway:

| Metric | Value |
|---|---|
| Requests fired | 30 |
| Requests queued (allowed) | 5 |
| Requests rate-limited (429) | 25 |
| Enforcement accuracy | Exact — matches bucket capacity ÷ per-request cost precisely |
| `retryAfterMs` quoted to rejected clients | ~400,000ms (~6.7 min) |
| Actual bucket recovery time | ~90 seconds |

That gap between the quoted wait and the real recovery is the most important number in this whole lab.

---

## Finding #4: the retry estimate was wrong, and it's wrong for a real, structural reason

The 25 rejected requests were told to wait ~6.7 minutes — a worst-case calculation based purely on the bucket's passive refill rate. But the 5 admitted jobs had all *over-reserved* their token budget (capped output, generous input estimate), and as each one completed, it refunded 89-131 unused tokens back into the bucket. By 90 seconds in, the bucket had recovered past 600 tokens — enough for three more requests — while the rejected clients were still sitting on a 6-minute timer that had already become stale.

**The rate limiter's retry estimate had no visibility into refunds from jobs already in flight.** It only knew about guaranteed passive refill, not the much faster recovery actually happening from settlement.

I went looking for how OpenAI or Anthropic solve this, expecting a clever fix I'd missed. They mostly don't solve it — because most production rate limiters don't have this problem in the first place. Classic rate limiting counts *requests*, where every unit costs exactly 1 and there's nothing to "settle" later. The industry-standard answer to imprecise retry estimates isn't a smarter calculation — it's client-side exponential backoff, which absorbs the imprecision rather than eliminating it.

LLM token budgeting is a harder version of the problem most rate limiter literature covers, because it pre-reserves a variable, uncertain cost and settles it later — closer to a credit card authorization hold than a simple request counter. That's not a flaw in this implementation. It's a genuine, underdiscussed gap between how rate limiting is usually taught and what LLM infrastructure actually requires.

---

## What real production systems actually do differently

Worth being precise here, since it's easy to hand-wave this:

- **OpenAI** reserves `max_tokens` as the pre-flight ceiling — exactly the fix I converged on independently for the output side.
- **Anthropic** splits the budget into separate ITPM and OTPM buckets rather than one combined TPM number, because input and output tokens have different computational costs (memory bandwidth vs. sequential compute) — a refinement I didn't build here, but is a natural next iteration.
- **Neither publishes** a solution to the pending-refund-visibility problem. It's reasonable to conclude this is accepted as a structural cost of the token bucket model, not something they've quietly solved and kept private.

---

## What's next

The honest fix for finding #4 — making `retryAfterMs` account for expected refunds from in-flight jobs — requires knowing how a job is progressing *before* it completes. That means streaming, mid-generation token check-ins, and incremental reservation top-ups instead of one-shot pre-flight guesses. That's Stage 3, not a patch bolted onto Stage 2.

Stage 3: streaming backpressure over Socket.IO, chaos injection (timeout, malformed response, dropped connection), and — if the streaming infrastructure supports it — a first attempt at closing the gap this lab found between quoted and actual recovery time.

---

*Lab #1 (concurrency & queueing) is here. Lab #2's code, including the full Lua script and the estimator's failure/fix history, is on GitHub.*