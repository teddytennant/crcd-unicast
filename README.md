# crcd_unicast

In-process implementation of arXiv 2610.07368's Isolated-Buffer Optimistic Model for Byzantine-tolerant causal unicast, constant application-message space, Congestion-Relaxed Causal Delivery.

Build and test with `cargo test`.

This is an in-process FIFO simulator, not a deployed network. There is no throughput table. The optional timeout-driven synchronous eviction variant is not implemented (only the event-driven cascading eviction in Algorithm 2). Caps are configurable rather than fixed at 1024.
