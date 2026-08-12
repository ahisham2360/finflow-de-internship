# Ingestion Notes

## Benchmark Results

The sequential ingestion took **168.46 seconds**, while the parallel ingestion took **111.37 seconds**.
The measured speedup was:
**168.46 / 111.37 = 1.51×**
So parallel ingestion was approximately **34% faster** than sequential ingestion.


## Why Parallel Ingestion Was Faster

In sequential ingestion, PaySim, FRED, and CFPB complaints wait for their turn to run, so total runtime is approximately the sum of the three ingestion times.
In parallel ingestion, The three ingestion tasks are independent of each other, so they can run simultaneously.
`ThreadPoolExecutor` starts the three ingestion functions concurrently using 3 workers. This allows their I/O operations, such as reading and writing files and requesting data from FRED, to overlap instead of waiting for one task to finish before starting the next.


## Race Conditions and Shared State

No race conditions were encountered during the parallel ingestion.
The three ingestion functions operate independently and there is no shared output file between the three ingestion tasks.
Each future is also handled independently using `as_completed()`. If one ingestion fails, its exception is caught and logged without preventing the other ingestion tasks from completing.


### What would happen if you used ProcessPoolExecutor here instead?

It would run each ingestion in a separate process, which would add more overhead.
This has little benefit here because ingestion is I/O-bound (network + disk), so the processes would mostly be waiting for I/O rather than using the CPU.

### In what scenario would you switch to it for ingestion?

I would switch to ProcessPoolExecutor if the ingestion became CPU-bound, such as heavy data transformations or calculations, because separate processes can execute CPU-intensive work in parallel.