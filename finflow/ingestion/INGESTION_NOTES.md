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

## 1.4 - Parallel Transformation

### Benchmark Results

Number of workers: 4
Each chunk size was run 3 times and the lowest time was used
==================================================
            TRANSFORMATION BENCHMARK
==================================================
Chunk size:       500,000 rows
--------------------------------------------------
Method                    Time (s)        Speedup
--------------------------------------------------
Sequential                   4.13           1.00x
Parallel                     5.07           0.82x
==================================================
Chunk size:       1,000,000 rows
--------------------------------------------------
Method                    Time (s)        Speedup
--------------------------------------------------
Sequential                   4.03           1.00x
Parallel                     6.19           0.65x
==================================================
Chunk size:       2,000,000 rows
--------------------------------------------------
Method                    Time (s)        Speedup
--------------------------------------------------
Sequential                   4.02           1.00x
Parallel                     6.32           0.64x
==================================================

### Analysis

The **500,000 chunk size** was selected because it gave the best result.

**Small Chunks:**
reduce amount of data handled by each worker but increase the number of chunks and overhead.
**Large Chunks:**
reduce number of chunks but need more memory per worker (slower in these tests).

### Sequential vs Parallel

Parallel transformation was slower than sequential for all chunk sizes. The scale of transformation operations isn't huge, so the overhead of creating processes, transferring DataFrame chunks between processes, and combining the results was greater than the benefit of parallel CPU execution.

with 1 worker, we get zero parallel speedup but still pay the multiprocessing cost.