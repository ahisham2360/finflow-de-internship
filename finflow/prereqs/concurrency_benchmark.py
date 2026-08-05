import requests
import time
import os
import psutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor



# ==========================================
# PART A: I/O-BOUND SETUP
# ==========================================
url_1 = "https://api.stlouisfed.org/fred/category?category_id=125&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"
url_2 = "https://api.stlouisfed.org/fred/category/children?category_id=13&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"
url_3 = "https://api.stlouisfed.org/fred/category/related?category_id=32073&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"
url_4 = "https://api.stlouisfed.org/fred/category/series?category_id=125&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"
url_5 = "https://api.stlouisfed.org/fred/category/tags?category_id=125&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json"

urls = [url_1, url_2, url_3, url_4, url_5]

def fetch_sequential(urls):
    results = []
    for url in urls:
        response = requests.get(url)
        results.append(len(response.content))
    return results

def fetch_parallel(urls):
    results = []
    with ThreadPoolExecutor() as executor:
        futures = []
        for url in urls:
            future = executor.submit(requests.get, url)
            futures.append(future)
        for future in futures:
            response = future.result()
            results.append(len(response.content))
    return results



# ==========================================
# PART B: CPU-BOUND SETUP
# ==========================================
def compute_heavy(n: int) -> int:
    worker_start = time.perf_counter()
    def is_prime(num):
        if num <2:
            return False
        for i in range(2, int(num**0.5) +1):
            if num % i == 0:
                return False
        return True
    
    results = sum(i for i in range(n) if is_prime(i))
    worker_end = time.perf_counter()    
    useful_time = worker_end - worker_start
    return results, useful_time

def run_sequential(inputs: list[int]):
    results = []
    total_useful_time = 0
    for n in inputs:
        result = compute_heavy(n)
        results.append(result)
        total_useful_time += result[1]
    return results, total_useful_time

def run_parallel(inputs: list[int]):
    results = []
    total_useful_time = 0
    pool_start = time.perf_counter()
    with ProcessPoolExecutor() as executor:
        pool_startup_time = time.perf_counter() - pool_start
        futures = []
        for n in inputs:
            future = executor.submit(compute_heavy, n)
            futures.append(future)
        for future in futures:
            result = future.result()
            results.append(result)
            total_useful_time += result[1]
    return results, pool_startup_time, total_useful_time



# ==========================================
# EXECUTION & OUTPUT
# ==========================================
if __name__ == "__main__":

    logical_cores = os.cpu_count()
    process = psutil.Process(os.getpid())
    psutil.cpu_percent(interval=None)
    
    print(f"--- HARDWARE BASELINE ---")
    print(f"Total CPU Cores Available: {logical_cores}")
    print(f"Baseline Memory Usage: {process.memory_info().rss / (1024 * 1024):.2f} MB\n")

    # --- PART A: I/O EXECUTION ---
    start_time = time.perf_counter()
    results_io_seq = fetch_sequential(urls)
    end_time = time.perf_counter()
    io_sequential_time = end_time - start_time

    start_time = time.perf_counter()
    results_io_par = fetch_parallel(urls)
    io_parallel_time = time.perf_counter() - start_time

    if io_parallel_time > 0:
        io_speedup = io_sequential_time / io_parallel_time
    else:
        io_speedup = float('inf')

    print(f"I/O-bound benchmark (ThreadPoolExecutor):")
    print(f"Method      |   Time (s)  |  Speedup")
    print(f"------------|-------------|----------")
    print(f"Sequential  |    {io_sequential_time:.2f}     |   1.0x")
    print(f"Parallel    |    {io_parallel_time:.2f}     |   {io_speedup:.1f}x\n")



    # --- PART B: CPU EXECUTION ---
    inputs = [500_000, 600_000, 700_000, 800_000]

    start_time = time.perf_counter()
    results_cpu_seq, seq_useful_time = run_sequential(inputs)
    cpu_sequential_time = time.perf_counter() - start_time
    seq_cpu = psutil.cpu_percent(interval=None)

    start_time = time.perf_counter()
    results_cpu_par, pool_startup, par_useful_time = run_parallel(inputs)
    cpu_parallel_time = time.perf_counter() - start_time
    par_cpu = psutil.cpu_percent(interval=None)

    if cpu_parallel_time > 0:
        cpu_speedup = cpu_sequential_time / cpu_parallel_time
    else:
        cpu_speedup = float('inf')

    par_overhead = cpu_parallel_time - (par_useful_time / len(inputs))

    print(f"CPU-bound benchmark (ProcessPoolExecutor):")
    print(f"Method      |   Time (s)  |  Speedup")
    print(f"------------|-------------|----------")
    print(f"Sequential  |    {cpu_sequential_time:.2f}     |   1.0x")
    print(f"Parallel    |    {cpu_parallel_time:.2f}     |   {cpu_speedup:.1f}x\n")



    print(f"--- DEEP DIVE METRICS ---")
    print(f"Metric                  | Sequential   | Parallel")
    print(f"------------------------|--------------|--------------")
    print(f"Total Wall Time         | {cpu_sequential_time:.2f} s       | {cpu_parallel_time:.2f} s")
    print(f"CPU Utilization         |{seq_cpu:>5.1f} %       | {par_cpu:>5.1f} %")
    print(f"Sum of Useful Work      | {seq_useful_time:.2f} s       | {par_useful_time:.2f} s")
    print(f"Pool Startup Time       | N/A          | {pool_startup:.4f} s")
    print(f"Estimated OS Overhead   | N/A          | {par_overhead:.2f} s")



# A ThreadPoolExecutor is suitable for part A because the network requests bypass the GIL, so multiple threads can run simultaneously.
# But it will not work for part B because because the GIL restricts Python to executing only one thread at a time, which would take longer because of the heavy math used.
# ProcessPoolExecutor fixes this by creating separate processes that have different GILs and different memory spaces, so they're able to run in parallel.