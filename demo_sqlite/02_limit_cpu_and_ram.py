import subprocess
import resource
import psutil
import time
import os

def set_limits(cpu_seconds, memory_mb):
    """
    Set CPU time and memory (address space) limits for the subprocess.
    """
    # CPU time limit (seconds)
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
    # Address space limit (bytes)
    mem_bytes = memory_mb * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (mem_bytes, mem_bytes))

def run_and_measure(cmd, cpu_seconds=10, memory_mb=100):
    """
    Run a command with resource limits and measure its CPU time and peak memory usage.
    """
    # Start subprocess with preexec_fn to set limits in child
    proc = subprocess.Popen(
        cmd,
        shell=True,
        preexec_fn=lambda: set_limits(cpu_seconds, memory_mb)
    )
    p = psutil.Process(proc.pid)
    
    peak_memory = 0
    start_time = time.time()
    
    # Poll process until completion
    try:
        while proc.poll() is None:
            mem = p.memory_info().rss
            peak_memory = max(peak_memory, mem)
            time.sleep(0.1)  # poll every 100ms
        return_code = proc.returncode
    except psutil.NoSuchProcess:
        return_code = None
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Final memory check
    try:
        mem = p.memory_info().rss
        peak_memory = max(peak_memory, mem)
    except psutil.NoSuchProcess:
        pass
    
    print(f"Command: {cmd}")
    print(f"Return code: {return_code}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Peak memory: {peak_memory / (1024 * 1024):.2f} MB")

# Example usage:
# This will run a Python script 'test_script.py' with a 5-second CPU limit and 50 MB memory limit
if __name__ == "__main__":
    run_and_measure("python3 01_hybrid_crypto.py", cpu_seconds=5, memory_mb=50)
