# common/timer.py

import time

class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.perf_counter()

    def stop(self):
        self.end_time = time.perf_counter()

    def elapsed(self) -> float:
        if self.start_time is not None and self.end_time is not None:
            return self.end_time - self.start_time
        return 0.0

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        print(f"[TIMER] Elapsed: {self.elapsed():.4f} sec")
