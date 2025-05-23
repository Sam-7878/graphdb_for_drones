#!/usr/bin/env python3
import apsw
import time
import psutil
import statistics
import threading
import csv
import os

# —————— 설정 파라미터 ——————
DB_PATH       = "db/drone_local.db"
LOG_TABLE     = "cdc_log"
N_INSERT      = 10000
N_UPDATE      = 5000
N_DELETE      = 5000
REPEAT        = 3
MONITOR_FREQ  = 1.0   # 초 단위로 리소스 모니터링

# —————— 리소스 모니터링 스레드 ——————
def monitor_resources(stop_event, out_list):
    proc = psutil.Process()
    while not stop_event.is_set():
        cpu = proc.cpu_percent(interval=None)
        mem = proc.memory_info().rss / (1024*1024)  # MB
        out_list.append((time.time(), cpu, mem))
        time.sleep(MONITOR_FREQ)

# —————— CDC 테이블·트리거 초기화 ——————
def init_db(conn):
    cur = conn.cursor()
    # items 테이블은 그대로
    cur.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, payload TEXT);")

    # CDC 로그 테이블 재생성
    cur.execute("DROP TABLE IF EXISTS cdc_log;")
    cur.execute("""
      CREATE TABLE cdc_log (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        ts    REAL,
        op    TEXT,
        rowid INTEGER
      );
    """)

    # 트리거들은 컬럼명을 명시
    cur.execute("DROP TRIGGER IF EXISTS log_insert;")
    cur.execute("""
      CREATE TRIGGER log_insert
      AFTER INSERT ON items
      BEGIN
        INSERT INTO cdc_log(ts, op, rowid)
          VALUES (strftime('%s','now')*1000.0, 'INSERT', NEW.rowid);
      END;
    """)

    cur.execute("DROP TRIGGER IF EXISTS log_update;")
    cur.execute("""
      CREATE TRIGGER log_update
      AFTER UPDATE ON items
      BEGIN
        INSERT INTO cdc_log(ts, op, rowid)
          VALUES (strftime('%s','now')*1000.0, 'UPDATE', NEW.rowid);
      END;
    """)

    cur.execute("DROP TRIGGER IF EXISTS log_delete;")
    cur.execute("""
      CREATE TRIGGER log_delete
      AFTER DELETE ON items
      BEGIN
        INSERT INTO cdc_log(ts, op, rowid)
          VALUES (strftime('%s','now')*1000.0, 'DELETE', OLD.rowid);
      END;
    """)


# —————— 벤치마크 함수 ——————
def run_benchmark():
    # Ensure result directory exists
    os.makedirs("result", exist_ok=True)
    conn = apsw.Connection(DB_PATH)
    init_db(conn)
    cursor = conn.cursor()

    results = []
    for run in range(1, REPEAT + 1):
        # 1) 리소스 모니터링 시작
        stop_evt = threading.Event()
        resource_log = []
        mon_thread = threading.Thread(target=monitor_resources, args=(stop_evt, resource_log))
        mon_thread.start()

        # 2) 작업 시작
        start_time = time.perf_counter()
        # INSERT
        for i in range(N_INSERT):
            cursor.execute("INSERT INTO items(payload) VALUES (?);", (f"data-{run}-{i}",))
        # UPDATE
        for i in range(N_UPDATE):
            cursor.execute("UPDATE items SET payload = ? WHERE id = ?;", (f"upd-{run}-{i}", i+1))
        # DELETE
        for i in range(N_DELETE):
            cursor.execute("DELETE FROM items WHERE id = ?;", (i+1,))
        elapsed = time.perf_counter() - start_time

        # 3) 모니터링 종료
        stop_evt.set()
        mon_thread.join()

        # 4) CDC 로그 분석
        log_rows = list(cursor.execute(f"SELECT ts, op FROM {LOG_TABLE} ORDER BY ts;"))
        latencies = []
        if log_rows:
            prev_ts = log_rows[0][0]
            for ts, op in log_rows[1:]:
                latencies.append(ts - prev_ts)
                prev_ts = ts

        # Compute throughput
        ops = N_INSERT + N_UPDATE + N_DELETE
        throughput = ops / elapsed

        # Latency statistics
        if latencies:
            latencies_sorted = sorted(latencies)
            count = len(latencies_sorted)
            avg_lat = statistics.mean(latencies_sorted)
            min_lat = latencies_sorted[0]
            max_lat = latencies_sorted[-1]
            stddev_lat = statistics.stdev(latencies_sorted) if count > 1 else 0.0
            p50 = statistics.quantiles(latencies_sorted, n=100)[49]
            p95 = statistics.quantiles(latencies_sorted, n=100)[94]
            p99 = statistics.quantiles(latencies_sorted, n=100)[98]
        else:
            count = avg_lat = min_lat = max_lat = stddev_lat = p50 = p95 = p99 = 0.0

        # Resource statistics
        cpus = [c for _, c, _ in resource_log]
        mems = [m for _, _, m in resource_log]
        cpu_avg = statistics.mean(cpus) if cpus else 0.0
        cpu_max = max(cpus) if cpus else 0.0
        cpu_p95 = statistics.quantiles(cpus, n=100)[94] if len(cpus) >= 100 else cpu_max
        mem_avg = statistics.mean(mems) if mems else 0.0
        mem_max = max(mems) if mems else 0.0
        mem_p95 = statistics.quantiles(mems, n=100)[94] if len(mems) >= 100 else mem_max

        results.append({
            "run": run,
            "elapsed_s": f"{elapsed:.3f}",
            "throughput_ops_per_s": f"{throughput:.1f}",
            "lat_count": count,
            "lat_avg_ms": f"{avg_lat:.2f}",
            "lat_min_ms": f"{min_lat:.2f}",
            "lat_max_ms": f"{max_lat:.2f}",
            "lat_stddev_ms": f"{stddev_lat:.2f}",
            "lat_p50_ms": f"{p50:.2f}",
            "lat_p95_ms": f"{p95:.2f}",
            "lat_p99_ms": f"{p99:.2f}",
            "cpu_avg_pct": f"{cpu_avg:.2f}",
            "cpu_max_pct": f"{cpu_max:.2f}",
            "cpu_p95_pct": f"{cpu_p95:.2f}",
            "mem_avg_mb": f"{mem_avg:.2f}",
            "mem_max_mb": f"{mem_max:.2f}",
            "mem_p95_mb": f"{mem_p95:.2f}",
        })

        # CDC 로그 초기화
        cursor.execute(f"DELETE FROM {LOG_TABLE};")

    # 5) 결과 CSV로 저장
    csv_file = "result/scenario1_detailed_results.csv"
    with open(csv_file, "w", newline="") as f:
        fieldnames = list(results[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Benchmark complete. Detailed results in '{csv_file}'")



if __name__ == "__main__":
    run_benchmark()

