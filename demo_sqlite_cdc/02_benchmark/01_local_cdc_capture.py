#!/usr/bin/env python3
import apsw
import time
import psutil
import statistics
import threading
import csv

# —————— 설정 파라미터 ——————
DB_PATH       = "db/drone_local.db"
LOG_TABLE     = "db/cdc_log"
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
    # 데이터 테이블
    cur.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, payload TEXT);")
    # CDC 로그 테이블
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            ts    REAL,
            op    TEXT,
            rowid INTEGER
        );
    """)
    # INSERT 트리거
    cur.execute(f"""
        CREATE TRIGGER IF NOT EXISTS log_insert
        AFTER INSERT ON items
        BEGIN
          INSERT INTO {LOG_TABLE} VALUES (strftime('%s','now')*1000.0, 'INSERT', NEW.rowid);
        END;
    """)
    # UPDATE 트리거
    cur.execute(f"""
        CREATE TRIGGER IF NOT EXISTS log_update
        AFTER UPDATE ON items
        BEGIN
          INSERT INTO {LOG_TABLE} VALUES (strftime('%s','now')*1000.0, 'UPDATE', NEW.rowid);
        END;
    """)
    # DELETE 트리거
    cur.execute(f"""
        CREATE TRIGGER IF NOT EXISTS log_delete
        AFTER DELETE ON items
        BEGIN
          INSERT INTO {LOG_TABLE} VALUES (strftime('%s','now')*1000.0, 'DELETE', OLD.rowid);
        END;
    """)

# —————— 벤치마크 함수 ——————
def run_benchmark():
    conn = apsw.Connection(DB_PATH)
    init_db(conn)
    cursor = conn.cursor()

    results = []
    for run in range(REPEAT):
        # 1) 리소스 모니터링 시작
        stop_evt = threading.Event()
        resource_log = []
        mon_thread = threading.Thread(target=monitor_resources, args=(stop_evt, resource_log))
        mon_thread.start()

        # 2) 작업 시작
        start = time.perf_counter()
        # INSERT
        for i in range(N_INSERT):
            cursor.execute("INSERT INTO items(payload) VALUES (?);", (f"data-{run}-{i}",))
        # UPDATE
        for i in range(N_UPDATE):
            cursor.execute("UPDATE items SET payload = ? WHERE id = ?;", (f"upd-{run}-{i}", i+1))
        # DELETE
        for i in range(N_DELETE):
            cursor.execute("DELETE FROM items WHERE id = ?;", (i+1,))
        elapsed = time.perf_counter() - start

        # 3) 모니터링 종료
        stop_evt.set()
        mon_thread.join()

        # 4) CDC 로그 분석
        log_rows = list(cursor.execute(f"SELECT ts, op FROM {LOG_TABLE};"))
        latencies = []
        for idx, (ts, op) in enumerate(log_rows):
            # ts는 밀리초 단위 타임스탬프
            # 대략 동작별 지연을 추정하려면 이벤트 순서 차이로 계산 가능
            if idx > 0:
                latencies.append(ts - prev_ts)
            prev_ts = ts

        ops = N_INSERT + N_UPDATE + N_DELETE
        throughput = ops / elapsed
        avg_lat = statistics.mean(latencies) if latencies else 0
        p50 = statistics.median(latencies) if latencies else 0

        results.append({
            "run": run+1,
            "throughput_ops_per_sec": throughput,
            "avg_latency_ms": avg_lat,
            "p50_latency_ms": p50,
            "cpu_avg_pct": statistics.mean([c for _,c,_ in resource_log]),
            "mem_avg_mb": statistics.mean([m for _,_,m in resource_log])
        })

        # CDC 로그 초기화
        cursor.execute(f"DELETE FROM {LOG_TABLE};")

    # 5) 결과 CSV로 저장
    with open("result/scenario1_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("Benchmark complete. See scenario1_results.csv")

if __name__ == "__main__":
    run_benchmark()
