#!/usr/bin/env python3
import time
import statistics
import apsw
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import json
import csv

# —————— 설정 파라미터 ——————
SQLITE_DB         = "db/drone_local.db"
TOPIC             = "drone.cdc.items"
KAFKA_BOOTSTRAP   = ["localhost:9092"]
GRAPHDB_DSN       = "host=localhost port=5432 dbname=testdb user=sam password=dooley"
OFFLINE_INTERVALS = [60, 300]   # 초 단위: 1분, 5분
REPEATS           = 3
BATCH_SIZE        = 500        # 배치 전송 크기

# —————— 프로듀서 / 컨슈머 / DB 커넥션 생성 ——————
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="recovery-tester",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def get_sqlite_conn():
    return apsw.Connection(SQLITE_DB)

def get_graph_conn():
    return psycopg2.connect(GRAPHDB_DSN)

# —————— 메인 테스트 루틴 ——————
def run_scenario3():
    sqlite_conn = get_sqlite_conn()
    sqlite_cur  = sqlite_conn.cursor()
    graphdb_conn = get_graph_conn()

    # 로컬 버퍼 테이블 초기화
    sqlite_cur.execute("DROP TABLE IF EXISTS cdc_log;")
    sqlite_cur.execute("""
      CREATE TABLE cdc_log (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT,
        ts      REAL DEFAULT (strftime('%s','now'))
      );
    """)

    results = []

    for interval in OFFLINE_INTERVALS:
        print(f"\n=== 오프라인 간격: {interval}s 테스트 시작 ===")
        for run in range(1, REPEATS+1):
            print(f"\n-- 반복 {run}/{REPEATS} --")

            # 1) 오프라인 버퍼링
            t_off_start = time.perf_counter()
            print(f"1) 네트워크 차단 → {interval}s 동안 이벤트 버퍼링 중...", end="")
            while time.perf_counter() - t_off_start < interval:
                payload_text = f"offline-{interval}-{run}-{int(time.time()*1000)}"
                sqlite_cur.execute(
                    "INSERT INTO cdc_log(payload) VALUES (?);",
                    (payload_text,)
                )
            t_off_end = time.perf_counter()
            buffering_time = t_off_end - t_off_start
            backlog = sqlite_cur.execute("SELECT COUNT(*) FROM cdc_log;").fetchone()[0]
            buffer_rate = backlog / buffering_time
            print(f"완료. 이벤트={backlog}, 소요={buffering_time:.1f}s, 속도={buffer_rate:.1f} ev/s")

            # 2) 온라인 복구 - 전송
            print("2) 네트워크 복구 → 배치 전송 시작")
            t_send_start = time.perf_counter()
            rows = list(sqlite_cur.execute("SELECT id, payload FROM cdc_log ORDER BY id;"))
            for i in range(0, backlog, BATCH_SIZE):
                batch = rows[i:i+BATCH_SIZE]
                for rec_id, payload in batch:
                    producer.send(TOPIC, {"id": rec_id, "payload": payload})
                producer.flush()
            t_send_end = time.perf_counter()
            send_time = t_send_end - t_send_start
            send_rate = backlog / send_time
            print(f"   • 전송 완료 in {send_time:.2f}s, 전송율={send_rate:.1f} ev/s")

            # 3) GraphDB 반영
            print("3) GraphDB 반영 대기 중…")
            applied = 0
            t_apply_start = time.perf_counter()
            while applied < backlog:
                records = consumer.poll(timeout_ms=1000)
                for tp, recs in records.items():
                    applied += len(recs)
            t_apply_end = time.perf_counter()
            apply_time = t_apply_end - t_apply_start
            total_recovery_time = t_apply_end - t_send_start
            apply_rate = backlog / apply_time
            print(f"   • 반영 완료 in {apply_time:.2f}s, 처리율={apply_rate:.1f} ev/s")

            # 4) 결과 기록
            results.append({
                "offline_s": interval,
                "run": run,
                "buffering_time_s": f"{buffering_time:.2f}",
                "buffer_rate_ev_s": f"{buffer_rate:.1f}",
                "backlog": backlog,
                "send_time_s": f"{send_time:.2f}",
                "send_rate_ev_s": f"{send_rate:.1f}",
                "apply_time_s": f"{apply_time:.2f}",
                "apply_rate_ev_s": f"{apply_rate:.1f}",
                "total_recovery_time_s": f"{total_recovery_time:.2f}"
            })

            # 5) 로컬 버퍼 초기화
            sqlite_cur.execute("DELETE FROM cdc_log;")
            time.sleep(1)

    # CSV 저장
    csv_file = "result/scenario3_detailed_recovery.csv"
    with open(csv_file, "w", newline="") as f:
        fieldnames = [
            "offline_s", "run",
            "buffering_time_s", "buffer_rate_ev_s",
            "backlog",
            "send_time_s", "send_rate_ev_s",
            "apply_time_s", "apply_rate_ev_s",
            "total_recovery_time_s"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ 시나리오 3 상세 결과가 '{csv_file}'에 저장되었습니다.")

    producer.close()
    consumer.close()
    sqlite_conn.close()
    graphdb_conn.close()

if __name__ == "__main__":
    run_scenario3()

