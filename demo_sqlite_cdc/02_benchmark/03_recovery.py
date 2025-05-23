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
GRAPHDB_DSN       = "host=localhost port=5432 dbname=agdb user=aguser password=agpw"
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
    # 1) 로컬 버퍼 테이블 준비
    sqlite_conn = get_sqlite_conn()
    sqlite_cur  = sqlite_conn.cursor()
    graphdb_conn = get_graph_conn()

    # 1) 로컬 버퍼 테이블 준비 (기존 테이블 삭제 후 재생성)
    sqlite_cur.execute("DROP TABLE IF EXISTS cdc_log;")
    sqlite_cur.execute("""
      CREATE TABLE cdc_log (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT,
        ts      REAL DEFAULT (strftime('%s','now'))
      );
    """)
    # sqlite_conn.commit()
    # APSW auto-commit: 별도 COMMIT 불필요

    results = []  # 각 (interval, run, backlog, rec_time, throughput)

    for interval in OFFLINE_INTERVALS:
        print(f"\n=== 오프라인 간격: {interval}s 테스트 시작 ===")
        for run in range(1, REPEATS+1):
            print(f"\n-- 반복 {run}/{REPEATS} --")

            # 2) 오프라인 모드: 이벤트 생성만 디스크에 버퍼링
            t_off_start = time.perf_counter()
            print(f"1) 네트워크 차단 → {interval}s 동안 이벤트 버퍼링 중...", end="")
            while time.perf_counter() - t_off_start < interval:
                payload = {"payload": f"offline-{interval}-{run}-{int(time.time()*1000)}"}
                sqlite_cur.execute(
                    "INSERT INTO cdc_log(payload) VALUES (?);",
                    (payload["payload"],)
                )
            # sqlite_conn.commit()

            # 백로그 크기
            backlog = sqlite_cur.execute("SELECT COUNT(*) FROM cdc_log;").fetchone()[0]
            print(f"완료. 쌓인 이벤트 수 = {backlog}")

            # 3) 온라인 복구: 디스크에서 배치로 읽어 전송
            print("2) 네트워크 복구 → 디스크 버퍼에서 배치 전송 시작")
            t_rec_start = time.perf_counter()
            rows = list(sqlite_cur.execute("SELECT id, payload FROM cdc_log ORDER BY id;"))
            for i in range(0, backlog, BATCH_SIZE):
                batch = rows[i:i+BATCH_SIZE]
                for _, payload in batch:
                    producer.send(TOPIC, {"id": _, "payload": payload})
                producer.flush()
            t_sent = time.perf_counter()
            print(f"   • 전송 완료 ({backlog}건) in {(t_sent - t_rec_start):.2f}s")

            # 4) GraphDB 반영 대기
            print("3) GraphDB 반영 대기 중…")
            applied = 0
            while applied < backlog:
                recs = consumer.poll(timeout_ms=1000)
                for tp, records in recs.items():
                    applied += len(records)
            t_rec_end = time.perf_counter()

            rec_time   = t_rec_end - t_rec_start
            throughput = backlog / rec_time
            print(f"   • 복구 완료 in {rec_time:.2f}s (처리량 {throughput:.1f} ev/s)")

            # 5) 결과 기록
            results.append({
                "offline_s": interval,
                "run": run,
                "backlog": backlog,
                "recovery_time_s": f"{rec_time:.2f}",
                "throughput_ev_s": f"{throughput:.1f}"
            })

            # 6) 로컬 버퍼 초기화
            sqlite_cur.execute("DELETE FROM cdc_log;")
            # sqlite_conn.commit()

            time.sleep(1)

    # —————— CSV 저장 ——————
    csv_file = "result/scenario3_recovery.csv"
    with open(csv_file, "w", newline="") as f:
        fieldnames = ["offline_s", "run", "backlog", "recovery_time_s", "throughput_ev_s"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ 시나리오 3 결과가 '{csv_file}'에 저장되었습니다.")

    # 정리
    producer.close()
    consumer.close()
    sqlite_conn.close()
    graphdb_conn.close()



if __name__ == "__main__":
    run_scenario3()

