#!/usr/bin/env python3
import time
import statistics
import threading
import apsw
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import json
import csv
import os

# —————— 설정 파라미터 ——————
SQLITE_DB       = "db/drone_local.db"
KAFKA_BOOTSTRAP = ["localhost:9092"]
TOPIC           = "drone.cdc.items"
GROUP_ID        = "latency-tester"
GRAPHDB_DSN     = "host=localhost port=5432 dbname=testdb user=sam password=dooley"
N_EVENTS        = 5000
LOG_INTERVAL    = 500  # 진행 메시지 출력 주기

# —————— SQLite 커넥션 ——————
def get_sqlite_conn():
    return apsw.Connection(SQLITE_DB)

# —————— Kafka 프로듀서/컨슈머 설정 ——————
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# —————— GraphDB 커넥션 ——————
def get_graphdb_conn():
    return psycopg2.connect(GRAPHDB_DSN)

# —————— 통계 계산 헬퍼 ——————
def summarize(latencies):
    data = sorted(latencies)
    count = len(data)
    avg = statistics.mean(data)
    min_v = data[0]
    max_v = data[-1]
    stddev = statistics.stdev(data) if count > 1 else 0.0
    # percentiles
    p50 = statistics.quantiles(data, n=100)[49]
    p95 = statistics.quantiles(data, n=100)[94]
    p99 = statistics.quantiles(data, n=100)[98]
    return {
        "count": count,
        "avg_ms": avg,
        "min_ms": min_v,
        "max_ms": max_v,
        "stddev_ms": stddev,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99
    }

# —————— 메인 테스트 루틴 ——————
def run_e2e_latency_test():
    # 초기화
    sqlite_conn = get_sqlite_conn()
    sqlite_cur  = sqlite_conn.cursor()
    graph_conn  = get_graphdb_conn()
    graph_cur   = graph_conn.cursor()

    sqlite_cur.execute("DELETE FROM items;")
    sqlite_cur.execute("DELETE FROM cdc_log;")
    graph_cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    graph_cur.execute("CREATE GRAPH vc_graph;")
    graph_cur.execute("SET graph_path = vc_graph;")
    graph_conn.commit()

    print(f"▶ 시작: 총 {N_EVENTS}건 E2E 지연 측정\n")
    print("1️⃣ Kafka Consumer 초기화 대기…")
    time.sleep(2)

    lat_drone_to_broker = []
    lat_broker_to_graph = []
    lat_end_to_end      = []

    for i in range(1, N_EVENTS + 1):
        payload = {"id": i, "payload": f"event-{i}"}

        # 1) 로컬 INSERT
        t0 = time.perf_counter()
        sqlite_cur.execute(
            "INSERT OR REPLACE INTO items(id,payload) VALUES(?,?);",
            (i, payload["payload"])
        )

        # 2) Kafka 전송
        fut = producer.send(TOPIC, value=payload)
        fut.get(timeout=10)
        t1 = time.perf_counter()

        # 3) Kafka 수신 및 GraphDB 반영
        msg = None
        while True:
            recs = consumer.poll(timeout_ms=500)
            found = False
            for tp, records in recs.items():
                for record in records:
                    if record.value.get("id") == i:
                        msg = record.value
                        found = True
                        break
                if found: break
            if msg: break

        graph_cur.execute(
            "CREATE (n:Item {id:%s, payload:%s});",
            (msg["id"], msg["payload"])
        )
        graph_conn.commit()
        t2 = time.perf_counter()

        # 지연 기록
        dt1 = (t1 - t0) * 1e3
        dt2 = (t2 - t1) * 1e3
        dtE = (t2 - t0) * 1e3
        lat_drone_to_broker.append(dt1)
        lat_broker_to_graph.append(dt2)
        lat_end_to_end.append(dtE)

        # 진행 메시지
        if i % LOG_INTERVAL == 0 or i == N_EVENTS:
            print(f"[{i}/{N_EVENTS}] 현재 E2E={dtE:.1f}ms (D→K={dt1:.1f}, K→G={dt2:.1f})")

    # 통계 요약
    stats = {
        "drone_to_broker": summarize(lat_drone_to_broker),
        "broker_to_graph": summarize(lat_broker_to_graph),
        "end_to_end":      summarize(lat_end_to_end)
    }

    # CSV 파일로 저장
    out_dir = "result"
    os.makedirs(out_dir, exist_ok=True)
    csv_file = os.path.join(out_dir, "scenario2_detailed_latency.csv")
    with open(csv_file, "w", newline="") as f:
        fieldnames = [
            "phase", "count", "avg_ms", "min_ms", "max_ms",
            "stddev_ms", "p50_ms", "p95_ms", "p99_ms"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for phase, s in stats.items():
            row = {"phase": phase}
            row.update({k: f"{v:.2f}" if isinstance(v, float) else v 
                        for k, v in s.items()})
            writer.writerow(row)

    # 콘솔 요약 출력
    print("\n▶ 상세 지연 통계:")
    for phase, s in stats.items():
        print(f"  ■ {phase}: count={s['count']}, avg={s['avg_ms']:.1f}ms, "
              f"min={s['min_ms']:.1f}, max={s['max_ms']:.1f}, stddev={s['stddev_ms']:.1f}ms, "
              f"P50={s['p50_ms']:.1f}ms, P95={s['p95_ms']:.1f}ms, P99={s['p99_ms']:.1f}ms")
    print(f"\n✅ 결과가 '{csv_file}'에 저장되었습니다.")

    # 정리
    producer.close()
    consumer.close()
    graph_conn.close()



if __name__ == "__main__":
    run_e2e_latency_test()

