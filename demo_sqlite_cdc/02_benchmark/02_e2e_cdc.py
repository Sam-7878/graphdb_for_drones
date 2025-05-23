#!/usr/bin/env python3
import time
import statistics
import threading
import apsw
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import json
import csv

# —————— 설정 파라미터 ——————
SQLITE_DB      = "db/drone_local.db"
KAFKA_BOOTSTRAP = ["localhost:9092"]
TOPIC           = "drone.cdc.items"
GROUP_ID        = "latency-tester"
GRAPHDB_DSN     = "host=localhost port=5432 dbname=testdb user=sam password=dooley"
N_EVENTS        = 5000
N_PRINT_COUNT   = 100

# —————— SQLite 초기화(트리거는 이미 생성되어 있다고 가정) ——————
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

# —————— GraphDB(AgensGraph) 커넥션 ——————
def get_graphdb_conn():
    return psycopg2.connect(GRAPHDB_DSN)

# —————— 단일 이벤트 E2E 측정 루프 ——————
def run_e2e_latency_test():
    sqlite_conn = get_sqlite_conn()
    sqlite_cur  = sqlite_conn.cursor()
    graph_conn  = get_graphdb_conn()
    graph_cur   = graph_conn.cursor()

    lat_drone_to_broker = []
    lat_broker_to_graph = []
    lat_e2e             = []

    # 테스트 루프 시작 전에
    # 1) SQLite DB 초기화
    sqlite_cur.execute("DELETE FROM items;")
    sqlite_cur.execute("DELETE FROM cdc_log;")   # CDC 로그도 함께 초기화

    # 2) AgensGraph 초기화
    graph_cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    graph_cur.execute("CREATE GRAPH vc_graph;")
    graph_cur.execute("SET graph_path = vc_graph;")
    graph_cur.connection.commit()

    print(f"▶ 시작: 총 {N_EVENTS}건 이벤트 처리 지연 측정\n")

    # 미리 컨슈머 파티션 할당 대기
    print("1️⃣  Kafka Consumer 초기화 중…")
    time.sleep(2)

    for i in range(1, N_EVENTS + 1):
        # 1) 로컬 INSERT
        payload = {"id": i, "payload": f"event-{i}"}
        if i % N_PRINT_COUNT == 0:
            print(f"[{i}/{N_EVENTS}]  단계① INSERT → SQLite", end="… ")
        t0 = time.perf_counter()
        sqlite_cur.execute("INSERT OR REPLACE INTO items(id, payload) VALUES (?, ?);", (i, payload["payload"]))

        # APSW 트리거가 CDC 로그 생성
        t1 = None

        # 2) Kafka 전송
        if i % N_PRINT_COUNT == 0:
            print("단계② 전송 → Kafka", end="… ")
        fut = producer.send(TOPIC, value=payload)
        fut.get(timeout=10)  # 전송 완료 대기
        t1 = time.perf_counter()

        # 3) GraphDB 반영
        if i % N_PRINT_COUNT == 0:
            print("단계③ 수신 → GraphDB 반영", end="… ")
        # Kafka에서 방금 보낸 메시지 소비
        msg = None
        while True:
            rec = consumer.poll(timeout_ms=1000)
            if rec:
                for tp, records in rec.items():
                    for record in records:
                        if record.value.get("id") == i:
                            msg = record.value
                            break
            if msg is not None:
                break

        # 예시: GraphDB에 단순 노드 생성 쿼리 (실환경에는 Debezium Sink Connector 로 자동 반영)
        graph_cur.execute(
            "CREATE (n:Item {id:%s, payload:%s});",
            (msg["id"], msg["payload"])
        )
        graph_conn.commit()
        t2 = time.perf_counter()

        # 4) 지연 계산
        dt1 = (t1 - t0) * 1e3         # 드론→브로커 (ms)
        dt2 = (t2 - t1) * 1e3         # 브로커→GraphDB (ms)
        dtE = (t2 - t0) * 1e3         # 전체 E2E (ms)
        lat_drone_to_broker.append(dt1)
        lat_broker_to_graph.append(dt2)
        lat_e2e.append(dtE)

        if i % N_PRINT_COUNT == 0:
            print(f"완료. E2E={dtE:.1f}ms (D→K={dt1:.1f}ms, K→G={dt2:.1f}ms)")

    # —————— 통계 산출 ——————
    def calc_stats(data):
        return {
            "avg": statistics.mean(data),
            "p50": statistics.quantiles(data, n=100)[49],
            "p95": statistics.quantiles(data, n=100)[94],
            "p99": statistics.quantiles(data, n=100)[98],
        }

    stats = {
        "drone_to_broker": calc_stats(lat_drone_to_broker),
        "broker_to_graph": calc_stats(lat_broker_to_graph),
        "end_to_end":      calc_stats(lat_e2e),
    }


    # —————— 콘솔 출력 ——————
    print("\n▶ 결과:")
    for phase, s in stats.items():
        print(f"  ■ {phase}: AVG={s['avg']:.1f}ms, P50={s['p50']:.1f}ms, "
              f"P95={s['p95']:.1f}ms, P99={s['p99']:.1f}ms")

    # —————— CSV 파일 저장 ——————
    csv_file = "result/scenario2_latency_stats.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["phase", "avg_ms", "p50_ms", "p95_ms", "p99_ms"])
        for phase, s in stats.items():
            writer.writerow([phase, f"{s['avg']:.1f}", f"{s['p50']:.1f}",
                             f"{s['p95']:.1f}", f"{s['p99']:.1f}"])
    print(f"\n✅ 지연 통계가 '{csv_file}'에 저장되었습니다.")


    # 종료
    producer.close()
    consumer.close()
    graph_conn.close()

if __name__ == "__main__":
    run_e2e_latency_test()
