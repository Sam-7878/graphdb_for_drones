import psycopg

# AgensGraph 연결 설정
connection_string = "host=localhost port=5433 dbname=edge user=sam"

# 연결 테스트
try:
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            # 간단한 쿼리 실행
            cur.execute("SELECT version();")
            result = cur.fetchone()
            print("AgensGraph Version:", result)
except Exception as e:
    print("Connection failed:", e)
