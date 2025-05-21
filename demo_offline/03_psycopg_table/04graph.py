# Note: the module name is psycopg, not psycopg3
import psycopg

# AgensGraph 연결 설정
connection_string = "host=localhost port=5433 dbname=edge user=sam"

try:
    # Connect to an existing database
    with psycopg.connect(connection_string) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            cur.execute("DROP GRAPH IF EXISTS my_graph2 CASCADE")

            cur.execute("CREATE GRAPH my_graph2;")
            cur.execute("SET graph_path = my_graph2;")

            cur.execute("CREATE (:v {name: 'AgensGraph'});")
            conn.commit();

            cur.execute("MATCH (n) RETURN n;")

            ## agensgraph-python sample에서 제시한 fechone()은 안 됨.
            #v = cur.fetchone()[0]
            #print(v.props['name'])
            
            result = cur.fetchall()
            print(result)            

except Exception as e:
    print("Connection failed:", e)