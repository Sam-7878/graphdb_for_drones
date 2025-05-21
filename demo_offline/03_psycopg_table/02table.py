# Note: the module name is psycopg, not psycopg3
import psycopg

# AgensGraph 연결 설정
connection_string = "host=localhost port=5433 dbname=edge user=sam"

try:
    # Connect to an existing database
    with psycopg.connect(connection_string) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test CASCADE")
            # Execute a command: this creates a new table
            cur.execute("CREATE TABLE test (id serial PRIMARY KEY,num integer,data text)")

            # Pass data to fill a query placeholders and let Psycopg perform
            # the correct conversion (no SQL injections!)
            cur.execute(
                "INSERT INTO test (num, data) VALUES (%s, %s)",
                (100, "abc'def"))

            # Query the database and obtain data as Python objects.
            cur.execute("SELECT * FROM test")

            result = cur.fetchall()
            print(result)

except Exception as e:
    print("Connection failed:", e)