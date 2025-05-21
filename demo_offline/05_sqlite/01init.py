from pysqlcipher3 import dbapi2 as sqlite

conn = sqlite.connect("../wallet.db")

conn.execute("PRAGMA key = 'securepassword'")
conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);")

conn.commit()
conn.close()