from arango import ArangoClient

# ArangoDB 클라이언트 초기화
client = ArangoClient()

# 시스템 데이터베이스 접속
sys_db = client.db("_system", username="root", password="dooley")

# 데이터베이스 생성 (없으면 생성)
if not sys_db.has_database("test"):
    sys_db.create_database("test")

# 데이터베이스 연결
db = client.db("test", username="root", password="dooley")

# Graph 이름 정의
graph_name = "social_graph"

# Graph가 존재하지 않으면 생성
if not db.has_graph(graph_name):
    graph = db.create_graph(graph_name)

    # Vertex 컬렉션 추가 (노드)
    graph.create_vertex_collection("users")

    # Edge 컬렉션 추가 (관계)
    graph.create_edge_definition(
        edge_collection="relationships",
        from_vertex_collections=["users"],
        to_vertex_collections=["users"]
    )

print(f"Graph '{graph_name}' 생성 완료!")


# Vertex 컬렉션 (노드) 데이터 추가
users = db.collection("users")
#users.insert({"_key": "alice", "name": "Alice"})
#users.insert({"_key": "bob", "name": "Bob"})

# Edge 컬렉션 (관계) 데이터 추가
relationships = db.collection("relationships")
relationships.insert({"_from": "users/alice", "_to": "users/bob", "type": "KNOWS"})


query = """
FOR v, e, p IN 1..1 OUTBOUND 'users/alice' GRAPH 'social_graph'
    RETURN v
"""
cursor = db.aql.execute(query)
for friend in cursor:
    print(friend)
