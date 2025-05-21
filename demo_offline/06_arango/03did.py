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

print("ArangoDB 연결 성공!")

# Vertex 컬렉션 생성
if not db.has_collection("users"):
    users = db.create_collection("users")
else:
    users = db.collection("users")


# Edge 컬렉션 생성
if not db.has_collection("relationships"):
    relationships = db.create_collection("relationships", edge=True)
else:
    relationships = db.collection("relationships")

# Graph 이름 정의
graph_name = "credentials_graph"

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



## DID Document를 Vertex 컬렉션으로 저장합니다.
did_doc = {
    "_key": "did:example:123",
    "public_key": "abcdef123456",
    "service_endpoint": "https://example.com",
    "created_at": "2025-01-01T00:00:00Z"
}
users.insert(did_doc)


## VC를 Edge 컬렉션으로 저장하여 Issuer와 Holder 간 관계를 표현합니다.
vc = {
    "_from": "users/issuer",
    "_to": "users/holder",
    "credential_data": '{"name": "Alice", "degree": "BSc"}',
    "signature": "abcdef123",
    "issued_at": "2025-01-01T00:00:00Z"
}
relationships.insert(vc)


## DID와 관련된 Verifiable Credential을 조회합니다.
query = """
FOR v, e IN 1..1 OUTBOUND 'users/did:example:123' GRAPH 'credentials_graph'
    RETURN {vc: e, holder: v}
"""
cursor = db.aql.execute(query)
for result in cursor:
    print(result)
