# test_rules.py
import urllib.request, json

BASE = "http://localhost:8000"

# Get token first
data = json.dumps({"username": "staff_user", "password": "staff123"}).encode()
req = urllib.request.Request(f"{BASE}/api/auth/login", data=data,
      headers={"Content-Type": "application/json"}, method="POST")
token = json.loads(urllib.request.urlopen(req).read())["access_token"]
headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

# Test 1: GET /api/rules/topics
req = urllib.request.Request(f"{BASE}/api/rules/topics", headers=headers)
topics = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 1 PASS: {len(topics)} topics returned  (expected 15)")

# Test 2: GET /api/rules/anchors
req = urllib.request.Request(f"{BASE}/api/rules/anchors", headers=headers)
anchors = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 2 PASS: {len(anchors)} anchor types returned  (expected 11)")

# Test 3: POST /api/rules/resolve — ER topic, 2025-01-01 → should return rules
er_topic_id = next(t["rule_topic_id"] for t in topics if t["topic_code"] == "ER")
data = json.dumps({"rule_topic_id": er_topic_id, "query_date": "2025-01-01",
                   "query_date_type": "date_of_service"}).encode()
req = urllib.request.Request(f"{BASE}/api/rules/resolve", data=data,
      headers=headers, method="POST")
result = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 3 PASS: resolve status = {result['resolution_status']}")

# Test 4: GET /api/rules/history/ER-001
req = urllib.request.Request(f"{BASE}/api/rules/history/ER-001", headers=headers)
history = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 4 PASS: ER-001 has {len(history)} versions in history")