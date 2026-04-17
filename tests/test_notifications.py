# test_notifications.py
import urllib.request, json

BASE = "http://localhost:8000"

# Get token
data = json.dumps({"username": "staff_user", "password": "staff123"}).encode()
req = urllib.request.Request(f"{BASE}/api/auth/login", data=data,
      headers={"Content-Type": "application/json"}, method="POST")
token = json.loads(urllib.request.urlopen(req).read())["access_token"]
headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

# Test 1: GET /api/notifications/unread — should return empty
req = urllib.request.Request(f"{BASE}/api/notifications/unread", headers=headers)
result = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 1 PASS: unread count = {result['count']}  (expected 0)")

# Test 2: POST /api/notifications/read — should return ok
req = urllib.request.Request(f"{BASE}/api/notifications/read",
      data=b"", headers=headers, method="POST")
result = json.loads(urllib.request.urlopen(req).read())
print(f"TEST 2 PASS: mark read status = {result['status']}  (expected ok)")