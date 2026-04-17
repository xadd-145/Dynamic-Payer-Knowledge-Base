import urllib.request, json

# Test 1: valid login
data = json.dumps({'username':'staff_user','password':'staff123'}).encode()
req = urllib.request.Request('http://localhost:8000/api/auth/login', data=data, headers={'Content-Type':'application/json'}, method='POST')
resp = urllib.request.urlopen(req)
print('TEST 1 PASS:', json.loads(resp.read()))

# Test 2: wrong credentials
data = json.dumps({'username':'wrong','password':'wrong'}).encode()
req = urllib.request.Request('http://localhost:8000/api/auth/login', data=data, headers={'Content-Type':'application/json'}, method='POST')
try:
    urllib.request.urlopen(req)
    print('TEST 2 FAIL: should have returned 401')
except urllib.error.HTTPError as e:
    print('TEST 2 PASS: 401 returned as expected, code:', e.code)