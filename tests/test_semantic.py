import os
import json
import urllib.request
import urllib.error

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
print(f"Key present: {bool(OPENAI_API_KEY)}")
print(f"Key starts with: {OPENAI_API_KEY[:10] if OPENAI_API_KEY else 'EMPTY'}")

payload = json.dumps({
    "model": "gpt-4o-mini",
    "temperature": 0,
    "max_tokens": 100,
    "response_format": {"type": "json_object"},
    "messages": [
        {
            "role": "user",
            "content": 'Respond with JSON: {"meaning_accuracy": 75, "completeness": 80, "anchor_alignment": 60}'
        }
    ]
}).encode('utf-8')

req = urllib.request.Request(
    "https://api.openai.com/v1/chat/completions",
    data=payload,
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
)

try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
        print(f"Keys: {list(data.keys())}")
        print(f"Content: {data['choices'][0]['message']['content']}")
except urllib.error.HTTPError as e:
    print(f"HTTP {e.code}: {e.read().decode()[:500]}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")