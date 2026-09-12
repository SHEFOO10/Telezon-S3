import sys
import httpx

if len(sys.argv) < 2:
    print("Usage: uv run python test_connection.py <RENDER_SERVICE_URL>")
    print("Example: uv run python test_connection.py https://telezon-s3.onrender.com")
    sys.exit(1)

url = sys.argv[1].rstrip("/")

print(f"Testing connectivity to: {url}\n")

with httpx.Client(follow_redirects=True, timeout=10.0) as client:
    # Test 1: Root / Docs
    try:
        print("1. Testing GET /docs...")
        r = client.get(f"{url}/docs")
        print(f"   Status: {r.status_code}")
        print(f"   Server: {r.headers.get('server', 'N/A')}")
        print(f"   CF-Ray: {r.headers.get('cf-ray', 'N/A')}")
    except Exception as e:
        print(f"   Connection failed: {e}")

    # Test 2: Put to S3 route with browser-like user agent
    try:
        print("\n2. Testing PUT /admin/sample.txt (Checking if S3 route reaches FastAPI)...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Content-Type": "text/plain",
        }
        r = client.put(f"{url}/admin/sample.txt", content=b"hello", headers=headers)
        print(f"   Status: {r.status_code}")
        print(f"   Headers: {dict(r.headers)}")
        print(f"   Body Response:\n{r.text}")
    except Exception as e:
        print(f"   Connection failed: {e}")
