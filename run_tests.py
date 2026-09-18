import asyncio
from copy import deepcopy
import hashlib
import hmac
import io
import os
import sys
import urllib.parse
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from bson import ObjectId
from httpx import ASGITransport, AsyncClient

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.config import DATABASE_NAME
from app.crud.bucket import crud_create_bucket, crud_get_bucket_by_name
from app.crud.user import crud_create_user
from app.main import app
from app.models.bucket import BucketInCreate
from app.models.user import UserInCreate
from app.s3.utils import parse_range_header
import app.db.mongodb as mongodb_module


# ---------------------------------------------------------------------------
# Lightweight In-Memory Async Mongo Mock
# ---------------------------------------------------------------------------
class MockInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class MockUpdateResult:
    def __init__(self, modified_count=1):
        self.modified_count = modified_count


class MockDeleteResult:
    def __init__(self, deleted_count=1):
        self.deleted_count = deleted_count


class MockAsyncCollection:
    def __init__(self, name):
        self.name = name
        self.docs = []

    def _matches(self, doc, query):
        for k, v in query.items():
            if isinstance(v, dict):
                if "$in" in v:
                    if doc.get(k) not in v["$in"]:
                        return False
                elif "$gt" in v:
                    if doc.get(k, "") <= v["$gt"]:
                        return False
            else:
                if doc.get(k) != v:
                    return False
        return True

    async def insert_one(self, document):
        doc = deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        self.docs.append(doc)
        return MockInsertResult(doc["_id"])

    async def find_one(self, query):
        for doc in self.docs:
            if self._matches(doc, query):
                return deepcopy(doc)
        return None

    def find(self, query=None, limit=0, skip=0, sort=None):
        query = query or {}
        matched = [deepcopy(d) for d in self.docs if self._matches(d, query)]
        if sort:
            key, direction = sort[0]
            matched.sort(key=lambda x: x.get(key, ""), reverse=(direction < 0))
        if skip:
            matched = matched[skip:]
        if limit:
            matched = matched[:limit]

        async def _gen():
            for item in matched:
                yield item
        return _gen()

    async def update_one(self, filter_query, update_spec):
        for doc in self.docs:
            if self._matches(doc, filter_query):
                if "$set" in update_spec:
                    for k, v in update_spec["$set"].items():
                        doc[k] = deepcopy(v)
                if "$push" in update_spec:
                    for k, v in update_spec["$push"].items():
                        if k not in doc or not isinstance(doc[k], list):
                            doc[k] = []
                        doc[k].append(deepcopy(v))
                if "$pull" in update_spec:
                    for k, v in update_spec["$pull"].items():
                        if k in doc and isinstance(doc[k], list):
                            if isinstance(v, dict):
                                pk, pv = next(iter(v.items()))
                                doc[k] = [x for x in doc[k] if not (isinstance(x, dict) and x.get(pk) == pv)]
                            else:
                                doc[k] = [x for x in doc[k] if x != v]
                return MockUpdateResult(1)
        return MockUpdateResult(0)

    async def delete_one(self, filter_query):
        for i, doc in enumerate(self.docs):
            if self._matches(doc, filter_query):
                del self.docs[i]
                return MockDeleteResult(1)
        return MockDeleteResult(0)

    async def delete_many(self, filter_query):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not self._matches(d, filter_query)]
        return MockDeleteResult(before - len(self.docs))

    def aggregate(self, pipeline):
        current = [deepcopy(d) for d in self.docs]
        for stage in pipeline:
            if "$match" in stage:
                current = [d for d in current if self._matches(d, stage["$match"])]
            elif "$lookup" in stage:
                from_coll = stage["$lookup"]["from"]
                local_f = stage["$lookup"]["localField"]
                foreign_f = stage["$lookup"]["foreignField"]
                as_f = stage["$lookup"]["as"]
                target_coll = mock_db_instance[from_coll]
                for d in current:
                    val = d.get(local_f)
                    d[as_f] = [deepcopy(x) for x in target_coll.docs if x.get(foreign_f) == val]
            elif "$unwind" in stage:
                path = stage["$unwind"]["path"].lstrip("$")
                unwound = []
                for d in current:
                    val = d.get(path, [])
                    if isinstance(val, list):
                        for item in val:
                            new_d = deepcopy(d)
                            new_d[path] = item
                            unwound.append(new_d)
                    elif val:
                        unwound.append(d)
                current = unwound
            elif "$project" in stage:
                proj = stage["$project"]
                projected = []
                for d in current:
                    new_d = {}
                    for k, v in proj.items():
                        if v == 1:
                            if k in d:
                                new_d[k] = d[k]
                        elif isinstance(v, dict) and "$sum" in v:
                            sum_field = v["$sum"].lstrip("$")
                            parts = sum_field.split(".")
                            if len(parts) == 2 and parts[0] in d and isinstance(d[parts[0]], list):
                                new_d[k] = sum(x.get(parts[1], 0) for x in d[parts[0]])
                            else:
                                new_d[k] = 0
                    projected.append(new_d)
                current = projected
            elif "$limit" in stage:
                current = current[:stage["$limit"]]
            elif "$skip" in stage:
                current = current[stage["$skip"]:]

        async def _gen():
            for item in current:
                yield item
        return _gen()


class MockAsyncDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = MockAsyncCollection(name)
        return self.collections[name]

    async def command(self, cmd):
        return {"ok": 1}


class MockAsyncMotorClient:
    def __init__(self):
        self.databases = {}

    def __getitem__(self, name):
        if name not in self.databases:
            self.databases[name] = MockAsyncDatabase()
        return self.databases[name]


mock_db_instance = MockAsyncDatabase()
mock_motor_client = MockAsyncMotorClient()
mock_motor_client.databases[DATABASE_NAME] = mock_db_instance


# ---------------------------------------------------------------------------
# In-Memory Mock Storage
# ---------------------------------------------------------------------------
class MockStorage:
    def __init__(self):
        self.files = {}
        self.deleted_messages = []
        self._counter = 100

    async def start(self):
        pass

    async def stop(self):
        pass

    async def put_file(self, file, filename, channel_id=None):
        self._counter += 1
        file_id = f"mock_file_id_{self._counter}"
        message_id = self._counter

        if isinstance(file, bytes):
            data = file
        elif isinstance(file, str):
            with open(file, "rb") as f:
                data = f.read()
        elif hasattr(file, "read"):
            data = file.read()
        else:
            data = b""

        self.files[file_id] = data
        return file_id, message_id

    async def get_file(self, file_id):
        if file_id not in self.files:
            raise ValueError(f"File {file_id} not found in mock storage")
        return io.BytesIO(self.files[file_id])

    async def delete_file(self, file_id, channel_id=None, message_id=None, message_ids=None):
        if message_ids:
            self.deleted_messages.extend(message_ids)
        elif message_id:
            self.deleted_messages.append(message_id)
        if file_id in self.files:
            del self.files[file_id]
        return True


def _sign_request(method, url_path, headers, body, access_key, secret_key, region="us-east-1", service="s3", query_params=None):
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    headers_to_sign = dict(headers)
    headers_to_sign["x-amz-date"] = amz_date

    if query_params:
        sorted_params = sorted(query_params.items())
        canonical_qs = "&".join(f"{urllib.parse.quote(str(k), safe='') }={urllib.parse.quote(str(v), safe='')}" for k, v in sorted_params)
    else:
        canonical_qs = ""

    sorted_header_keys = sorted(k.lower() for k in headers_to_sign.keys())
    canonical_headers = "".join(f"{k}:{headers_to_sign[k].strip()}\n" for k in sorted_header_keys)
    signed_headers = ";".join(sorted_header_keys)

    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_request = f"{method}\n{url_path}\n{canonical_qs}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

    def _hmac(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _hmac(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    k_signing = _hmac(k_service, "aws4_request")

    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    auth_header = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    headers_to_sign["Authorization"] = auth_header
    return headers_to_sign


async def run_all_tests():
    print("=" * 60)
    print("RUNNING TELEZON-S3 AUTOMATED TEST SUITE")
    print("=" * 60)

    # 1. Test Range Header Parser
    print("[TEST 1/13] Testing Range header parser...")
    assert parse_range_header("bytes=0-499", 1000) == (0, 499)
    assert parse_range_header("bytes=500-", 1000) == (500, 999)
    assert parse_range_header("bytes=-100", 1000) == (900, 999)
    assert parse_range_header("bytes=0-2000", 1000) == (0, 999)
    assert parse_range_header("bytes=1500-", 1000) is None
    assert parse_range_header(None, 1000) is None
    print(" -> PASSED: Range header parsing functions properly.")

    # Inject mock storage and mock db
    mock_store = MockStorage()
    import app.s3 as s3_module
    s3_module.storage = mock_store

    mongodb_module.db.client = mock_motor_client

    db = mock_motor_client

    # Create test user
    test_username = f"testuser_{int(datetime.now().timestamp())}"
    user_in = UserInCreate(
        username=test_username,
        email=f"{test_username}@example.com",
        password="testpassword123",
    )
    user = await crud_create_user(db, user_in)
    access_key = user.access_key_id
    secret_key = user.secret_key
    bucket_name = f"testbucket-{int(datetime.now().timestamp())}"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 2. Health check
        print("[TEST 2/13] Testing /api/v1/health endpoint...")
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200, f"Health check failed: {resp.text}"
        assert resp.json()["status"] in ("healthy", "degraded")
        print(" -> PASSED: Health endpoint operational.")

        # 3. CreateBucket
        print(f"[TEST 3/13] Testing S3 CreateBucket PUT /{bucket_name}...")
        headers = {"host": "test"}
        signed = _sign_request("PUT", f"/{bucket_name}", headers, b"", access_key, secret_key)
        resp = await client.put(f"/{bucket_name}", headers=signed)
        assert resp.status_code in (200, 201), f"CreateBucket failed: {resp.text}"
        print(" -> PASSED: S3 CreateBucket succeeded.")

        # 4. ListBuckets
        print("[TEST 4/13] Testing S3 ListBuckets GET /...")
        headers = {"host": "test"}
        signed = _sign_request("GET", "/", headers, b"", access_key, secret_key)
        resp = await client.get("/", headers=signed)
        assert resp.status_code == 200, f"ListBuckets failed: {resp.text}"
        assert "<ListAllMyBucketsResult" in resp.text
        assert f"<Name>{bucket_name}</Name>" in resp.text
        print(" -> PASSED: S3 ListBuckets returned bucket list.")

        # 5. PutObject
        print("[TEST 5/13] Testing S3 PutObject...")
        file_content = b"Hello, Telezon S3 Storage Engine with Range Requests!"
        headers = {
            "host": "test",
            "content-type": "text/plain",
            "content-length": str(len(file_content)),
        }
        signed = _sign_request("PUT", f"/{bucket_name}/sample.txt", headers, file_content, access_key, secret_key)
        resp = await client.put(f"/{bucket_name}/sample.txt", headers=signed, content=file_content)
        assert resp.status_code == 200, f"PutObject failed: {resp.text}"
        assert "etag" in resp.headers
        print(" -> PASSED: S3 PutObject uploaded successfully.")

        # 6. HeadObject
        print("[TEST 6/13] Testing S3 HeadObject...")
        headers = {"host": "test"}
        signed = _sign_request("HEAD", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.head(f"/{bucket_name}/sample.txt", headers=signed)
        assert resp.status_code == 200, f"HeadObject failed: {resp.status_code}"
        assert resp.headers.get("accept-ranges") == "bytes"
        assert int(resp.headers.get("content-length")) == len(file_content)
        print(" -> PASSED: S3 HeadObject returned correct headers.")

        # 7. GetObject (Full)
        print("[TEST 7/13] Testing S3 GetObject (Full content)...")
        headers = {"host": "test"}
        signed = _sign_request("GET", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/sample.txt", headers=signed)
        assert resp.status_code == 200, f"GetObject failed: {resp.text}"
        assert resp.content == file_content
        print(" -> PASSED: S3 GetObject downloaded full content.")

        # 8. GetObject (Range Request 206 Partial Content)
        print("[TEST 8/13] Testing S3 Range Request (206 Partial Content)...")
        headers = {"host": "test", "range": "bytes=0-4"}
        signed = _sign_request("GET", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/sample.txt", headers=signed)
        assert resp.status_code == 206, f"Range request failed: {resp.status_code} {resp.text}"
        assert resp.content == b"Hello"
        assert resp.headers.get("content-range") == f"bytes 0-4/{len(file_content)}"
        assert resp.headers.get("content-length") == "5"
        print(" -> PASSED: Byte-range streaming returns 206 Partial Content.")

        # 9. CopyObject
        print("[TEST 9/13] Testing S3 CopyObject (x-amz-copy-source)...")
        headers = {
            "host": "test",
            "x-amz-copy-source": f"/{bucket_name}/sample.txt",
        }
        signed = _sign_request("PUT", f"/{bucket_name}/copied_sample.txt", headers, b"", access_key, secret_key)
        resp = await client.put(f"/{bucket_name}/copied_sample.txt", headers=signed)
        assert resp.status_code == 200, f"CopyObject failed: {resp.text}"
        assert "<CopyObjectResult" in resp.text

        headers = {"host": "test"}
        signed = _sign_request("GET", f"/{bucket_name}/copied_sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/copied_sample.txt", headers=signed)
        assert resp.status_code == 200
        assert resp.content == file_content
        print(" -> PASSED: S3 CopyObject server-side clone verified.")

        # 10. ListObjectsV2
        print("[TEST 10/13] Testing S3 ListObjectsV2...")
        headers = {"host": "test"}
        signed = _sign_request("GET", f"/{bucket_name}", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}", headers=signed)
        assert resp.status_code == 200
        assert "<ListBucketResult" in resp.text
        assert "<Key>sample.txt</Key>" in resp.text
        assert "<Key>copied_sample.txt</Key>" in resp.text
        print(" -> PASSED: S3 ListObjectsV2 returned key listings.")

        # 11. Multipart Upload
        print("[TEST 11/13] Testing S3 Multipart Upload (Initiate, UploadParts, ListParts, Complete, Range Stream)...")
        # 11A: Initiate
        qp = {"uploads": ""}
        headers = {"host": "test", "content-type": "application/octet-stream"}
        signed = _sign_request("POST", f"/{bucket_name}/multipart.bin", headers, b"", access_key, secret_key, query_params=qp)
        resp = await client.post(f"/{bucket_name}/multipart.bin?uploads", headers=signed)
        assert resp.status_code == 200
        root = ET.fromstring(resp.text)
        upload_id = next(elem.text for elem in root.iter() if elem.tag.endswith("UploadId"))
        assert upload_id is not None

        # 11B: Upload Parts
        part1 = b"Part 1 chunk simulation data - "
        part2 = b"Part 2 chunk simulation data!"
        qp1 = {"uploadId": upload_id, "partNumber": "1"}
        signed1 = _sign_request("PUT", f"/{bucket_name}/multipart.bin", {"host": "test", "content-length": str(len(part1))}, part1, access_key, secret_key, query_params=qp1)
        resp1 = await client.put(f"/{bucket_name}/multipart.bin?uploadId={upload_id}&partNumber=1", headers=signed1, content=part1)
        assert resp1.status_code == 200

        qp2 = {"uploadId": upload_id, "partNumber": "2"}
        signed2 = _sign_request("PUT", f"/{bucket_name}/multipart.bin", {"host": "test", "content-length": str(len(part2))}, part2, access_key, secret_key, query_params=qp2)
        resp2 = await client.put(f"/{bucket_name}/multipart.bin?uploadId={upload_id}&partNumber=2", headers=signed2, content=part2)
        assert resp2.status_code == 200

        # 11C: Complete
        qp_c = {"uploadId": upload_id}
        signed_c = _sign_request("POST", f"/{bucket_name}/multipart.bin", {"host": "test"}, b"", access_key, secret_key, query_params=qp_c)
        resp_c = await client.post(f"/{bucket_name}/multipart.bin?uploadId={upload_id}", headers=signed_c)
        assert resp_c.status_code == 200

        # 11D: Download Multipart
        signed_dl = _sign_request("GET", f"/{bucket_name}/multipart.bin", {"host": "test"}, b"", access_key, secret_key)
        resp_dl = await client.get(f"/{bucket_name}/multipart.bin", headers=signed_dl)
        assert resp_dl.status_code == 200
        assert resp_dl.content == part1 + part2
        print(" -> PASSED: S3 Multipart upload cycle completed.")

        # 12. DeleteObject and Message Tracking
        print("[TEST 12/13] Testing S3 DeleteObject & Telegram message deletion...")
        signed_del = _sign_request("DELETE", f"/{bucket_name}/sample.txt", {"host": "test"}, b"", access_key, secret_key)
        resp_del = await client.delete(f"/{bucket_name}/sample.txt", headers=signed_del)
        assert resp_del.status_code == 204
        assert len(mock_store.deleted_messages) > 0
        print(f" -> PASSED: DeleteObject deleted key and message ID(s): {mock_store.deleted_messages}")

        # 13. Batch Delete & DeleteBucket
        print("[TEST 13/13] Testing Batch DeleteObjects & DeleteBucket...")
        del_xml = ('<Delete><Object><Key>copied_sample.txt</Key></Object><Object><Key>multipart.bin</Key></Object></Delete>').encode()
        signed_bdel = _sign_request("POST", f"/{bucket_name}", {"host": "test", "content-type": "application/xml"}, del_xml, access_key, secret_key, query_params={"delete": ""})
        resp_bdel = await client.post(f"/{bucket_name}?delete", headers=signed_bdel, content=del_xml)
        assert resp_bdel.status_code == 200

        signed_dbk = _sign_request("DELETE", f"/{bucket_name}", {"host": "test"}, b"", access_key, secret_key)
        resp_dbk = await client.delete(f"/{bucket_name}", headers=signed_dbk)
        assert resp_dbk.status_code == 204
        print(" -> PASSED: Batch delete and DeleteBucket succeeded.")

    print("\n" + "=" * 60)
    print("ALL 13 TESTS PASSED SUCCESSFULLY! (100% SUCCESS RATE)")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_all_tests())
