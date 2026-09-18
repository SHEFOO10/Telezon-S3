import asyncio
from copy import deepcopy
import hashlib
import hmac
import io
import os
import urllib.parse
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from bson import ObjectId
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import DATABASE_NAME
from app.crud.bucket import crud_create_bucket, crud_get_bucket_by_name
from app.crud.user import crud_create_user, crud_get_user_by_username
from app.main import app
from app.models.bucket import BucketInCreate
from app.models.user import UserInCreate
from app.s3.utils import parse_range_header
import app.db.mongodb as mongodb_module


# ---------------------------------------------------------------------------
# Lightweight In-Memory Async Mongo Mock for Tests
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
# In-Memory Mock Storage for Deterministic Unit/Integration Testing
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


# ---------------------------------------------------------------------------
# SigV4 Helper for Test Requests
# ---------------------------------------------------------------------------
def _sign_request(method, url_path, headers, body, access_key, secret_key, region="us-east-1", service="s3", query_params=None):
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    headers_to_sign = dict(headers)
    headers_to_sign["x-amz-date"] = amz_date

    # Canonical query string
    if query_params:
        sorted_params = sorted(query_params.items())
        canonical_qs = "&".join(f"{urllib.parse.quote(str(k), safe='') }={urllib.parse.quote(str(v), safe='')}" for k, v in sorted_params)
    else:
        canonical_qs = ""

    # Canonical headers
    sorted_header_keys = sorted(k.lower() for k in headers_to_sign.keys())
    canonical_headers = "".join(f"{k}:{headers_to_sign[k].strip()}\n" for k in sorted_header_keys)
    signed_headers = ";".join(sorted_header_keys)

    # Payload hash
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


# ---------------------------------------------------------------------------
# Unit Tests: Range Header Parser
# ---------------------------------------------------------------------------
def test_parse_range_header():
    total_size = 1000

    # Normal range
    assert parse_range_header("bytes=0-499", total_size) == (0, 499)
    # Range start only
    assert parse_range_header("bytes=500-", total_size) == (500, 999)
    # Suffix range
    assert parse_range_header("bytes=-100", total_size) == (900, 999)
    # Out of bounds clamped
    assert parse_range_header("bytes=0-2000", total_size) == (0, 999)
    # Invalid ranges
    assert parse_range_header("bytes=1500-", total_size) is None
    assert parse_range_header("bytes=500-200", total_size) is None
    assert parse_range_header(None, total_size) is None
    assert parse_range_header("invalid", total_size) is None


# ---------------------------------------------------------------------------
# Integration Tests: S3 API Endpoints
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_s3_full_flow(monkeypatch):
    mock_store = MockStorage()
    # Inject mock storage
    import app.s3 as s3_module
    monkeypatch.setattr(s3_module, "storage", mock_store)

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
        # 1. Health Check
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        health_data = resp.json()
        assert "status" in health_data

        # 2. S3 CreateBucket (PUT /{bucket_name})
        headers = {"host": "test"}
        signed_headers = _sign_request("PUT", f"/{bucket_name}", headers, b"", access_key, secret_key)
        resp = await client.put(f"/{bucket_name}", headers=signed_headers)
        assert resp.status_code in (200, 201)

        # 3. S3 ListBuckets (GET /)
        headers = {"host": "test"}
        signed_headers = _sign_request("GET", "/", headers, b"", access_key, secret_key)
        resp = await client.get("/", headers=signed_headers)
        assert resp.status_code == 200
        assert "<ListAllMyBucketsResult" in resp.text
        assert f"<Name>{bucket_name}</Name>" in resp.text

        # 4. S3 PutObject (PUT /{bucket_name}/sample.txt)
        file_content = b"Hello, Telezon S3 Storage Engine with Range Requests!"
        headers = {
            "host": "test",
            "content-type": "text/plain",
            "content-length": str(len(file_content)),
        }
        signed_headers = _sign_request("PUT", f"/{bucket_name}/sample.txt", headers, file_content, access_key, secret_key)
        resp = await client.put(f"/{bucket_name}/sample.txt", headers=signed_headers, content=file_content)
        assert resp.status_code == 200
        assert "etag" in resp.headers

        # 5. S3 HeadObject (HEAD /{bucket_name}/sample.txt)
        headers = {"host": "test"}
        signed_headers = _sign_request("HEAD", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.head(f"/{bucket_name}/sample.txt", headers=signed_headers)
        assert resp.status_code == 200
        assert resp.headers.get("accept-ranges") == "bytes"
        assert int(resp.headers.get("content-length")) == len(file_content)

        # 6. S3 GetObject (Full content)
        headers = {"host": "test"}
        signed_headers = _sign_request("GET", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/sample.txt", headers=signed_headers)
        assert resp.status_code == 200
        assert resp.content == file_content

        # 7. S3 GetObject with HTTP Range (206 Partial Content)
        headers = {"host": "test", "range": "bytes=0-4"}
        signed_headers = _sign_request("GET", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/sample.txt", headers=signed_headers)
        assert resp.status_code == 206
        assert resp.content == b"Hello"
        assert resp.headers.get("content-range") == f"bytes 0-4/{len(file_content)}"
        assert resp.headers.get("content-length") == "5"

        # 8. S3 CopyObject (x-amz-copy-source)
        copy_source = f"/{bucket_name}/sample.txt"
        headers = {
            "host": "test",
            "x-amz-copy-source": copy_source,
        }
        signed_headers = _sign_request("PUT", f"/{bucket_name}/copied_sample.txt", headers, b"", access_key, secret_key)
        resp = await client.put(f"/{bucket_name}/copied_sample.txt", headers=signed_headers)
        assert resp.status_code == 200
        assert "<CopyObjectResult" in resp.text

        # Verify copied object download
        headers = {"host": "test"}
        signed_headers = _sign_request("GET", f"/{bucket_name}/copied_sample.txt", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}/copied_sample.txt", headers=signed_headers)
        assert resp.status_code == 200
        assert resp.content == file_content

        # 9. S3 ListObjectsV2
        headers = {"host": "test"}
        signed_headers = _sign_request("GET", f"/{bucket_name}", headers, b"", access_key, secret_key)
        resp = await client.get(f"/{bucket_name}", headers=signed_headers)
        assert resp.status_code == 200
        assert "<ListBucketResult" in resp.text
        assert "<Key>sample.txt</Key>" in resp.text
        assert "<Key>copied_sample.txt</Key>" in resp.text

        # 10. S3 Multipart Upload Flow
        # 10A: Initiate (?uploads)
        headers = {"host": "test", "content-type": "application/octet-stream"}
        qp = {"uploads": ""}
        signed_headers = _sign_request("POST", f"/{bucket_name}/multipart.bin", headers, b"", access_key, secret_key, query_params=qp)
        resp = await client.post(f"/{bucket_name}/multipart.bin?uploads", headers=signed_headers)
        assert resp.status_code == 200
        assert "<InitiateMultipartUploadResult" in resp.text
        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)
        upload_id = None
        for elem in root.iter():
            if elem.tag.endswith("UploadId"):
                upload_id = elem.text
                break
        assert upload_id is not None

        # 10B: UploadPart (part 1 & part 2)
        part1_data = b"Part 1 Data Content (5MB chunk simulation) - "
        part2_data = b"Part 2 Data Content (final chunk simulation)!"
        
        qp1 = {"uploadId": upload_id, "partNumber": "1"}
        headers1 = {"host": "test", "content-length": str(len(part1_data))}
        signed1 = _sign_request("PUT", f"/{bucket_name}/multipart.bin", headers1, part1_data, access_key, secret_key, query_params=qp1)
        resp1 = await client.put(f"/{bucket_name}/multipart.bin?uploadId={upload_id}&partNumber=1", headers=signed1, content=part1_data)
        assert resp1.status_code == 200

        qp2 = {"uploadId": upload_id, "partNumber": "2"}
        headers2 = {"host": "test", "content-length": str(len(part2_data))}
        signed2 = _sign_request("PUT", f"/{bucket_name}/multipart.bin", headers2, part2_data, access_key, secret_key, query_params=qp2)
        resp2 = await client.put(f"/{bucket_name}/multipart.bin?uploadId={upload_id}&partNumber=2", headers=signed2, content=part2_data)
        assert resp2.status_code == 200

        # 10C: ListParts (?uploadId=...)
        qp_lp = {"uploadId": upload_id}
        headers_lp = {"host": "test"}
        signed_lp = _sign_request("GET", f"/{bucket_name}/multipart.bin", headers_lp, b"", access_key, secret_key, query_params=qp_lp)
        resp_lp = await client.get(f"/{bucket_name}/multipart.bin?uploadId={upload_id}", headers=signed_lp)
        assert resp_lp.status_code == 200
        assert "<ListPartsResult" in resp_lp.text
        assert "<PartNumber>1</PartNumber>" in resp_lp.text
        assert "<PartNumber>2</PartNumber>" in resp_lp.text

        # 10D: CompleteMultipartUpload (?uploadId=...)
        qp_comp = {"uploadId": upload_id}
        headers_comp = {"host": "test"}
        signed_comp = _sign_request("POST", f"/{bucket_name}/multipart.bin", headers_comp, b"", access_key, secret_key, query_params=qp_comp)
        resp_comp = await client.post(f"/{bucket_name}/multipart.bin?uploadId={upload_id}", headers=signed_comp)
        assert resp_comp.status_code == 200
        assert "<CompleteMultipartUploadResult" in resp_comp.text

        # 10E: Download Completed Multipart Object
        headers_get_mp = {"host": "test"}
        signed_get_mp = _sign_request("GET", f"/{bucket_name}/multipart.bin", headers_get_mp, b"", access_key, secret_key)
        resp_get_mp = await client.get(f"/{bucket_name}/multipart.bin", headers=signed_get_mp)
        assert resp_get_mp.status_code == 200
        assert resp_get_mp.content == part1_data + part2_data

        # 11. S3 DeleteObject & Message Deletion Check
        headers = {"host": "test"}
        signed_headers = _sign_request("DELETE", f"/{bucket_name}/sample.txt", headers, b"", access_key, secret_key)
        resp = await client.delete(f"/{bucket_name}/sample.txt", headers=signed_headers)
        assert resp.status_code == 204
        # Verify message was deleted in storage
        assert len(mock_store.deleted_messages) > 0

        # 12. S3 Batch Delete (POST /{bucket_name}?delete)
        delete_xml = (
            '<Delete><Object><Key>copied_sample.txt</Key></Object>'
            '<Object><Key>multipart.bin</Key></Object></Delete>'
        ).encode("utf-8")
        qp_del = {"delete": ""}
        headers_bdel = {"host": "test", "content-type": "application/xml"}
        signed_bdel = _sign_request("POST", f"/{bucket_name}", headers_bdel, delete_xml, access_key, secret_key, query_params=qp_del)
        resp_bdel = await client.post(f"/{bucket_name}?delete", headers=signed_bdel, content=delete_xml)
        assert resp_bdel.status_code == 200
        assert "<DeleteResult" in resp_bdel.text
        assert "<Key>copied_sample.txt</Key>" in resp_bdel.text
        assert "<Key>multipart.bin</Key>" in resp_bdel.text

        # 13. S3 DeleteBucket (DELETE /{bucket_name})
        headers = {"host": "test"}
        signed_headers = _sign_request("DELETE", f"/{bucket_name}", headers, b"", access_key, secret_key)
        resp = await client.delete(f"/{bucket_name}", headers=signed_headers)
        assert resp.status_code == 204
