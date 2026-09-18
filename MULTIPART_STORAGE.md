# S3 Multipart Upload Architecture & Modes

Telezon-S3 supports two configurable modes for handling large file multipart uploads:

1. **`diskless` (Default)**: Direct part streaming to Telegram with MongoDB manifest. (Recommended for stateless/cloud containers like Render, Docker, Kubernetes).
2. **`assembled`**: Local temporary disk assembly of parts before uploading a single document to Telegram.

---

## Configuration

You can toggle between the two modes using the `MULTIPART_MODE` environment variable in your `.env` file:

```env
# Options: "diskless" (default) or "assembled"
MULTIPART_MODE=diskless
```

---

## Comparison of Modes

| Feature | `diskless` (Default) | `assembled` (Legacy / Disk Mode) |
| :--- | :--- | :--- |
| **Local Disk Required** | ❌ **0 MB (No disk needed)** |  **Requires disk space equal to file size** |
| **RAM Footprint** |  **Constant ~10MB–20MB** |  **Constant ~10MB–20MB** |
| **Complete Time** | ⚡ **Instantaneous** (manifest save) | ⏳ **Re-uploads the full merged file to Telegram** |
| **Telegram Storage** | Each part is stored as a separate message | Single unified document message per file |
| **Download Behavior** | Parts are streamed sequentially on-the-fly | Single file is streamed directly |
| **Max File Size** | Up to 2GB+ (MTProto User API) | Up to 2GB+ (MTProto User API) |
| **Best Used For** | Render, Heroku, Serverless, Docker | Persistent servers with attached storage volumes |

---

## How Each Mode Works

### 1. `diskless` Mode (`MULTIPART_MODE=diskless`)

```text
Client -> PUT /bucket/file?partNumber=1&uploadId=...
          └─ Telezon-S3 uploads chunk directly to Telegram -> records part_file_id in MongoDB.
          (Zero bytes written to local disk)

Client -> POST /bucket/file?uploadId=... (Complete)
          └─ Telezon-S3 saves manifest [part_1_id, part_2_id, ...] to MongoDB Blob record.
          (Instant completion, no re-uploading needed)

Client -> GET /bucket/file (Download)
          └─ Telezon-S3 streams part 1, part 2, ... part N sequentially on-the-fly as one continuous HTTP stream.
```

### 2. `assembled` Mode (`MULTIPART_MODE=assembled`)

```text
Client -> PUT /bucket/file?partNumber=1&uploadId=...
          └─ Telezon-S3 writes chunk to /tmp/telezon_uploadId_1.part.

Client -> POST /bucket/file?uploadId=... (Complete)
          └─ Telezon-S3 stitches all .part files into /tmp/telezon_assembled_uploadId.tmp
          └─ Uploads the assembled 2GB file to Telegram as a single message
          └─ Deletes all temporary disk files.

Client -> GET /bucket/file (Download)
          └─ Telezon-S3 streams the single Telegram document.
```

---

## S3 Client Compatibility

Both modes conform 100% to the AWS S3 REST API specifications:
* **Laravel / Flysystem**: `Storage::disk('s3')->put(...)` and multipart stream writers.
* **AWS SDK / Boto3**: `s3.upload_file(...)`, `s3.create_multipart_upload(...)`.
* **AWS CLI**: `aws s3 cp large_file.mp4 s3://mybucket/`.
* **Cyberduck / Transmit / rclone**: Standard drag-and-drop chunked uploads.
