# S3 Bulk File Processor

`S3 Bulk File Processor` is a **Micronaut-based Java application** for processing large files stored in **Amazon S3**.  
It reads a file from S3 in **parallel chunks** (without loading the full file into memory), processes each chunk independently, and uploads the results back to S3 using **multipart upload**.

The design is **extensible** — you can customize the chunk-processing logic (e.g., enrich data, call external APIs, transform records) and rebuild your own version.

---

## ✨ Features
- Efficient **chunked reading** from S3 (does not load the full file into memory).
- Parallel processing of chunks using configurable worker threads.
- **Line-Safe Splitting** – Chunks are carefully aligned on line boundaries, **guaranteeing no overlap and no missing lines**. Every line in the source file is processed exactly once.  
- **Multipart upload** to S3 (supports files >5MB).
- Configurable chunking by **line count** or **byte size**.
- Extensible hook for custom processing (transform, enrich, validate).
- Packaged as a standalone **runnable JAR**.

---

## 📦 Build

Clone and build using Maven:

```bash
git clone https://github.com/ArunKumarPal/S3-Bulk-File-Processor.git
cd s3-file-processor
mvn clean package
```

This will create the JAR under:

```
target/s3-file-processor.jar
```

---

## 🚀 Run

You can run the JAR with required parameters:

```bash
java -jar target/s3-file-processor.jar   --bucketName=my-bucket   --inputFileKey=input/large-file.csv   --outputFileKey=output/processed-file.csv   --delimiter=,
```

---

## ⚙️ Parameters

### Required
| Property       | Description                          | Example                        |
|----------------|--------------------------------------|--------------------------------|
| `bucketName`   | S3 bucket name                       | `my-bucket`                    |
| `inputFileKey` | S3 key of input file                 | `input/large-file.csv`         |
| `outputFileKey`| S3 key where processed file is saved | `output/processed-file.csv`    |
| `delimiter`    | Field delimiter in the file          | `,`                            |

### Optional
| Property        | Description                                      | Default        |
|-----------------|--------------------------------------------------|----------------|
| `minChunkLines` | Minimum number of lines per chunk                | `5000`         |
| `minChunkSize`  | Minimum size of each chunk (≥ 5MB recommended)   | `5242880` (5MB)|
| `maxThread`     | Max number of parallel chunk processors           | `4`            |

---

## 🔄 Processing Workflow

```
                ┌───────────────────────┐
                │       S3 Bucket       │
                │   (inputFileKey)      │
                └─────────┬─────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Get File Headers    │
              │       (header[])      │
              └─────────┬─────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ Initiate Multipart     │
              │   Upload → UploadId    │
              └─────────┬─────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Split into Chunks   │
              │ (List<Chunk>, threads)│
              └─────────┬─────────────┘
                          │
              ┌───────────┴──────────────┐
              │                          │
       ┌─────────────┐            ┌─────────────┐
       │ Process     │   ...      │ Process     │
       │ Chunk #1    │   │        │ Chunk #N    │
       │ (Thread)    │   │        │ (Thread)    │
       └─────┬───────┘            └─────┬───────┘
             │                           │
             ▼                           ▼
       ┌─────────────┐            ┌─────────────┐
       │ Upload Part │            │ Upload Part │
       │  to S3      │            │  to S3      │
       └─────┬───────┘            └─────┬───────┘
             └───────────┬──────────────┘
                         ▼
              ┌───────────────────────┐
              │ Complete Multipart    │
              │      Upload           │
              └───────────────────────┘
```

Steps:
1. Download file in **chunks** from S3 using range requests.
2. Split into records based on the configured `delimiter`.
3. **Process each record** (default: passthrough).  
   👉 Developers can plug in custom logic (e.g., call external API, apply transformation).
4. Collect processed chunk results.
5. Upload back to S3 using **multipart upload**.
6. Complete the multipart upload to assemble final file.

---

## 🛠️ Customization
You can modify the chunk processing logic:

- Locate the method `processChunk(...)` in the codebase.
- Replace the default passthrough with your logic (API calls, transformations, validations, etc.).
- Rebuild the project with Maven.

---

## 🌐 Example Run

```bash
java -jar target/s3-file-processor.jar   --bucketName=data-bucket   --inputFileKey=raw/input.csv   --outputFileKey=processed/output.csv   --delimiter="|"   --minChunkLines=10000   --minChunkSize=10485760   --maxThread=8
```

---

## 📋 Notes
- **AWS credentials** must be available via:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), or
  - AWS CLI profile, or
  - IAM role attached to the instance/container.
- Ensure `minChunkSize >= 5MB` for proper S3 multipart upload.
- Works with any file type that can be processed line-by-line (CSV, TSV, JSONL, etc.).

---

