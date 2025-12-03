# README


# Assignment 1: Text Analysis in the Cloud
## How to Run

### 1. Upload JARs to S3
```bash
# Create S3 bucket for JARs
aws s3 mb s3://ahd.jars

# Upload Manager (password-protected)
zip -P Ahd Manager-1.0-SNAPSHOT.zip manager-1.0-SNAPSHOT.jar
aws s3 cp Manager-1.0-SNAPSHOT.zip s3://ahd.jars/

# Upload Worker (password-protected)
zip -P Ahd Worker-1.0.0-SNAPSHOT.zip worker-1.0.0-SNAPSHOT-all.jar
aws s3 cp Worker-1.0.0-SNAPSHOT.zip s3://ahd.jars/

# Upload Stanford NLP JARs
aws s3 cp stanford-parser-3.6.0.jar s3://ahd.jars/nlp/
aws s3 cp stanford-parser-3.6.0-models.jar s3://ahd.jars/nlp/
aws s3 cp stanford-corenlp-3.6.0.jar s3://ahd.jars/nlp/
aws s3 cp stanford-corenlp-3.6.0-models.jar s3://ahd.jars/nlp/
```

### 2. Run LocalApp
```bash
# Basic usage
java -jar LocalApp.jar input.txt output.html 10

# With termination
java -jar LocalApp.jar input.txt output.html 10 terminate
```

**Input file format:**
```
POS	https://www.gutenberg.org/files/1661/1661-0.txt
CONSTITUENCY	https://www.gutenberg.org/files/1342/1342-0.txt
DEPENDENCY	https://example.com/sample.txt
```

---

## System Architecture

### Components
1. **LocalApp** - Uploads input to S3, starts Manager if needed, waits for HTML summary
2. **Manager** - Downloads input, creates worker tasks, spawns workers, collects results, generates HTML
3. **Workers** - Poll for tasks, download text files, perform NLP analysis, upload results to S3

### Communication
**4 SQS Queues:**
- `LocalToManagerQueue` - Job requests
- `ManagerToLocalQueue` - Completion notifications  
- `ManagerToWorkerQueue` - Analysis tasks
- `WorkerToManagerQueue` - Results

**Message Flow:**
```
LocalApp → S3 (upload input)
LocalApp → LocalToManagerQueue → Manager
Manager → S3 (download input) → ManagerToWorkerQueue
Workers → Download text → NLP analysis → S3 (upload results) → WorkerToManagerQueue
Manager → Collect results → S3 (upload HTML) → ManagerToLocalQueue
LocalApp → Download HTML summary
```

### Worker Calculation
```java
requiredWorkers = ceil(totalTasks / n)
// If n=10 and 25 files → need 3 workers
// Max: 19 instances (18 workers + 1 manager)
```

---

## EC2 Configuration

- **Instance Type:** t2.micro
- **AMI:** ami-00e95a9222311e8ed 
- **Regions:** us-west-2 (S3/SQS), us-east-1 (EC2)
- **S3 Bucket:** mahmood-ttest-05850805111213569
- **Max Instances:** 19
- **IAM Role:** LabInstanceProfile (no credentials in code)

---

## Performance Results

### Processing Time by File Size and Analysis Type

**POS Tagging:**
- Processing time: ~few seconds (regardless of file size)
- Fast and efficient for all file sizes

**CONSTITUENCY & DEPENDENCY Parsing:**

| File Size | Analysis Types | Processing Time | 
|-----------|---------------|-----------------|
| 178 KB | CONSTITUENCY & DEPENDENCY | ~2 minutes |
| 590 KB | CONSTITUENCY & DEPENDENCY | ~7 minutes |
| 1.1 MB | CONSTITUENCY & DEPENDENCY | ~20 minutes |

### Test Configuration

**n value used:** 1

**Why n=1?** 
- Maximum parallelization - creates 1 worker per file
- Significantly faster processing since each file gets dedicated worker
- Optimal for our workload where NLP analysis is CPU-intensive
- Trade-off: More workers spawned, but much faster completion time
- Example: 10 files with n=1 → 10 workers (vs n=10 → 1 worker)


## Questions

### Security
**Did you think about security?**

Yes:
1. **IAM Instance Profiles** - Workers and Manager use `LabInstanceProfile` role
   ```java
   .iamInstanceProfile(IamInstanceProfileSpecification.builder()
       .name("LabInstanceProfile").build())
   ```
   No credentials in code or user-data scripts.

2. **Password-Protected JARs** - All JARs compressed with password "Ahd"
   ```bash
   zip -P Ahd Manager.zip manager.jar
   ```

3. **No Hardcoded Credentials** - AWS SDK automatically uses IAM role

### Scalability  
**Will your program work with 1 million clients?**

Yes, with current limitations:
- **CachedThreadPool** - Manager scales threads dynamically for parallel LocalApp processing
- **SQS Buffering** - Unlimited message capacity

### Persistence & Fault Tolerance
**What if a node dies?**

1. **Worker Crashes:**
   - SQS visibility timeout = 1000 seconds (~16 minutes)
   - If worker dies, message becomes visible after timeout
   - Another worker automatically picks it up
   
2. **Duplicate Messages:**
   - Manager checks for duplicates using `analysisType + inputUrl`
   - Ignores duplicate successes
   - Replaces errors with successes

3. **Worker Stalls:**
   - HTTP connection timeout = 30 seconds
   - Read timeout = 30 seconds
   - Long visibility timeout (1000s) prevents premature retries

### Threading
**When are threads a good idea?**

**Good Use:**
- **Manager:** `CachedThreadPool` for parallel LocalApp processing
  ```java
  ExecutorService localAppProcessor = Executors.newCachedThreadPool();
  ```
  - Scales dynamically with workload
  - Each thread handles one LocalApp independently
  
- **Manager:** Separate thread for collecting worker results
  ```java
  Thread resultCollectorThread = new Thread(this::collectWorkerResults);
  ```

**Thread Safety:**
- `ConcurrentHashMap<String, JobContext>` for job tracking
- `synchronized` blocks when updating results
- `Collections.synchronizedList()` for result lists

### Multiple Clients
**Did you run more than one client?**

Yes. Each LocalApp:
- Gets unique Job ID: `job-<timestamp>`
- Uploads to separate folder: `jobs/<jobId>/input.txt`
- Manager processes in parallel using thread pool
- Results filtered by Job ID in response queue

### Termination
**Did you manage termination?**

Yes:
1. LocalApp sends "terminate" message
2. Manager sets `shouldTerminate = true`
3. Manager stops accepting new jobs
4. Manager waits for active jobs to complete
5. Manager terminates all workers
6. Manager terminates itself

### System Limitations
**Did you consider system limitations?**

Yes:
- **Max 19 instances** enforced
- **Visibility timeout:** 1000 seconds (safe for long-running tasks)
- **Thread pool:** CachedThreadPool (scales dynamically)
- **Worker calculation respects limit:**
  ```java
  workersToStart = Math.min(requiredWorkers, MAX_INSTANCES - currentWorkers);
  ```

### Worker Distribution
**Are all workers working equally?**

Yes:
- Workers pull from shared `ManagerToWorkerQueue`
- SQS automatically distributes messages
- No idle workers - each processes until queue empty
- Load balancing is automatic

### Manager Workload
**Is your manager doing too much work?**

No. Manager only:
- Coordinates: receives requests, creates tasks, spawns workers
- Tracks job progress
- Generates HTML summaries

Workers do the heavy work:
- Download text files
- Perform CPU-intensive NLP analysis
- Upload results

### Distributed System
**Do you understand what distributed means?**

Yes. Our system is distributed:
- **Async Communication:** All via SQS (no direct calls)
- **Independent Workers:** Don't know about each other
- **No Blocking:** Manager processes multiple LocalApps in parallel
- **Horizontal Scaling:** Can add workers dynamically

**Intentional Waiting:**
- LocalApp waits for result (user is waiting anyway)
- Manager waits for jobs before shutdown (graceful termination)

---

**Last Updated:** [Date]