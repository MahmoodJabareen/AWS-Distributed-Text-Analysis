package com.myapp;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class Manager {
    
    private final AWS aws;
    
    
    private final String localToManagerQueueUrl;      // Local → Manager (requests)
    private final String managerToLocalQueueUrl;      // Manager → Local (responses)
    private final String managerToWorkerQueueUrl;     // Manager → Workers (tasks)
    private final String workerToManagerQueueUrl;     // Workers → Manager (results)
    
    // State
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private final Map<String, JobContext> activeJobs = new ConcurrentHashMap<>();
    //Thread pool for parallel processing
    private final ExecutorService localAppProcessor =Executors.newCachedThreadPool();   
    private static final String WORKER_JAR_KEY = "worker.jar";
    private static final int MAX_INSTANCES = 19;
    
    public Manager() {
        System.out.println("=== Initializing Manager ===");
        
        
        this.aws = AWS.getInstance();
        
        
        
        aws.createBucketIfNotExists(aws.bucketName);
        try {
        aws.uploadStringToS3("manager_started.txt", "Manager is running");
        } catch (Exception e) {
        e.printStackTrace();
        }
        
        
        this.localToManagerQueueUrl = aws.getOrCreateQueue("LocalToManagerQueue");
        this.managerToLocalQueueUrl = aws.getOrCreateQueue("ManagerToLocalQueue");
        this.managerToWorkerQueueUrl = aws.getOrCreateQueue("ManagerToWorkerQueue");
        this.workerToManagerQueueUrl = aws.getOrCreateQueue("WorkerToManagerQueue");
        
        System.out.println("[INFO] Manager initialized successfully!");
        System.out.println("[INFO] Local → Manager queue: LocalToManagerQueue");
        System.out.println("[INFO] Manager → Local queue: ManagerToLocalQueue");
    }
    
    public void run() {
        System.out.println("=== Manager Started ===");
        Thread resultCollectorThread = new Thread(this::collectWorkerResults, "ResultCollector");
        resultCollectorThread.start();
        
        
        while (!shouldTerminate.get() || !activeJobs.isEmpty()) {
            try {
                processLocalApplicationMessages();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[ERROR] Main loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("[INFO] Manager loop ended - shutting down");
    // Tell collector to stop and wait for it to finish
    shouldTerminate.set(true);
    try {
        resultCollectorThread.join();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }

    shutdown();
}
    
    
    private void processLocalApplicationMessages() {
    List<Message> messages = aws.receiveMessages(localToManagerQueueUrl, 10, 5);
    System.out.println("[DEBUG] Polled " + messages.size() + " messages from LocalToManagerQueue");
    
    for (Message message : messages) {
         //Submit each message to thread pool for parallel processing
            localAppProcessor.submit(() -> {
        try {
            System.out.println("[INFO] Received from local app: " + message.body());
            handleLocalApplicationMessage(message);
            System.out.println("[DEBUG-AWS] deleteMessage() called for receipt: " + message.receiptHandle());
            aws.deleteMessage(localToManagerQueueUrl, message.receiptHandle());
            System.out.println("[DEBUG-AWS] deleteMessage() finished");
        } catch (Exception e) {
            System.err.println("[ERROR] Processing message: " + e.getMessage());
            e.printStackTrace();
        }
    });

        }
    }

    
    
    private void handleLocalApplicationMessage(Message message) {
        String body = message.body();
        String[] parts = body.split("\t");
        String messageType = parts[0];
        
        if (messageType.equals("terminate")) {
            handleTerminationMessage();
        } else if (messageType.equals("new_task")) {
            // Message format: "new_task\t<s3_location>\t<local_app_id>\t<n>"
            // s3_location can be either:
            //   - Just a key: "inputs/job1.txt" (uses default bucket)
            //   - Full S3 URL: "s3://bucket-name/inputs/job1.txt"
            String s3Location = parts[1];
            String localAppId = parts[2];
            int n = Integer.parseInt(parts[3]);
            handleNewTask(s3Location, localAppId, n);
        } else {
            System.err.println("[WARNING] Unknown message type: " + messageType);
        }
    }
    
  
    private void handleNewTask(String s3Location, String localAppId, int n) {
        if (shouldTerminate.get()) {
            System.out.println("[WARNING] Rejected task - termination in progress");
            return;
        }
        
        System.out.println("=== NEW TASK ===");
        System.out.println("Local App ID: " + localAppId);
        System.out.println("S3 Location: " + s3Location);
        System.out.println("Files per worker (n): " + n);
        
        try {
            // FIXED: Parse S3 location (handle both key and full URL)
            String bucket;
            String key;
            
            if (s3Location.startsWith("s3://")) {
                // Full S3 URL
                String[] s3Parts = s3Location.substring(5).split("/", 2);
                bucket = s3Parts[0];
                key = s3Parts.length > 1 ? s3Parts[1] : "";
                System.out.println("[INFO] Parsed S3 URL - Bucket: " + bucket + ", Key: " + key);
            } else {
                // Just a key, use default bucket
                bucket = aws.bucketName;
                key = s3Location;
                System.out.println("[INFO] Using default bucket: " + bucket + ", Key: " + key);
            }
            
            // Download input file using AWS helper
            String inputContent = aws.downloadStringFromS3(bucket, key);
            System.out.println("[INFO] Downloaded input file");
            
            // Parse lines
            String[] lines = inputContent.split("\n");
            System.out.println("[INFO] Input has " + lines.length + " lines");
            
            // Create job context (don't need to track bucket for summary)
            JobContext jobContext = new JobContext(localAppId);
            // Create worker tasks
            int taskCount = createWorkerTasks(lines, localAppId);
            jobContext.setTotalTasks(taskCount);
            System.out.println("[INFO] Created " + taskCount + " tasks");
             activeJobs.put(localAppId, jobContext);
            
            // Start workers
            int requiredWorkers = (int) Math.ceil((double) taskCount / n);
            System.out.println("[INFO] Required workers: " + requiredWorkers);
            
            // Check for empty input case
            if (taskCount == 0) {
                System.out.println("[WARNING] No valid tasks created - generating empty summary");
                generateSummaryFile(localAppId, jobContext);
                activeJobs.remove(localAppId);
                return;
            }
            
            startWorkersIfNeeded(requiredWorkers);
            
        } catch (Exception e) {
            System.err.println("[ERROR] Handling new task: " + e.getMessage());
            e.printStackTrace();
        }
    }
    

    private int createWorkerTasks(String[] lines, String localAppId) {
        int taskCount = 0;
        
        for (String line : lines) {
            if (line.trim().isEmpty()) continue;
            
            String[] parts = line.split("\t");
            if (parts.length != 2) {
                System.err.println("[WARNING] Invalid line: " + line);
                continue;
            }
            
            String analysisType = parts[0].trim();
            String url = parts[1].trim();
            
            // Validate analysis type
            if (!analysisType.equals("POS") && 
                !analysisType.equals("CONSTITUENCY") && 
                !analysisType.equals("DEPENDENCY")) {
                System.err.println("[WARNING] Invalid analysis type: " + analysisType);
                continue;
            }
            
            // Create message: "ANALYSIS_TYPE\tURL\tLOCAL_APP_ID"
            String workerMessage = String.format("%s\t%s\t%s", analysisType, url, localAppId);
            aws.sendMessage(managerToWorkerQueueUrl, workerMessage);
            
            taskCount++;
        }
        
        return taskCount;
    }
    
    private void startWorkersIfNeeded(int requiredWorkers) throws IOException {
        int currentWorkers = aws.getRunningWorkerCount();
        System.out.println("[INFO] Current workers: " + currentWorkers);
        
        int workersToStart = requiredWorkers - currentWorkers;
        
        if (workersToStart <= 0) {
            System.out.println("[INFO] Sufficient workers running");
            return;
        }
        
        // Respect instance limit
        int maxNewWorkers = (MAX_INSTANCES - 1) - currentWorkers;
        workersToStart = Math.min(workersToStart, maxNewWorkers);
        
        if (workersToStart <= 0) {
            System.out.println("[WARNING] Cannot start more workers (at limit)");
            return;
        }
        
        System.out.println("[INFO] Starting " + workersToStart + " workers");
        String userData = createWorkerUserData();
        try{
           if (workersToStart > 0) {
                 aws.createEC2(userData, "Worker", workersToStart);
            }
            
            System.out.println("[INFO] Workers started successfully");
        } catch (Exception e) {
            System.err.println("[ERROR] Starting workers: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
  private String createWorkerUserData() {

    String dataBucket = AWS.getInstance().bucketName; // logs + markers
    String jarFilesBucket = "ahd.jars";

  return "#!/bin/bash\n" +
           "# Workers use LabInstanceProfile IAM role - no credentials needed\n" +
           "cd ~\n" +

           "INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo unknown)\n" +
           "echo 'worker-userdata-ok' > worker_userdata_marker-${INSTANCE_ID}.txt\n" +
           "aws s3 cp worker_userdata_marker-${INSTANCE_ID}.txt s3://" + dataBucket + "/worker_userdata_markers/worker_userdata_marker-${INSTANCE_ID}.txt\n" +

           "# Download worker zip (password-protected)\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/Worker-1.0.0-SNAPSHOT.zip Worker-1.0.0-SNAPSHOT.zip\n" +
           "unzip -P Ahd Worker-1.0.0-SNAPSHOT.zip\n" +

           "# Download Stanford jars\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/nlp/stanford-parser-3.6.0.jar stanford-parser-3.6.0.jar\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/nlp/stanford-parser-3.6.0-models.jar stanford-parser-3.6.0-models.jar\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/nlp/stanford-corenlp-3.6.0.jar stanford-corenlp-3.6.0.jar\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/nlp/stanford-corenlp-3.6.0-models.jar stanford-corenlp-3.6.0-models.jar\n" +

           "# Start Worker\n" +
           "nohup java -cp \"worker-1.0.0-SNAPSHOT-all.jar:stanford-parser-3.6.0.jar:stanford-parser-3.6.0-models.jar:stanford-corenlp-3.6.0.jar:stanford-corenlp-3.6.0-models.jar\" com.myapp.Worker > worker.log 2>&1 &\n" +

           "# Periodic log snapshots\n" +
           "(\n" +
           "  while true; do\n" +
           "    sleep 120\n" +
           "    SNAPSHOT=\"worker-${INSTANCE_ID}-$(date +%Y%m%d-%H%M%S).log\"\n" +
           "    cp worker.log \"$SNAPSHOT\"\n" +
           "    aws s3 cp \"$SNAPSHOT\" s3://" + dataBucket + "/worker_logs/\"$SNAPSHOT\" || true\n" +
           "  done\n" +
           ") &\n";
}

    
    // Collect results from workers (background thread)
    private void collectWorkerResults() {
        System.out.println("[INFO] Result collector started");
        
        while (!shouldTerminate.get() || !activeJobs.isEmpty()) {
            try {
                List<Message> messages = aws.receiveMessages(workerToManagerQueueUrl, 10, 5);
                
                for (Message message : messages) {
                    handleWorkerResult(message);
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Collecting results: " + e.getMessage());
            }
        }
        
        System.out.println("[INFO] Result collector ended");
    }
    
private void handleWorkerResult(Message message) {
    try {
        String body = message.body();
        System.out.println("[INFO] Worker result: " + body);
        
        // Parse worker message
        String[] parts = body.split("\t", -1);
        
        if (parts.length < 6) {
            System.err.println("[ERROR] Invalid worker message format: " + body);
            aws.deleteMessage(workerToManagerQueueUrl, message.receiptHandle());
            return;
        }
        
        String analysisType = parts[0];
        String inputUrl = parts[1];
        String outputS3Key = parts[2];
        String localAppId = parts[3];
        String status = parts[4];
        String error = parts[5];
        
        // Find job
        JobContext jobContext = activeJobs.get(localAppId);
        if (jobContext != null) {
            synchronized (jobContext) {
                
                //  FIX: Check for duplicate using BOTH analysisType AND inputUrl
                TaskResult existingResult = jobContext.getResults().stream()
                    .filter(r -> r.analysisType.equals(analysisType) && 
                                 r.inputUrl.equals(inputUrl))  // ← CHECK BOTH!
                    .findFirst()
                    .orElse(null);
                
                if (existingResult != null) {
                    // Already have a result for this SPECIFIC task (type + URL)
                    
                    if (existingResult.status.equals("success")) {
                        // Already have success - ignore this duplicate
                        System.out.println("[INFO] Ignoring duplicate result for: " + 
                            analysisType + " " + inputUrl + " (already have success)");
                        aws.deleteMessage(workerToManagerQueueUrl, message.receiptHandle());
                        return;
                        
                    } else if (existingResult.status.equals("error") && status.equals("success")) {
                        // Had error, now have success - replace error with success
                        System.out.println("[INFO] Replacing error with success for: " + 
                            analysisType + " " + inputUrl);
                        jobContext.getResults().remove(existingResult);
                        // Continue to add new success result below
                        
                    } else {
                        // Both errors or other combinations - keep first, ignore duplicate
                        System.out.println("[INFO] Ignoring duplicate result for: " + 
                            analysisType + " " + inputUrl);
                        aws.deleteMessage(workerToManagerQueueUrl, message.receiptHandle());
                        return;
                    }
                }
                
                // Add new result (either first time, or replacing error with success)
                TaskResult result = new TaskResult(analysisType, inputUrl, outputS3Key, status, error);
                jobContext.addResult(result);
                System.out.println("[INFO] Job " + localAppId + " progress: " + 
                    jobContext.getCompletedTasks() + "/" + jobContext.getTotalTasks());
                
                // Check if complete
                if (jobContext.isComplete()) {
                    System.out.println("=== JOB COMPLETE: " + localAppId + " ===");
                    generateSummaryFile(localAppId, jobContext);
                    activeJobs.remove(localAppId);
                }
            } 
        } else {
            System.err.println("[WARNING] Result for unknown job: " + localAppId);
        }
        
        // Delete message
        aws.deleteMessage(workerToManagerQueueUrl, message.receiptHandle());
        
    } catch (Exception e) {
        System.err.println("[ERROR] Handling worker result: " + e.getMessage());
        e.printStackTrace();
    }
}
    

    private void generateSummaryFile(String localAppId, JobContext jobContext) {
        System.out.println("[INFO] Generating summary for " + localAppId);
        
        try {
            // Build HTML
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>\n<html>\n<head>\n");
            html.append("<title>Analysis Results - ").append(localAppId).append("</title>\n");
            html.append("<style>\n");
            html.append("body { font-family: Arial, sans-serif; margin: 20px; }\n");
            html.append("h1 { color: #333; }\n");
            html.append("p { margin: 10px 0; }\n");
            html.append("a { color: #0066cc; text-decoration: none; }\n");
            html.append("a:hover { text-decoration: underline; }\n");
            html.append(".error { color: #cc0000; }\n");
            html.append("</style>\n</head>\n<body>\n");
            html.append("<h1>Text Analysis Results</h1>\n");
            html.append("<p>Job ID: ").append(localAppId).append("</p>\n");
            html.append("<p>Total files: ").append(jobContext.getTotalTasks()).append("</p>\n");
            html.append("<hr>\n");
            
            // Add results
            for (TaskResult result : jobContext.getResults()) {
                html.append("<p>");
                html.append("<strong>").append(result.analysisType).append(":</strong> ");
                html.append("<a href=\"").append(result.inputUrl).append("\" target=\"_blank\">")
                    .append(result.inputUrl).append("</a> ");
                
                if (result.status.equals("success") && !result.outputS3Key.isEmpty()) {
                    // BUG FIX: Always use aws.bucketName for output links
                    String s3Url = "https://" + aws.bucketName + ".s3.amazonaws.com/" + result.outputS3Key;
                    html.append("<a href=\"").append(s3Url).append("\" target=\"_blank\">")
                        .append(s3Url).append("</a>");
                } else {
                    html.append("<span class=\"error\">");
                    html.append(result.error.isEmpty() ? "Processing failed" : result.error);
                    html.append("</span>");
                }
                
                html.append("</p>\n");
            }
            
            html.append("</body>\n</html>");
            
            // Upload to S3 (always to aws.bucketName)
            String summaryKey = "summaries/" + localAppId + "_summary.html";
            aws.uploadStringToS3(summaryKey, html.toString());
            
            // BUG FIX: Send full S3 URL using aws.bucketName (where we actually uploaded)
            String fullS3Url = "s3://" + aws.bucketName + "/" + summaryKey;
            String doneMessage = "done\t" + fullS3Url + "\t" + localAppId;
            
            // FIXED: Send to separate response queue
            aws.sendMessage(managerToLocalQueueUrl, doneMessage);
            
            System.out.println("[INFO] Summary complete for " + localAppId);
            System.out.println("[INFO] Sent done message: " + doneMessage);
            System.out.println("[INFO] Local app should read from: ManagerToLocalQueue");
            
        } catch (Exception e) {
            System.err.println("[ERROR] Generating summary: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void handleTerminationMessage() {
        System.out.println("=== TERMINATION REQUESTED ===");
        shouldTerminate.set(true);
    }
    
    private void shutdown() {
    System.out.println("=== Shutting Down ===");
    localAppProcessor.shutdown();
      try {
            if (!localAppProcessor.awaitTermination(60, TimeUnit.SECONDS)) {
                localAppProcessor.shutdownNow();
            }
        } catch (InterruptedException e) {
            localAppProcessor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    // Wait for active jobs to finish (defensive — in case we still have some)
    while (!activeJobs.isEmpty()) {
        System.out.println("[INFO] Waiting for " + activeJobs.size() + " jobs...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
        }
    }
    
    // Terminate workers
    System.out.println("[INFO] Terminating workers...");
    aws.terminateAllWorkers();
    aws.killManager();
    
    

    
    // No executorService anymore, so nothing to shut down here
    
    System.out.println("=== Manager Terminated ===");
}

    
    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.run();
    }
}

/**
 * Job Context - tracks a single job
 */
class JobContext {
    private final String localAppId;
    private int totalTasks;
    private final List<TaskResult> results = Collections.synchronizedList(new ArrayList<>());
    
    public JobContext(String localAppId) {
        this.localAppId = localAppId;
    }
    
    public void setTotalTasks(int totalTasks) {
        this.totalTasks = totalTasks;
    }
    
    public void addResult(TaskResult result) {
        results.add(result);
    }
    public void removeResult(TaskResult result) {
    results.remove(result);
    }
    
    public boolean isComplete() {
        return results.size() >= totalTasks;
    }
    
    public int getTotalTasks() {
        return totalTasks;
    }
    
    public int getCompletedTasks() {
        return results.size();
    }
    
    public List<TaskResult> getResults() {
        return new ArrayList<>(results);
    }
}


// Task Result - single analysis result
 
class TaskResult {
    final String analysisType;
    final String inputUrl;
    final String outputS3Key;
    final String status;
    final String error;
    
    public TaskResult(String analysisType, String inputUrl, String outputS3Key,
                     String status, String error) {
        this.analysisType = analysisType;
        this.inputUrl = inputUrl;
        this.outputS3Key = outputS3Key;
        this.status = status;
        this.error = error;
    }
}