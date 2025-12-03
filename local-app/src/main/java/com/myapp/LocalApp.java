package com.myapp;

import software.amazon.awssdk.services.sqs.model.Message;
import java.io.File;
import java.util.List;


public class LocalApp {
    
    private final AWS aws;
    
    
    private static final String LOCAL_TO_MANAGER_QUEUE = "LocalToManagerQueue";
    private static final String MANAGER_TO_LOCAL_QUEUE = "ManagerToLocalQueue";
    
    
    private final String inputFileName;
    private final String outputFileName;
    private final int n;
    private final boolean terminate;
    
    
    private final String jobId;
    
    public LocalApp(String[] args) {
        
        if (args.length < 3) {
            System.err.println("Usage: java -jar app.jar <inputFile> <outputFile> <n> [terminate]");
            System.exit(1);
        }
        
        this.inputFileName = args[0];
        this.outputFileName = args[1];
        this.n = Integer.parseInt(args[2]);
        this.terminate = (args.length > 3 && args[3].equalsIgnoreCase("terminate"));
        
        // Generate unique job ID
        this.jobId = "job-" + System.currentTimeMillis();
        
        // Get AWS instance
        this.aws = AWS.getInstance();
        
        System.out.println("=== LocalApp Starting ===");
        System.out.println("[INFO] Input file: " + inputFileName);
        System.out.println("[INFO] Output file: " + outputFileName);
        System.out.println("[INFO] Files per worker (n): " + n);
        System.out.println("[INFO] Terminate: " + terminate);
        System.out.println("[INFO] Job ID: " + jobId);
    }
    
    public void run() {
        try {
            // Step 1: Ensure bucket exists
            System.out.println("\n=== Step 1: Initialize S3 Bucket ===");
            aws.createBucketIfNotExists(aws.bucketName);
            
            // Step 2: Ensure Manager is running
            System.out.println("\n=== Step 2: Check Manager ===");
            ensureManagerRunning();
            
            // Step 3: Upload input file to S3
            System.out.println("\n=== Step 3: Upload Input File ===");
            String s3Key = uploadInputFile();
            String s3Location = "s3://" + aws.bucketName + "/" + s3Key;
            System.out.println("[INFO] Uploaded to: " + s3Location);
            
            // Step 4: Send task to Manager
            System.out.println("\n=== Step 4: Send Task to Manager ===");
            sendTaskToManager(s3Location);
            
            // Step 5: Wait for result
            System.out.println("\n=== Step 5: Wait for Results ===");
            String summaryS3Url = waitForResult();
            
            // Step 6: Download summary
            System.out.println("\n=== Step 6: Download Summary ===");
            downloadSummary(summaryS3Url);
            
            // Step 7: Send terminate if requested
            if (terminate) {
                System.out.println("\n=== Step 7: Send Termination ===");
                sendTerminate();
            }
            
            System.out.println("\n=== LocalApp Complete ===");
            System.out.println("[SUCCESS] Output saved to: " + outputFileName);
            
        } catch (Exception e) {
            System.err.println("\n[ERROR] LocalApp failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Ensure Manager is running, start if needed
     */
    private void ensureManagerRunning() {
        if (!aws.isManagerRunning()) {
            try{
            String script = generateUserDataScript();
            aws.createEC2(script, "Manager", 1);
            System.out.println("[INFO] Manager started");
            // Wait for manager to initialize
            System.out.println("[INFO] Waiting 30 seconds for Manager to start...");
            Thread.sleep(30000);            
            
        }
        catch (InterruptedException e) {
            System.err.println("[ERROR] Interrupted while waiting for Manager");
            Thread.currentThread().interrupt();
            System.exit(1);

        }
    } else {
        System.out.println("[INFO] Manager already running");
    }
}

private static String generateUserDataScript() {
    String dataBucket = AWS.getInstance().bucketName;
    String jarFilesBucket = "ahd.jars";

    return "#!/bin/bash\n" +
           "# Manager uses LabInstanceProfile IAM role - no credentials needed\n" +
           "cd ~\n" +

           "# Sanity marker\n" +
           "echo 'userdata-ok' > userdata_marker.txt\n" +
           "aws s3 cp userdata_marker.txt s3://" + dataBucket + "/userdata_marker.txt\n" +

           "# Download Manager ZIP (password-protected)\n" +
           "aws s3 cp s3://" + jarFilesBucket + "/Manager-1.0-SNAPSHOT.zip .\n" +
           "unzip -P Ahd Manager-1.0-SNAPSHOT.zip\n" +

           "# Start Manager\n" +
           "nohup java -cp manager-1.0-SNAPSHOT.jar com.myapp.Manager > manager.log 2>&1 &\n" +

           "# Periodic log snapshots\n" +
           "(\n" +
           "  while true; do\n" +
           "    sleep 120\n" +
           "    SNAPSHOT=\"manager-$(date +%Y%m%d-%H%M%S).log\"\n" +
           "    cp manager.log \"$SNAPSHOT\"\n" +
           "    aws s3 cp \"$SNAPSHOT\" s3://" + dataBucket + "/manager_logs/\"$SNAPSHOT\" || true\n" +
           "  done\n" +
           ") &\n";
}
    
    /**
     * Upload input file to S3
     */
    private String uploadInputFile() throws Exception {
        File inputFile = new File(inputFileName);
        
        if (!inputFile.exists()) {
            throw new Exception("Input file not found: " + inputFileName);
        }
        
        // Create unique key for this job's input
        String s3Key = "jobs/" + jobId + "/input.txt";
        
        System.out.println("[INFO] Uploading " + inputFile.getAbsolutePath() + " to S3...");
        
        
        aws.uploadFileToS3(inputFile.getAbsolutePath(), s3Key);
        System.out.println("[INFO] Uploaded as: " + s3Key);
        
        return s3Key;
    }
    
    
    private void sendTaskToManager(String s3Location) {
        // Get or create queue
        String localToManagerQueueUrl = aws.getOrCreateQueue(LOCAL_TO_MANAGER_QUEUE);
        
        // Build message in Manager's expected format
        String message = String.format("new_task\t%s\t%s\t%d", 
            s3Location, jobId, n);
        
        System.out.println("[INFO] Sending message: " + message);
        System.out.println("[INFO] To queue: " + LOCAL_TO_MANAGER_QUEUE);
        
        aws.sendMessage(localToManagerQueueUrl, message);
        System.out.println("[SUCCESS] Task sent to Manager");
    }
    
    
    private String waitForResult() {
        // Get or create response queue
        String managerToLocalQueueUrl = aws.getOrCreateQueue(MANAGER_TO_LOCAL_QUEUE);
        
        System.out.println("[INFO] Polling queue: " + MANAGER_TO_LOCAL_QUEUE);
        System.out.println("[INFO] Waiting for job: " + jobId);
        
        
        while (true) {
            List<Message> messages = aws.receiveMessages(managerToLocalQueueUrl, 10, 5);
            
            for (Message message : messages) {
                String body = message.body();
                System.out.println("[DEBUG] Received message: " + body);
                
                // Check if this is a "done" message
                if (!body.startsWith("done\t")) {
                    System.out.println("[DEBUG] Not a done message, ignoring");
                    continue;
                }
                
                // Parse: "done\t<s3_url>\t<local_app_id>"
                String[] parts = body.split("\t");
                if (parts.length != 3) {
                    System.err.println("[WARNING] Invalid done message format: " + body);
                    continue;
                }
                
                String messageType = parts[0];
                String summaryS3Url = parts[1];
                String messageJobId = parts[2];
                
                // Check if this message is for our job
                if (!messageJobId.equals(jobId)) {
                    System.out.println("[DEBUG] Message for different job (" + messageJobId + "), ignoring");
                    continue;
                }
                
                // This is our result!
                System.out.println("[SUCCESS] Received result for job: " + jobId);
                System.out.println("[INFO] Summary location: " + summaryS3Url);
                
                // Delete the message
                aws.deleteMessage(managerToLocalQueueUrl, message.receiptHandle());
                
                return summaryS3Url;
            }
            
        }
        
    }
    
    
    private void downloadSummary(String s3Url) throws Exception {
        System.out.println("[INFO] Downloading summary from: " + s3Url);
        
        // Parse S3 URL: s3://bucket/key
        if (!s3Url.startsWith("s3://")) {
            throw new Exception("Invalid S3 URL: " + s3Url);
        }
        
        String[] parts = s3Url.substring(5).split("/", 2);
        if (parts.length != 2) {
            throw new Exception("Invalid S3 URL format: " + s3Url);
        }
        
        String bucket = parts[0];
        String key = parts[1];
        
        System.out.println("[INFO] Bucket: " + bucket);
        System.out.println("[INFO] Key: " + key);
        
        // Download summary file
        File outputFile = new File(outputFileName);
        aws.downloadFileFromS3(key, outputFile);
        
        System.out.println("[SUCCESS] Summary downloaded to: " + outputFile.getAbsolutePath());
    }
    
    private void sendTerminate() {
        String localToManagerQueueUrl = aws.getOrCreateQueue(LOCAL_TO_MANAGER_QUEUE);
        
        System.out.println("[INFO] Sending terminate message to Manager");
        
        aws.sendMessage(localToManagerQueueUrl, "terminate");
        
        System.out.println("[SUCCESS] Terminate message sent");
    }
    
    public static void main(String[] args) {
        LocalApp app = new LocalApp(args);
        app.run();
    }
}

