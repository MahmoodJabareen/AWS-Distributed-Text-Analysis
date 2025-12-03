package com.myapp;

import software.amazon.awssdk.services.sqs.model.Message;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;
import java.io.BufferedReader;
import java.io.StringReader;
import edu.stanford.nlp.ling.Word;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Worker - Processes text analysis tasks
 * 
 * Implements POS tagging, Constituency parsing, and Dependency parsing
 * using Stanford NLP
 */
public class Worker {
    
    private final AWS aws;
    
    // Queue URLs
    private final String managerToWorkerQueueUrl;
    private final String workerToManagerQueueUrl;
    
    // Stanford NLP components (loaded once for efficiency)
    private MaxentTagger posTagger;
    private LexicalizedParser parser;
    
    public Worker() {
        System.out.println("=== Initializing Worker ===");
        // Get AWS instance
        this.aws = AWS.getInstance();
        // Get queue URLs
        this.managerToWorkerQueueUrl = aws.getQueueUrl("ManagerToWorkerQueue");
        this.workerToManagerQueueUrl = aws.getQueueUrl("WorkerToManagerQueue");
        // Initialize Stanford NLP models
        System.out.println("[INFO] Loading Stanford NLP models..");
        initializeNLPModels();
        System.out.println("[INFO] Worker initialized successfully");
    }
    
    
    // Initialize Stanford NLP models 
    private void initializeNLPModels() {
        try {
            // Load POS tagger model
            System.out.println("[INFO] Loading POS tagger...");
            posTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
            
            // Load parser model
            System.out.println("[INFO] Loading parser...");
            parser = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
            
            System.out.println("[INFO] NLP models loaded successfully!");
            
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to load NLP models: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void run() {
        System.out.println("=== Worker Started ===");
        System.out.println("[INFO] Polling queue: " + managerToWorkerQueueUrl);
        
        while (true) {
            try {
                // Poll for messages (long polling - 20 seconds)
                List<Message> messages = aws.receiveMessages(managerToWorkerQueueUrl, 1, 20);
                
                if (messages.isEmpty()) {
                    System.out.println("[DEBUG] No messages, continuing to poll...");
                    continue;
                }
                // Process each message
                for (Message message : messages) {
                    processMessage(message);
                }
                
            } catch (Exception e) {
                System.err.println("[ERROR] Worker loop error: " + e.getMessage());
                e.printStackTrace();
                // Don't crash - continue processing
                try {
                    Thread.sleep(5000); // Wait 5 seconds before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    
    // Process a single message from the manager
     
    private void processMessage(Message message) {
        String messageBody = message.body();
        System.out.println("=== Processing Message ===");
        System.out.println("[INFO] Message: " + messageBody);
        
        String analysisType = null;
        String url = null;
        String localAppId = null;
        
        try {
            // STEP 2: Parse message
            // Expected format: "ANALYSIS_TYPE\tURL\tLOCAL_APP_ID"
            String[] parts = messageBody.split("\t");
            
            if (parts.length != 3) {
              System.err.println("[ERROR] Invalid message format");
              aws.deleteMessage(managerToWorkerQueueUrl, message.receiptHandle());
              return; // Can't even send error - don't know localAppId
      }
            
            analysisType = parts[0].trim();
            url = parts[1].trim();
            localAppId = parts[2].trim();
            
            System.out.println("[INFO] Analysis Type: " + analysisType);
            System.out.println("[INFO] URL: " + url);
            System.out.println("[INFO] Job ID: " + localAppId);
            
            // Validate analysis type
            if (!analysisType.equals("POS") && 
                !analysisType.equals("CONSTITUENCY") && 
                !analysisType.equals("DEPENDENCY")) {
                throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
            }
            
            // STEP 3: Download text file from URL
            System.out.println("[INFO] Downloading text from URL...");
            String textContent = downloadTextFromUrl(url);
            System.out.println("[INFO] Downloaded " + textContent.length() + " characters");
            
            // STEP 4: Perform analysis
            System.out.println("[INFO] Performing " + analysisType + " analysis...");
            String analysisResult = performAnalysis(analysisType, textContent);
            System.out.println("[INFO] Analysis complete, result length: " + analysisResult.length());
            
            // STEP 5: Upload result to S3
            String outputKey = "outputs/" + localAppId + "_" + UUID.randomUUID() + ".txt";
            System.out.println("[INFO] Uploading result to S3: " + outputKey);
            uploadResultToS3(outputKey, analysisResult);
            
            // STEP 6: Send success message to manager
            sendSuccessResult(analysisType, url, outputKey, localAppId);
            
            // Delete message from queue (mark as successfully processed)
            aws.deleteMessage(managerToWorkerQueueUrl, message.receiptHandle());
            System.out.println("[INFO] Message processed successfully");
            
        }catch (Exception e) {
        // ANY exception = send error to manager and mark as processed
        System.err.println("[ERROR] Processing failed: " + e.getMessage());
        e.printStackTrace();
        
        // Send error message to manager
        if (localAppId != null) {
            sendErrorResult(analysisType, url, localAppId, "Error: " + e.getMessage());
        } else {
            System.err.println("[ERROR] Cannot send error - localAppId unknown");
        }
        
        // Delete message so it won't be retried forever
        aws.deleteMessage(managerToWorkerQueueUrl, message.receiptHandle());
        System.out.println("[INFO] Message marked as failed and deleted");
    }
     
}
    
    // Send error result to manager
    private void sendErrorResult(String analysisType, String url, String localAppId, String errorMessage) {
        try {
            if (analysisType == null) analysisType = "UNKNOWN";
            if (url == null) url = "UNKNOWN";
            if (localAppId == null) localAppId = "UNKNOWN";
            if (errorMessage == null) errorMessage = "Unknown error";
            
            // Format: "ANALYSIS_TYPE\tINPUT_URL\tOUTPUT_S3_KEY\tLOCAL_APP_ID\tSTATUS\tERROR"
            String resultMessage = String.format("%s\t%s\t\t%s\terror\t%s",
                analysisType, url, localAppId, errorMessage);
            
            aws.sendMessage(workerToManagerQueueUrl, resultMessage);
            System.out.println("[INFO] Sent error result to manager");
            
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send error result: " + e.getMessage());
        }
    }
    
    
    //Download text content from URL
    private String downloadTextFromUrl(String urlString) throws Exception {
        System.out.println("[INFO] Connecting to: " + urlString);
        
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(30000); // 30 seconds
        connection.setReadTimeout(30000);    // 30 seconds
        
        int responseCode = connection.getResponseCode();
        System.out.println("[INFO] HTTP Response Code: " + responseCode);
        
        if (responseCode != 200) {
            throw new Exception("HTTP Error " + responseCode + ": " + connection.getResponseMessage());
        }
        
        // Read the response
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        
        connection.disconnect();
        
        if (content.length() == 0) {
            throw new Exception("Downloaded file is empty");
        }
        
        return content.toString();
    }
    
    
    // Perform Stanford NLP analysis
    
private String performAnalysis(String analysisType, String text) throws Exception {
    switch (analysisType) {
        case "POS":
            return performPOSTagging(text);
        case "CONSTITUENCY":
            return performConstituencyParsing(text);
        case "DEPENDENCY":
            return performDependencyParsing(text);
        default:
            throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
    }
}
    
    
    // Tags each word with part of speech
private String performPOSTagging(String text) throws Exception {
    StringBuilder result = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new StringReader(text))) {
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue; // skip empty lines
            }

            // Tokenize by whitespace for this assignment (good enough)
            List<HasWord> sentence = new ArrayList<>();
            for (String token : line.split("\\s+")) {
                if (!token.isEmpty()) {
                    sentence.add(new Word(token));
                }
            }

            if (sentence.isEmpty()) {
                continue;
            }

            List<TaggedWord> tagged = posTagger.tagSentence(sentence);
            for (TaggedWord word : tagged) {
                result.append(word.word())
                      .append("/")
                      .append(word.tag())
                      .append(" ");
            }
            result.append("\n");
        }
    }

    return result.toString();
}

    
    
    //- Shows hierarchical sentence structure
private String performConstituencyParsing(String text) throws Exception {
    StringBuilder result = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new StringReader(text))) {
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            List<HasWord> sentence = new ArrayList<>();
            for (String token : line.split("\\s+")) {
                if (!token.isEmpty()) {
                    sentence.add(new Word(token));
                }
            }

            if (sentence.isEmpty()) {
                continue;
            }

            Tree parse = parser.apply(sentence);
            result.append(parse.toString()).append("\n");
        }
    }

    return result.toString();
}

    
    
    // Shows word relationships
   private String performDependencyParsing(String text) throws Exception {
    StringBuilder result = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new StringReader(text))) {
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            List<HasWord> sentence = new ArrayList<>();
            for (String token : line.split("\\s+")) {
                if (!token.isEmpty()) {
                    sentence.add(new Word(token));
                }
            }

            if (sentence.isEmpty()) {
                continue;
            }

            Tree parse = parser.apply(sentence);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            TreePrint tp = new TreePrint("typedDependencies");
            tp.printTree(parse, pw);
            pw.flush();

            result.append(sw.toString()).append("\n");
        }
    }

    return result.toString();
}
   
    private void uploadResultToS3(String key, String content) throws Exception {
        aws.uploadStringToS3(key, content);  // true = public read
        System.out.println("[INFO] Uploaded to S3: " + key);
    }
    
   
    private void sendSuccessResult(String analysisType, String url, String outputKey, String localAppId) {
        try {
            // Format: "ANALYSIS_TYPE\tINPUT_URL\tOUTPUT_S3_KEY\tLOCAL_APP_ID\tSTATUS\tERROR"
            String resultMessage = String.format("%s\t%s\t%s\t%s\tsuccess\t",
                analysisType, url, outputKey, localAppId);
            
            aws.sendMessage(workerToManagerQueueUrl, resultMessage);
            System.out.println("[INFO] Sent success result to manager");
            
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send success result: " + e.getMessage());
        }
    }
    
    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.run();
    }
}