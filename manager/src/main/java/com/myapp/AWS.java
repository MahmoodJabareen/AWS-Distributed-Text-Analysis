package com.myapp;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import java.io.*;
import java.util.*;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    public static String ami = "ami-00e95a9222311e8ed";
    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;
    private static final AWS instance = new AWS();

   // public enum ec2Type{Manager , Worker} 

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

public String bucketName = "mahmood-ttest-05850805111213569";


    //--- EC2 ---
    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(numberOfInstances)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<String> instanceIds = new ArrayList<>();
        for (Instance instance : response.instances()) {
        instanceIds.add(instance.instanceId());
        }
        Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceIds)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceIds.size(), tagName);
           for (String id : instanceIds) {
             System.out.println("[INFO] - Instance: " + id);
          }
        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceIds.get(0);
    }
    public boolean isInstanceRunning(String InstanceId){
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().
            instanceIds(InstanceId).
            build();
        
        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest);
        for(Reservation reservation :response.reservations() ){
            for(Instance instance : reservation.instances()){
                if (instance.state().name() == InstanceStateName.RUNNING || instance.state().name() == InstanceStateName.PENDING)
                    return true;
            }
        }
        return false;
    }
    public List<Instance> getAllInstances(){
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build() ; 
        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest) ;

        List<Instance> res = new ArrayList<>();
        for(Reservation reservation : response.reservations()){
            res.addAll((reservation.instances()));
        }
        return res ;
    }
    public List<Instance> getInstancesByTag(String tagValue){
        DescribeInstancesResponse response = ec2.describeInstances();
        List<Instance> res = new ArrayList<>();

        for(Reservation reservation : response.reservations()){
            for(Instance instance : reservation.instances()){
                   // Filter for RUNNING or PENDING only
            InstanceStateName state = instance.state().name();
            if (state.equals(InstanceStateName.RUNNING) || 
                state.equals(InstanceStateName.PENDING)) {
                List<Tag> tags = instance.tags();
                
                // Check if tags exist (null check)
                if(tags != null){
                    for(Tag tag : tags){
                        if("Name".equals(tag.key()) && tagValue.equals(tag.value())){
                            res.add(instance);
                            break; // Exit inner loop to avoid duplicates
                        }
                      }
                    }
                }
            }
        }
        return res;
    }
    public boolean isManagerRunning() {
    try {
        // Get all instances with tag "Manager"
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
            .filters(
                Filter.builder()
                    .name("tag:Name")
                    .values("Manager")
                    .build(),
                Filter.builder()
                    .name("instance-state-name")
                    .values("running", "pending")  // Include BOTH running and pending!
                    .build()
            )
            .build();
        System.out.println("HERERERERERE11");
        
        DescribeInstancesResponse response = ec2.describeInstances(request);
        
        // Check if any Manager instances exist
        for (Reservation reservation : response.reservations()) {
            System.out.println("THERERE2222");
            if (!reservation.instances().isEmpty()) {
                System.out.println("[INFO] Manager instance found: " + 
                    reservation.instances().get(0).instanceId());
                return true;
            }
        }
        
        System.out.println("[INFO] No Manager instance found");
        return false;
        
    } catch (Exception e) {
        System.err.println("[ERROR] Failed to check Manager status: " + e.getMessage());
        return false;  // Assume not running if check fails
    }
}
public boolean isInstanceRunningFromAmi(String ami) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("image-id")
                                .values(ami)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running")
                                .build()
                )
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request);
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .anyMatch(instance -> ami.equals(instance.imageId()));
    }
    
    //Terminate a single instance
    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        ec2.terminateInstances(request);
        System.out.println("[INFO] Terminated instance: " + instanceId);
    }
    //Terminate multiple instances
    public void terminateInstances(List<String> instanceIds) {
        if (instanceIds.isEmpty()) {
            return;
        }
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceIds)
                .build();
        ec2.terminateInstances(request);
        System.out.println("[INFO] Terminated " + instanceIds.size() + " instances");
    }
    //Get running worker count
    public int getRunningWorkerCount() {
        List<Instance> workers = getInstancesByTag("Worker");
        int count = 0;
        for (Instance instance : workers) {
            InstanceStateName state = instance.state().name();
            if (state.equals(InstanceStateName.RUNNING) || state.equals(InstanceStateName.PENDING)) {
                count++;
            }
        }
        return count;
    }
    //Terminate all workers
    public void terminateAllWorkers() {
        List<Instance> workers = getInstancesByTag("Worker");
        List<String> instanceIds = new ArrayList<>();
        
        for (Instance instance : workers) {
            InstanceStateName state = instance.state().name();
            if (state.equals(InstanceStateName.RUNNING) || state.equals(InstanceStateName.PENDING)) {
                instanceIds.add(instance.instanceId());
            }
        }
        
        if (!instanceIds.isEmpty()) {
            terminateInstances(instanceIds);
            System.out.println("[INFO] Terminated all workers");
        }
    }
    public void killManager() {
        List<Instance> manager = getInstancesByTag("Manager");
        if (manager.isEmpty()) {
             System.err.println("[ERROR] No Manager found to terminate!");
            return;
        }
        List<String> maninstanceIds = new ArrayList<>();
        InstanceStateName state = manager.get(0).state().name();
        if (state.equals(InstanceStateName.RUNNING) || state.equals(InstanceStateName.PENDING)) {
            maninstanceIds.add(manager.get(0).instanceId());
        }
        
        
        if (!maninstanceIds.isEmpty()) {
            terminateInstances(maninstanceIds);
            System.out.println("[INFO] Terminated manager");
        }
    }

    //---- S3 ----
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    public String downloadStringFromS3(String key) throws IOException {
        return downloadStringFromS3(bucketName, key);
    }

    public String downloadStringFromS3(String bucket, String key) throws IOException {
        System.out.println("[INFO] Downloading from S3: bucket=" + bucket + ", key=" + key);
        
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        
        byte[] data = s3.getObjectAsBytes(request).asByteArray();
        return new String(data, "UTF-8");
    }
    public void downloadFileFromS3(String key, File destination) {
    File parent = destination.getParentFile();
    if (parent != null && !parent.exists()) {
        parent.mkdirs();
    }
    
    s3.getObject(
        GetObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .build(),
        ResponseTransformer.toFile(destination));
}
    //added Upload string content to S3
    public void uploadStringToS3(String key, String content) {
        System.out.println("[INFO] Uploading to S3: " + key);
        
        PutObjectRequest.Builder builder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType("text/html");
        
        
        s3.putObject(builder.build(), RequestBody.fromString(content));
    }
    //added Upload file to S3
    public void uploadFileToS3(String filePath , String s3Key) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + filePath);
        }   
        String fileName = file.getName();

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3.putObject(request, RequestBody.fromFile(file));
        System.out.println("[INFO] Uploaded to S3: " + fileName);
    }
    

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }
        
    //adde Get queue URL
    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(request).queueUrl();
    }

    
    //added Check if queue exists
    public boolean isQueueExist(String queueName) {
        try {
            getQueueUrl(queueName);
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        }
    }
    //added  Get or create queue
    public String getOrCreateQueue(String queueName) {
        if (isQueueExist(queueName)) {
            System.out.println("QUEUE EXISTS");
            return getQueueUrl(queueName);
        } else {
            System.out.println("QUEUE DOESN'T EXIST");
            createSqsQueue(queueName);
            return getQueueUrl(queueName);
        }
    }
    //added  Send message to queue
    public void sendMessage(String queueUrl, String messageBody) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();
        sqs.sendMessage(request);
    }
    //added  Receive messages from queue
    public List<Message> receiveMessages(String queueUrl, int maxMessages, int waitTimeSeconds) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .visibilityTimeout(1000)
                .build();
        return sqs.receiveMessage(request).messages();
    }
    //added Delete message from queue  
    public void deleteMessage(String queueUrl, String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        sqs.deleteMessage(request);
    }
    //added Get queue size
    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse response = sqs.getQueueAttributes(request);
        Map<QueueAttributeName, String> attributes = response.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
               Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
               Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }
    //added Delete queue
    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest request = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(request);
        System.out.println("[INFO] Deleted queue: " + queueUrl);
    }

}
