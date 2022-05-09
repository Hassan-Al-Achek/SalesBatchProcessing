package emse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.File;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class ClientApp {
	
	public static void uploadFileS3Bucket(S3Client s3, String bucketName,String PATH) {
		  try
	        {
	            RandomAccessFile aFile = new RandomAccessFile(PATH,"r");
	 
	            FileChannel inChannel = aFile.getChannel();
	            long fileSize = inChannel.size();
	 
	            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
	            inChannel.read(buffer);
	            buffer.flip();
	            
	          
	    	    PutObjectRequest objectRequest = PutObjectRequest.builder()
	                    .bucket(bucketName)
	                    .key(PATH.substring(PATH.lastIndexOf("\\")+1))
	                    .build();
	    	    
	            s3.putObject(objectRequest, RequestBody.fromByteBuffer(buffer));
	    	    
	           
	            inChannel.close();
	            aFile.close();
	            
	            System.out.printf("%s file uploaded to Bucket %s \n", PATH, bucketName);
	            
	        } catch (IOException exc){
	            System.out.println(exc);
	            System.exit(1);
	        }
	}
	
	public static String getQueueURL(SqsClient sqsClient, String queueName) {
		
		try {
			
			GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
	}
	
	public  static void sendMessage(SqsClient sqsClient, String queueName, String fileName, String bucketName) {
		
		String queueUrl = getQueueURL(sqsClient, queueName);
		
        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody(bucketName + "," + fileName.substring(fileName.lastIndexOf("\\")+1)).build(),
                		SendMessageBatchRequestEntry.builder().id("id2").messageBody(bucketName + "," + fileName.substring(fileName.lastIndexOf("\\")+1)).build())
                .build();
        
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
            
            System.out.printf("Message added to queue %s \n", queueName);
	}
	
	
	public static List<Message> getMessage(SqsClient sqsClient, String queueName) {
		
		try {
			   String queueUrl = getQueueURL(sqsClient, queueName);
			   
			   ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
		                .queueUrl(queueUrl)
		                .maxNumberOfMessages(5)
		                .build();
	            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
	            return messages;
	            
	        } catch (SqsException e) {
	            System.err.println(e.awsErrorDetails().errorMessage());
	            System.exit(1);
	        }
		
	        return null;
	}
	
	
	public static void deleteMessages(SqsClient sqsClient, String queueName) {
        
		String queueUrl = getQueueURL(sqsClient, queueName);
    	List <Message> messages = getMessage(sqsClient, queueName);
    	
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
        
        System.out.println("Message Deleted");
	}
	
	
	public static BufferedReader getFileS3Bucket(S3Client s3Client, String bucketName, String fileName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        
        ResponseInputStream<GetObjectResponse> s3objectResponse = s3Client.getObject(getObjectRequest);
        
        BufferedReader buffReader = new BufferedReader(new InputStreamReader(s3objectResponse));
        
        
        return buffReader;
        
	}
	
	public static void main(String[] args) {
		
		String PATH = "src\\emse\\sales-2021-01-02.csv";

		String bucketName = "newbucket1337110";
		String queueName = "Inbox";
//		String PATH = args[1];
		List<String> names = new ArrayList<String>();
		
		
		Region region = Region.US_EAST_2;
	    S3Client s3Client = S3Client.builder().region(region).build();
	    SqsClient sqsClient = SqsClient.builder().region(region).build();
		
	   
	    uploadFileS3Bucket(s3Client, bucketName, PATH);
	    sendMessage(sqsClient, queueName, PATH, bucketName);
	    
	    while (true) {
	    	try {
				Thread.sleep(60000);
				
				if(!getMessage(sqsClient,"Outbox").isEmpty()) {
					Thread.sleep(60000);
					List <Message> messages = getMessage(sqsClient,"Outbox");
					
					Thread.sleep(60000);
					deleteMessages(sqsClient,"Outbox");
					
					for(Message message : messages) {
						names.add(message.body().toString());
					}
					
					break;
				}
				
			} catch (InterruptedException e) {
				System.out.println("Thread probelm");
				e.printStackTrace();
			}
	    }
	    
	    	
	    	BufferedReader resultFileBuffer = getFileS3Bucket(s3Client, bucketName, names.get(0).split(",", 2)[0]);
	    	
	    	File resultFile = new File("src\\emse\\" + names.get(0).split(",", 2)[0]);
	    	
	    	
			try {
					FileWriter localFileWriter;
					localFileWriter = new FileWriter(resultFile);
		    		BufferedWriter localBufferWriter = new BufferedWriter(localFileWriter);
		    		
		    		String tempStr;
		    		while((tempStr = resultFileBuffer.readLine()) != null) {
			    			localBufferWriter.write(tempStr + "\n");
		    			}
	    		
		    		localBufferWriter.close();
		    		resultFileBuffer.close();
		    		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	    		
	    	System.out.println("[+] Done Result Saved");
	}

}
