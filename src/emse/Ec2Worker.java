package emse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Ec2Worker {
	
	public static String getQueueURL(SqsClient sqsClient, String queueName) {
		
		try {
			
			GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
        	 return "";
        }
       
	}
	
	public static boolean createQueue(SqsClient sqsClient, String queueName) {
		if (getQueueURL(sqsClient, queueName) == "") {
	        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
	                .queueName(queueName)
	                .build();
	
	            sqsClient.createQueue(createQueueRequest);
	            
	            System.out.printf("Queue %s created\n", queueName);
	            return true;
		}
		else {
			return false;
		}
	}
	
public static List <Message> getMessage(SqsClient sqsClient, String queueName) {
		
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

public static List<String> calculationPerProdCountry(List<List<String>> records, String productName, String countryName) {
	
	int productColumnIndex = records.get(0).indexOf("Product");
	int countryColumnIndex = records.get(0).indexOf("Country");
	int soldColumnIndex  = records.get(0).indexOf("Price");
	float soldAmount= 0;
	float averageSold = 0;
	int productNumberCount = 0;
	List<String> toReturn = new ArrayList<>();
	
	
	for (List<String> row: records) {
		if((row.get(productColumnIndex).equals(productName)) && (row.get(countryColumnIndex).equals(countryName))) {
			productNumberCount ++;
			soldAmount = soldAmount + Float.parseFloat(row.get(soldColumnIndex));
		}
	}
	averageSold = soldAmount/productNumberCount;
	
	// In case the product does not exist for the country
	if ((productNumberCount == 0) && (soldAmount == 0)) { return null;} 
	
	toReturn.add(String.valueOf(productNumberCount));
	toReturn.add(String.valueOf(soldAmount));
	toReturn.add(String.valueOf(averageSold));
	
	return toReturn;
}


/* This function will be used to create a list that contains all countries names or to create
 * A list that contains all products names
*/
public static List<String> createList(List<List<String>> records, String indicator){
    List<String> toReturn = new ArrayList<>();
    int indexOfIndicatorColumn = records.get(0).indexOf(indicator);
    
	for(List<String> row: records) {
		if (!toReturn.contains(row.get(indexOfIndicatorColumn))) {
			toReturn.add(row.get(indexOfIndicatorColumn));
		}
	}
	return toReturn;
}

public static String calculation(BufferedReader buff, String SalesFileName){
	
	List<List<String>> records = new ArrayList<>();
    List<String> countries = new ArrayList<>();
    List<String> products = new ArrayList<>();    
	String PATH = "src\\emse\\" + "Result-" +SalesFileName;
    String line;
    File resultFile = new File(PATH);
    
    try {
		while ((line = buff.readLine()) != null) {
		    String[] values = line.split(",", 11);
		    records.add(Arrays.asList(values));
		}
		

		countries = createList(records, "Country");
		countries = countries.subList(1, countries.size()-1);
		products = createList(records, "Product");
		products = products.subList(1, products.size()-1);
		
		
		FileWriter resultFileWriter = new FileWriter(resultFile);
		List<String> TempList;
		resultFileWriter.write("Product, Country, Total Number of Sales, Total Amount Sold, Average Sold\n");
		for(String product: products) {
			for (String country: countries){
				
				TempList = calculationPerProdCountry(records, product, country);
				if(TempList != null) {
					resultFileWriter.write(product + ", " + country + ",");
				}else {
					continue;
				}
				
				for (int index = 0; index < TempList.size(); index++) {
					if(index == TempList.size()-1) {
						resultFileWriter.write(TempList.get(index) + "\n");
					}
					else {
						resultFileWriter.write(TempList.get(index) + ",");
					}
				}
			}
		}
		
		resultFileWriter.close();
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	return PATH;
}

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
          
          System.out.printf("%s file uploaded to Bucket %s ", PATH, bucketName);
          
      } catch (IOException exc){
          System.out.println(exc);
          System.exit(1);
      }
}


public  static void sendMessage(SqsClient sqsClient, String queueName, String InfileName, String outFileName) {
	
	String queueUrl = getQueueURL(sqsClient, queueName);
	
    SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody(outFileName + "," + InfileName).build(),
            		SendMessageBatchRequestEntry.builder().id("id2").messageBody(outFileName + "," + InfileName).build())
            .build();
    
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
        
        System.out.printf("Message added to queue %s", queueName);
        System.out.println();
}

	
public static void main(String[] args) {
		
		Region region = Region.US_EAST_2;
		BufferedReader bufferReader;
		String S3BucketName = "newbucket1337110";
		List <String> names  = new ArrayList<String>();
		
		
		
		S3Client s3Client = S3Client.builder().region(region).build();
		SqsClient sqsClient = SqsClient.builder().region(region).build();
		
		if(!createQueue(sqsClient, "Inbox")) {
			System.out.println("Already Created Inbox");
		}
		
		if(!createQueue(sqsClient, "Outbox")) {
			System.out.println("Already Created outbox");
		}

		while(true) {
			try {
				Thread.sleep(60000);
				if(!getMessage(sqsClient,"Inbox").isEmpty()) {
					/*Sometime the message inside the queue will be on flight mode which prevent the code from work properly*/
					/*That's why i add  a sleep before each message read or delete from the queue to avoid confusion*/
					Thread.sleep(60000);
					List <Message> messages = getMessage(sqsClient,"Inbox");
					
					/*Delete Inbox Messages*/
					Thread.sleep(60000);
					deleteMessages(sqsClient,"Inbox");
					
					for(Message message : messages) {
						names.add(message.body().toString());
					}
					
					names.forEach(it->System.out.println(it));
					
					bufferReader = getFileS3Bucket(s3Client, names.get(0).split(",", 2)[0], names.get(0).split(",", 2)[1]);
					
					
					String PATH = calculation(bufferReader, names.get(0).split(",", 2)[1]);
					uploadFileS3Bucket(s3Client, S3BucketName, PATH);
					
					// For test only 
					System.out.println(names.get(0).split(",", 2)[1]);
					
					
					sendMessage(sqsClient, "Outbox", names.get(0).split(",", 2)[1], PATH.substring(PATH.lastIndexOf("\\")+1));
					
				}
				else {
					System.out.println("[+] Waiting");
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
}


// Save the result for each iteration like staticName-date
// who create the S3 bucket? client or worker


/*
newbucket1337110
sales-2021-01-02.csv
 */


//AWS client configure