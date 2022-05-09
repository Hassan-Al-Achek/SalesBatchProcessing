# Batch Porgram For Sales Processing
# MEMBERS OF THE GROUP :

## Hassan Al Achek
## Farid Bechaalany

## INSTRUCTIONS :

*The EC2worker app will run on a EC2 instance. The steps to run it are:
-connect to the instance.
-upload the .jar file to it.
-run this file on the instance.

*The Client app will be run on the local machine.

WORKER APP :

*Having run the worker app, it check if the 2 queues (Inbox and Outbox) have already been created. If not, they are created.

Code:
if(!createQueue(sqsClient, "Inbox")) {
			System.out.println("Already Created Inbox");
		}
		
		if(!createQueue(sqsClient, "Outbox")) {
			System.out.println("Already Created outbox");
		}
End of code

*Then the worker app enters an infinite loop that will keep on checking for messages from the client.

Code :
while(true) {
			try {
				Thread.sleep(1000);
				if(!getMessage(sqsClient,"Inbox").isEmpty()) {
					
					Thread.sleep(60000);
[1]					List <Message> messages = getMessage(sqsClient,"Inbox");
					
					/*Delete Inbox Messages*/
					Thread.sleep(60000);
[2]					deleteMessages(sqsClient,"Inbox");
					
					for(Message message : messages) {
						names.add(message.body().toString());
					}
					
					names.forEach(it->System.out.println(it));
					
[3]					bufferReader = getFileS3Bucket(s3Client, names.get(0).split(",", 2)[0], names.get(0).split(",", 2)[1]);
					
					
[4]					String PATH = calculation(bufferReader, names.get(0).split(",", 2)[1]);
[5]					uploadFileS3Bucket(s3Client, S3BucketName, PATH);
					
					// For test only 
					System.out.println(names.get(0).split(",", 2)[1]);
					
					
[6]					sendMessage(sqsClient, "Outbox", names.get(0).split(",", 2)[1], PATH.substring(PATH.lastIndexOf("\\")+1));
					
				}
				else {
					System.out.println("[+] Waiting");
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
End of code

functions:

*[1] getMessage retrieves the messages from a specified queue (here "Inbox").

*[2] deleteMessages deletes the retrieved messages.

*The retrieved messages containing the name of the bucket and the file name and are saved in a List of strings "names".

*[3] getFileS3Bucket takes the names of the bucket and the file and retrieves the information from the file.

*[4] calculation makes the necessary calculations and returns the path of the created file.

*[5] uploadFileS3Bucket upload the result file (the PATH given by calculation) to the bucket.

*[6] the worker sends a message via the sendMessage function that takes the name of the queue (here "Outbox"), the input file and the result file.

*In case there are no messages in the Inbox from the client, the worker app just wait.

*The process repeats itself.

CLIENT APP :

*It uploads the hourly sales file (sales-2021-01-02.csv) to the bucket using the same uploadFileS3Bucket function as before.

*It then send a message in the Inbox queue containing the name of the bucket and the name of the file via the sendMessage function (another than before as it take the bucket name as argument).

Code :
uploadFileS3Bucket(s3Client, bucketName, PATH);
sendMessage(sqsClient, queueName, PATH, bucketName);
End of code

*As the worker app, the client app enters an infinite loop waiting a reply from the worker in the Outbox queue.

Code :
while (true) {
	    	try {
				Thread.sleep(1000);
				
				if(!getMessage(sqsClient,"Outbox").isEmpty()) {
					Thread.sleep(60000);
[1]					List <Message> messages = getMessage(sqsClient,"Outbox");
					
					Thread.sleep(60000);
[2]					deleteMessages(sqsClient,"Outbox");
					
					for(Message message : messages) {
						names.add(message.body().toString());
					}
					
[3]					break;
				}
				
			} catch (InterruptedException e) {
				System.out.println("Thread problem");
				e.printStackTrace();
			}
	    }
End of code 

functions :

*[1] This is the same getMessage function as in the worker class.

*[2] This is also the same deleteMessages.

*[3] In case a message is found, the client app retrieves it, deletes it and break out of the loop.

*The content of the message ("output file" and "input file") is saved in the List of strings "names".

*Then, the function getFileS3Bucket retrieves the result file and store it in a buffer.

Code :
BufferedReader resultFileBuffer = getFileS3Bucket(s3Client, bucketName, names.get(0).split(",", 2)[0]);
End of code

*The client creates a file with the same name as the result file and fills it with the content of the buffer (which was copied from the content of the actual file).

Code:
File resultFile = new File("src\\emse\\" + names.get(0).split(",", 2)[0]);
	    	
	    	
			try {
					FileWriter localFileWriter;
					localFileWriter = new FileWriter(resultFile);
		    		BufferedWriter localBufferWriter = new BufferedWriter(localFileWriter);
		    		
		    		String tempStr;
		    		while((tempStr = resultFileBuffer.readLine()) != null) {
			    			System.out.println(tempStr);
			    			localBufferWriter.write(tempStr + "\n");
		    			}
	    		
		    		localBufferWriter.close();
		    		resultFileBuffer.close();
		    		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
End of code

*A file named result-file... will appear in the project folder, the job is done!!