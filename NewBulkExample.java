/* Author : Rangesh Kona
*/
import java.io.*;
import java.util.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;


public class NewBulkExample {

   public static void main(String[] args) throws AsyncApiException, ConnectionException, IOException {

	NewBulkExample nbe = new NewBulkExample();
	nbe.createRecords("Account", "rangeshsecure@gmail.com","Layout$1234", "mySampleData.csv");
   }

   public void createRecords(String sobjectType, String userName, String password, String sampleFileName) 
		throws ConnectionException, IOException, AsyncApiException { 
	
	// get SFDC connection using partner WSDL 
	BulkConnection 		connection  = getBulkConnection(userName, password);

	// Create Job
	JobInfo 		job = createJob(sobjectType, connection);


	// Create Batch
	BatchInfo 		batchInfo  = createBatchFromCSVFile(connection, job, sampleFileName);
	
	// Close Job
 	closeJob(connection, job.getId());	
	
	// Await Completion
	awaitCompletion(connection, job, batchInfo); 

	// Check Results
	checkResults(connection, job, batchInfo);
   }



	private void checkResults(BulkConnection connection, JobInfo job,
                                BatchInfo batchInfo)
                throws AsyncApiException, IOException {
                CSVReader rdr = new CSVReader(connection.getBatchResultStream (job.getId(), batchInfo.getId()));
                List<String> resultHeader = rdr.nextRecord();
                int resultCols = resultHeader.size();
                List<String> row;

                while((row = rdr.nextRecord()) != null) {
                   Map<String, String> resultInfo = new HashMap<String, String>();
                   for(int i = 0; i<resultCols;i++) {
                      resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created ) {
                   System.out.println("Created row with id " + id );
                }
                else if (!success) {
                   System.out.println("Failed with error: " + error);
                }
                }
          }


  private void awaitCompletion(BulkConnection connection, JobInfo job, BatchInfo batchInfo) throws AsyncApiException {

	long sleepTime = 0L;
	Set<String> incomplete = new HashSet<String>();
	incomplete.add(batchInfo.getId());
	while (!incomplete.isEmpty()) {
	   try {
	      Thread.sleep(sleepTime);
	   }
	   catch(InterruptedException e) {}
	   System.out.println("Awaiting results...." + incomplete.size());
	   sleepTime = 10000L;
	   BatchInfo[] statusList = connection.getBatchInfoList(job.getId()).getBatchInfo();
	   for(BatchInfo b : statusList) {
		if(b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed) {
		   if (incomplete.remove(b.getId())) {
                       System.out.println("Batch Status : \n" + b);
                   }
		}	
           }

	}
  } 
 

  private void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
	JobInfo job = new JobInfo();
	job.setId(jobId);
	job.setState(JobStateEnum.Closed);
	connection.updateJob(job);
  }



  private BatchInfo createBatchFromCSVFile(BulkConnection connection, JobInfo jobInfo, String sampleFileName) throws IOException, AsyncApiException {


	FileInputStream tmpInputStream	= new FileInputStream(sampleFileName);
 
	BatchInfo batchInfo;

	try {
	    batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream);
	    System.out.println("Batch Info " + batchInfo);
	}
	finally {
	    tmpInputStream.close();
	}
	return batchInfo;
  }

   private JobInfo createJob(String sobjectType, BulkConnection connection) throws AsyncApiException {

	JobInfo job = new JobInfo();
	job.setObject(sobjectType);
	job.setOperation(OperationEnum.insert);
	job.setContentType(ContentType.CSV);
	job = connection.createJob(job);
	System.out.println("Created Job " + job );
	return job;

   }

   private BulkConnection getBulkConnection(String userName, String password) throws ConnectionException, AsyncApiException {

   	ConnectorConfig partnerConfig = new ConnectorConfig();
	partnerConfig.setUsername(userName);
	partnerConfig.setPassword(password);
	partnerConfig.setAuthEndpoint("https://rangesh-dev-ed.my.salesforce.com/services/Soap/u/41.0");
	new PartnerConnection(partnerConfig);
	ConnectorConfig config = new ConnectorConfig();
	config.setSessionId(partnerConfig.getSessionId());
	String soapEndpoint	= partnerConfig.getServiceEndpoint();
	String apiVersion	= "41.0";
	String restEndpoint    = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
	config.setRestEndpoint(restEndpoint);
	config.setCompression(true);
	config.setTraceMessage(false);
	BulkConnection connection = new BulkConnection(config);
	return connection;
	
   }
}

