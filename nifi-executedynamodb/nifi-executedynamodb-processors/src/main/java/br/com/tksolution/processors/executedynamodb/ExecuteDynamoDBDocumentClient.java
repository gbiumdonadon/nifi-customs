package br.com.tksolution.processors.executedynamodb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class ExecuteDynamoDBDocumentClient {
	
	private static ExecuteDynamoDBDocumentClient instance;
	private AmazonDynamoDB client;
	
	public AmazonDynamoDB getDocumentClient() {
		return client;
	}
	
	private ExecuteDynamoDBDocumentClient(AWSCredentialsProvider awsCredentialsProvider, Regions region) throws Exception {
		
		client = AmazonDynamoDBClient.builder()
				.withCredentials(awsCredentialsProvider)
				.withRegion(region)
				.build();
					
	}
	
	public static ExecuteDynamoDBDocumentClient getInstance(AWSCredentialsProvider awsCredentialsProvider, Regions region) throws Exception 
	{
		if (instance == null) {
            instance = new ExecuteDynamoDBDocumentClient(awsCredentialsProvider, region);
        }
        return instance;
	}

}
