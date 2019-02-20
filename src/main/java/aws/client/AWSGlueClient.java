package aws.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

public class AWSGlueClient {

    public static AWSGlue createAWSGlueClient(String accessKeyId, String secretAccessKey, String awsRegion){
        AWSCredentials credentials;
        try {
            credentials = new BasicAWSCredentials(accessKeyId,secretAccessKey);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to create AWS Credentials");
        }

        System.out.println("===========================================");
        System.out.println("Getting Started with AWS Glue");
        System.out.println("===========================================");

        return AWSGlueClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(awsRegion)
                .build();

    }
}
