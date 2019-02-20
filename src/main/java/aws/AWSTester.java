package aws;

import aws.client.AWSGlueClient;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;

import java.util.Map;

public class AWSTester {

    public void add() {
        System.out.println("223r3");
    }


    public static void main(String[] args) {


        AWSGlue awsGlue = AWSGlueClient.createAWSGlueClient("AKIAJ5YTZVWQYU5EXSNA", "VsUB03TvLwfVCDBjpj8mmQq6IiMPNsIZ59RmVd9s", Regions.US_EAST_1.getName());

        GetDatabasesResult databases = awsGlue.getDatabases(new GetDatabasesRequest());
        databases.getDatabaseList().forEach((d) -> {
            System.out.println("**************************");
            System.out.println("Database : " + d.getName());
            System.out.println("DB's location : " + d.getLocationUri());
            System.out.println("**************************");

            GetTablesResult tables = awsGlue.getTables(new GetTablesRequest().withDatabaseName(d.getName()));
            tables.getTableList().forEach(t -> {
                System.out.println("Full Table Details");
                System.out.println(t);
                System.out.println("-------------------------");
                System.out.println("Table Name :" + t.getName());
                System.out.println("Table Type : " + t.getTableType());

                System.out.println("Schema :");
                t.getStorageDescriptor().getColumns().forEach(c -> System.out.println(c.getName() + " : " + c.getType()));
                System.out.println("Parameters");
                Map<String, String> parameters = t.getParameters();
                parameters.keySet().forEach(k -> System.out.println(k + ":" + parameters.get(k)));
                System.out.println("-------------------------");
            });
            System.out.println("-------------------------");
        });


        System.out.println("JOBS");
        GetJobsResult jobs = awsGlue.getJobs(new GetJobsRequest());
        jobs.getJobs().forEach(job -> {
            System.out.println(job.getName());
            JobCommand command = job.getCommand();
        });

        System.out.println("CRAWLERS");
        GetCrawlersResult crawlers = awsGlue.getCrawlers(new GetCrawlersRequest());
        crawlers.getCrawlers().forEach(crawler -> {
            System.out.println("Crawler Name : " + crawler.getName());
            CrawlerTargets targets = crawler.getTargets();
            System.out.println("S3 Targets");
            targets.getS3Targets().forEach(s3Target -> {
                System.out.println(s3Target.getPath());
            });
        });


    }
}


/* Amazon S3 Sample Code


//
//        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withRegion("us-west-2")
//                .build();
//


//
//        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
//        String key = "MyObjectKey";
//
//        System.out.println("===========================================");
//        System.out.println("Getting Started with Amazon S3");
//        System.out.println("===========================================\n");
//
//
//        List<Bucket> buckets = s3.listBuckets();
//
//        for(Bucket bucket : buckets){
//            System.out.println(bucket.getName());
//        }


 */
