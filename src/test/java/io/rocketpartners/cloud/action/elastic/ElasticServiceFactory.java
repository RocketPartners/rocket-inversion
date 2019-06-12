/**
 * 
 */
package io.rocketpartners.cloud.action.elastic;

import java.util.concurrent.TimeUnit;

import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.CreateElasticsearchDomainRequest;
import com.amazonaws.services.elasticsearch.model.CreateElasticsearchDomainResult;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainResult;
import com.amazonaws.services.elasticsearch.model.EBSOptions;
import com.amazonaws.services.elasticsearch.model.ElasticsearchClusterConfig;
import com.amazonaws.services.elasticsearch.model.NodeToNodeEncryptionOptions;
import com.amazonaws.services.elasticsearch.model.VolumeType;

import ch.qos.logback.classic.Level;

/**
 * @author tc-rocket
 *
 */
public class ElasticServiceFactory
{
   public static String domainName = "test-northwind2";

   public static void main(String[] args)
   {
      setLogLevel(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME, Level.INFO);
      createElasticCluster();
   }

   public static void createElasticCluster()
   {
      AWSElasticsearch client = AWSElasticsearchClientBuilder.standard().build();
      createDomain(client, domainName);
      waitForDomainProcessing(client, domainName);
   }

   static void createDomain(AWSElasticsearch client, String domainName)
   {

      String accountId = "906203010865";
      String region = "us-east-1";
      String testUserArn = accountId; // "arn:aws:iam::" + accountId + ":user/*";
      String esDomainArn = "arn:aws:es:" + region + ":" + accountId + ":domain/" + domainName + "/*";

      // Create the request and set the desired configuration options
      CreateElasticsearchDomainRequest createRequest = new CreateElasticsearchDomainRequest();
      createRequest.withDomainName(domainName)//
                   .withElasticsearchVersion("6.5")//
                   .withElasticsearchClusterConfig(//
                         new ElasticsearchClusterConfig()//
                                                         .withDedicatedMasterEnabled(false)//
                                                         .withInstanceType("t2.small.elasticsearch")//
                                                         .withInstanceCount(1)//
                   )
                   // Many instance types require EBS storage.
                   .withEBSOptions(//
                         new EBSOptions()//
                                         .withEBSEnabled(true)//
                                         .withVolumeSize(10)//
                                         .withVolumeType(VolumeType.Gp2)//
                   )//
                   .withAccessPolicies("{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"" + testUserArn + "\"]},\"Action\":[\"es:*\"],\"Resource\":\"" + esDomainArn + "\"}]}").withNodeToNodeEncryptionOptions(new NodeToNodeEncryptionOptions().withEnabled(true));

      // Make the request.
      System.out.println("Sending domain creation request...");
      CreateElasticsearchDomainResult createResponse = client.createElasticsearchDomain(createRequest);
      System.out.println("Domain creation response from Amazon Elasticsearch Service:");
      System.out.println(createResponse.getDomainStatus().toString());

   }

   /**
    * Waits for the domain to finish processing changes. New domains typically take
    * 10-15 minutes to initialize. Most updates to existing domains take a similar
    * amount of time. This method checks every 15 seconds and finishes only when
    * the domain's processing status changes to false.
    *
    * @param client
    *            The AWSElasticsearch client to use for the requests to Amazon
    *            Elasticsearch Service
    * @param domainName
    *            The name of the domain that you want to check
    */
   static void waitForDomainProcessing(AWSElasticsearch client, String domainName)
   {
      // Create a new request to check the domain status.
      final DescribeElasticsearchDomainRequest describeRequest = new DescribeElasticsearchDomainRequest().withDomainName(domainName);

      System.out.println("Waiting for Elasticsearch Service Domain to finish processing...");

      // Check whether the domain is processing, which usually takes 10-15 minutes
      // after creation or a configuration change.
      // This loop checks every 15 seconds.
      DescribeElasticsearchDomainResult describeResponse = client.describeElasticsearchDomain(describeRequest);
      while (describeResponse.getDomainStatus().isProcessing())
      {
         try
         {
            System.out.print(".");
            TimeUnit.SECONDS.sleep(15);
            describeResponse = client.describeElasticsearchDomain(describeRequest);
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
      }

      // Once we exit that loop, the domain is available
      System.out.println(" ");
      System.out.println("Amazon Elasticsearch Service has finished processing changes for your domain.");
      System.out.println("Domain description response from Amazon Elasticsearch Service:");
      System.out.println(describeResponse.toString());
   }

   static void setLogLevel(String loggerName, Level level)
   {
      ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(loggerName);
      logger.setLevel(level);
   }

}
