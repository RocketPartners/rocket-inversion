/**
 * 
 */
package io.rocketpartners.cloud.action.elastic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

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
import io.rocketpartners.cloud.action.sql.SqlServiceFactory;
import io.rocketpartners.cloud.model.ArrayNode;
import io.rocketpartners.cloud.model.ObjectNode;
import io.rocketpartners.cloud.model.Response;
import io.rocketpartners.cloud.service.Service;
import io.rocketpartners.cloud.utils.HttpUtils;
import io.rocketpartners.cloud.utils.HttpUtils.FutureResponse;

/**
 * @author tc-rocket
 *
 */
public class ElasticServiceFactory
{
   //public static String    region     = "us-east-1";
   //public static String    accountId  = "906203010865";
   public static String     domainName = "test-northwind";
   public static String[]   allowedIps = new String[]{"38.142.96.138"};
   static AWSElasticsearch  elasticsearchClient;

   static List<IndexConfig> indexes    = new ArrayList<>();

   static
   {
      setLogLevel(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME, Level.INFO);

      indexes.add(new IndexConfig("orders", "northwind/source/orders?pageSize=1000", "orderid"));
      indexes.add(new IndexConfig("products", "northwind/source/products?pageSize=1000", "productid"));
      indexes.add(new IndexConfig("suppliers", "northwind/source/suppliers?pageSize=1000", "supplierid"));
   }

   public static void main(String[] args)
   {
      try
      {
         service();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static synchronized Service service() throws Exception
   {
      Service service = SqlServiceFactory.service();

      String domainEndpoint = findDomainEndpoint();
      if (domainEndpoint == null)
      {
         System.out.println("No existing elastic search cluster found. Creating new elastic search cluster. " + domainName);
         createElasticCluster();
      }

      domainEndpoint = "https://" + domainEndpoint + "/";

      System.out.println("Found existing elastic search cluster '" + domainName + "' with endpoint: " + domainEndpoint);
      System.out.println("Refreshing the elastic search indexes");
      refreshIndexes(domainEndpoint, service);

      return null;
   }

   static void refreshIndexes(String domainEndpoint, Service service)
   {
      for (IndexConfig index : indexes)
      {
         deleteIndex(domainEndpoint, index.name).get();
      }

      List<FutureResponse> futures = new ArrayList<>();
      for (IndexConfig index : indexes)
      {
         System.out.println("Refreshing index: " + index.name);
         Response res = service.service("GET", index.sourcePath);
         ArrayNode data = res.data();

         for (Object obj : data)
         {
            ObjectNode doc = (ObjectNode) obj;
            futures.add(indexDocument(domainEndpoint, index.name, doc.getString(index.idColumn), doc.toString()));
         }
      }

      // Do this to wait for all requests to return
      for (FutureResponse future : futures)
      {
         future.get();
      }

   }

   static FutureResponse indexDocument(String domainEndpoint, String indexName, String documentId, String documentContent)
   {
      ArrayListValuedHashMap<String, String> headers = new ArrayListValuedHashMap();
      headers.put("Content-type", "application/json");

      return HttpUtils.rest("PUT", domainEndpoint + indexName + "/_doc/" + documentId, documentContent, headers, -1);
   }

   static FutureResponse deleteIndex(String domainEndpoint, String indexName)
   {
      ArrayListValuedHashMap<String, String> headers = new ArrayListValuedHashMap();
      headers.put("Content-type", "application/json");

      return HttpUtils.rest("DELETE", domainEndpoint + indexName, null, headers, -1);
   }

   static AWSElasticsearch getElasticsearchClient()
   {
      if (elasticsearchClient == null)
      {
         elasticsearchClient = AWSElasticsearchClientBuilder.standard().build();
      }

      return elasticsearchClient;
   }

   static void createElasticCluster()
   {
      createDomain();
      System.out.println("It will take around 10-15 minutes for this new cluster to be ready.");
      waitForDomainProcessing();
   }

   static void createDomain()
   {
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
                   .withAccessPolicies(buildAccessPolicy())//
                   .withNodeToNodeEncryptionOptions(new NodeToNodeEncryptionOptions().withEnabled(true));

      // Make the request.
      System.out.println("Sending domain creation request...");
      CreateElasticsearchDomainResult createResponse = getElasticsearchClient().createElasticsearchDomain(createRequest);
      System.out.println("Domain creation response from Amazon Elasticsearch Service:");
      System.out.println(createResponse.getDomainStatus().toString());

   }

   static String buildAccessPolicy()
   {
      ObjectNode statement = new ObjectNode();
      statement.put("Effect", "Allow");
      statement.put("Principal", new ObjectNode("AWS", "*"));
      statement.put("Action", new ArrayNode("es:*"));
      statement.put("Condition", new ObjectNode("IpAddress", new ObjectNode("aws:SourceIp", new ArrayNode(allowedIps))));
      statement.put("Resource", "*");

      ObjectNode root = new ObjectNode();
      root.put("Version", "2012-10-17");
      root.put("Statement", new ArrayNode(statement));

      return root.toString();
   }

   /**
    * Waits for the domain to finish processing changes. New domains typically take
    * 10-15 minutes to initialize. Most updates to existing domains take a similar
    * amount of time. This method checks every 10 seconds and finishes only when
    * the domain's processing status changes to false.
    *
    * @param client
    *            The AWSElasticsearch client to use for the requests to Amazon
    *            Elasticsearch Service
    * @param domainName
    *            The name of the domain that you want to check
    */
   static void waitForDomainProcessing()
   {
      // Create a new request to check the domain status.
      final DescribeElasticsearchDomainRequest describeRequest = new DescribeElasticsearchDomainRequest().withDomainName(domainName);

      System.out.println("Waiting for Elasticsearch Service Domain to finish processing...");

      // Check whether the domain is processing, which usually takes 10-15 minutes
      // after creation or a configuration change.
      // This loop checks every 10 seconds.
      DescribeElasticsearchDomainResult describeResponse = getElasticsearchClient().describeElasticsearchDomain(describeRequest);

      while (describeResponse.getDomainStatus().isProcessing())
      {
         try
         {
            System.out.print(".");
            TimeUnit.SECONDS.sleep(10);
            describeResponse = getElasticsearchClient().describeElasticsearchDomain(describeRequest);
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

   static String findDomainEndpoint()
   {
      try
      {
         // Create a new request to check the domain status.
         final DescribeElasticsearchDomainRequest describeRequest = new DescribeElasticsearchDomainRequest().withDomainName(domainName);
         DescribeElasticsearchDomainResult describeResponse = getElasticsearchClient().describeElasticsearchDomain(describeRequest);

         return describeResponse.getDomainStatus().getEndpoint();
      }
      catch (Exception e)
      {
      }

      return null;

   }

   static void setLogLevel(String loggerName, Level level)
   {
      ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(loggerName);
      logger.setLevel(level);
   }

   static class IndexConfig
   {
      String name;
      String sourcePath;
      String idColumn;

      public IndexConfig(String name, String sourcePath, String idColumn)
      {
         super();
         this.name = name;
         this.sourcePath = sourcePath;
         this.idColumn = idColumn;
      }

   }

}
