package io.rocketpartners.cloud.action.dynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rocketpartners.cloud.action.rest.RestAction;
import io.rocketpartners.cloud.action.sql.SqlServiceFactory;
import io.rocketpartners.cloud.model.Api;
import io.rocketpartners.cloud.model.ArrayNode;
import io.rocketpartners.cloud.model.Collection;
import io.rocketpartners.cloud.model.ObjectNode;
import io.rocketpartners.cloud.model.Response;
import io.rocketpartners.cloud.service.Service;
import io.rocketpartners.cloud.utils.Utils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class DynamoServiceFactory
{
   public static String                  northwind    = "test-northwind";
   public static boolean                 relaodDynamo = false;
   protected static Map<String, Service> services     = new HashMap();

   public static synchronized Service service() throws Exception
   {
      return service("northwind", "northwind");
   }

   public static synchronized Service service(String apiName, final String ddl) throws Exception
   {
      Service service = services.get(apiName.toLowerCase());
      if (service != null)
         return service;

      service = buildService(apiName, ddl, "test-" + ddl);

      services.put(apiName, service);

      return service;
   }

   protected static Service buildService(String apiCode, final String ddl, String dynamoTbl) throws Exception
   {
      buildTables();

      Service service = SqlServiceFactory.service();

      final DynamoDb dynamoDb = new DynamoDb("dynamo", dynamoTbl);
      DynamoDbClient dynamoClient = dynamoDb.getDynamoClient();

      final Api api = service.getApi(apiCode);
      api.withDb(dynamoDb);
      api.withEndpoint("GET,PUT,POST,DELETE", "dynamodb/*", new RestAction());

      dynamoDb.startup();

      Collection orders = api.getCollection(dynamoTbl + "s");
      orders.withName("orders");

      orders.getAttribute("hk").withName("orderId");
      orders.getAttribute("sk").withName("type");

      orders.getAttribute("gs1hk").withName("employeeId");
      orders.getAttribute("gs1sk").withName("orderDate");

      orders.getAttribute("ls1").withName("shipCity");
      orders.getAttribute("ls2").withName("shipName");
      orders.getAttribute("ls3").withName("requireDate");

      orders.withIncludePaths("dynamodb/*");

      Response res = null;

      if (relaodDynamo)
      {
         System.out.print("CLEARING DYNAMO...");

         Map<String, AttributeValue> lastKey = null;
         int deletedCount = 0;

         do
         {
            ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(dynamoTbl);
            if (lastKey != null)
            {
               scanBuilder.exclusiveStartKey(lastKey);
            }

            ScanResponse scanResponse = dynamoClient.scan(scanBuilder.build());

            for (Map<String, AttributeValue> item : scanResponse.items())
            {
               deletedCount += 1;
               if (deletedCount % 100 == 0)
                  System.out.print(deletedCount + " ");

               Map<String, AttributeValue> keyMap = new HashMap<>();
               keyMap.put("hk", item.get("hk"));
               keyMap.put("sk", item.get("sk"));

               dynamoClient.deleteItem(DeleteItemRequest.builder()
                  .tableName(dynamoTbl)
                  .key(keyMap)
                  .build());
            }

            lastKey = scanResponse.lastEvaluatedKey();
         }
         while (lastKey != null && !lastKey.isEmpty());

         //--confirm all deleted
         res = service.get("northwind/dynamodb/orders");
         res.statusOk();
         Utils.assertEq(0, res.findArray("data").length());

         System.out.println("");
         System.out.println("RELOADING DYNAMO...");

         int pages = 0;
         int total = 0;
         String start = "northwind/source/orders?pageSize=100&sort=orderid";
         String next = start;
         do
         {
            ArrayNode toPost = new ArrayNode();

            res = service.get(next);
            if (res.data().size() == 0)
               break;

            pages += 1;
            next = res.next();

            //-- now post to DynamoDb
            for (Object o : res.data())
            {
               total += 1;
               ObjectNode js = (ObjectNode) o;

               js.remove("href");
               js.put("type", "ORDER");

               for (String key : js.keySet())
               {
                  String value = js.getString(key);
                  if (value != null && (value.startsWith("http://") || value.startsWith("https://")))
                  {
                     value = value.substring(value.lastIndexOf("/") + 1, value.length());
                     js.remove(key);

                     if (!key.toLowerCase().endsWith("id"))
                        key = key + "Id";

                     js.put(key, value);
                  }
               }
               toPost.add(js);
            }

            res = service.post("northwind/dynamodb/orders", toPost);
            Utils.assertEq(201, res.getStatusCode());
            System.out.println("DYNAMO LOADED: " + total);
         }
         while (pages < 200 && next != null);

         Utils.assertEq(9, pages);
         Utils.assertEq(830, total);
      }

      return service;
   }

   public static void main(String[] args) throws Exception
   {
      buildTables();
   }

   public static void buildTables() throws Exception
   {
      if (!tableExists(northwind))
         createNorthwind();
   }

   public static boolean tableExists(String tableName) throws Exception
   {
      try (DynamoDbClient client = DynamoDb.buildDynamoClient(tableName)) {
         try {
            client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            return true;
         } catch (ResourceNotFoundException e) {
            return false;
         }
      }
   }

   public static void deleteTable(String tableName) throws Exception
   {
      try(DynamoDbClient client = DynamoDb.buildDynamoClient(tableName)) {
         client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
      }
   }

   public static void createNorthwind() throws Exception
   {
      List<AttributeDefinition> attrs = new ArrayList<>();

      attrs.add(AttributeDefinition.builder().attributeName("hk").attributeType(ScalarAttributeType.N).build());
      attrs.add(AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build());

      attrs.add(AttributeDefinition.builder().attributeName("gs1hk").attributeType(ScalarAttributeType.N).build());
      attrs.add(AttributeDefinition.builder().attributeName("gs1sk").attributeType(ScalarAttributeType.S).build());

      attrs.add(AttributeDefinition.builder().attributeName("gs2hk").attributeType(ScalarAttributeType.S).build());

      attrs.add(AttributeDefinition.builder().attributeName("ls1").attributeType(ScalarAttributeType.S).build());
      attrs.add(AttributeDefinition.builder().attributeName("ls2").attributeType(ScalarAttributeType.S).build());
      attrs.add(AttributeDefinition.builder().attributeName("ls3").attributeType(ScalarAttributeType.S).build());

      List<KeySchemaElement> keys = new ArrayList<>();
      keys.add(KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH).build());
      keys.add(KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build());

      Projection allProjection = Projection.builder().projectionType(ProjectionType.ALL).build();
      ProvisionedThroughput gsiThroughput = ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build();

      List<LocalSecondaryIndex> lsxs = new ArrayList();
      lsxs.add(LocalSecondaryIndex.builder().indexName("ls1").keySchema(
            KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("ls1").keyType(KeyType.RANGE).build())
            .projection(allProjection).build());

      lsxs.add(LocalSecondaryIndex.builder().indexName("ls2").keySchema(
            KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("ls2").keyType(KeyType.RANGE).build())
            .projection(allProjection).build());

      lsxs.add(LocalSecondaryIndex.builder().indexName("ls3").keySchema(
            KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("ls3").keyType(KeyType.RANGE).build())
            .projection(allProjection).build());

      List<GlobalSecondaryIndex> gsxs = new ArrayList();
      gsxs.add(GlobalSecondaryIndex.builder().indexName("gs1").keySchema(
            KeySchemaElement.builder().attributeName("gs1hk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("gs1sk").keyType(KeyType.RANGE).build())
            .projection(allProjection).provisionedThroughput(gsiThroughput).build());

      gsxs.add(GlobalSecondaryIndex.builder().indexName("gs2").keySchema(
            KeySchemaElement.builder().attributeName("gs2hk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("ls3").keyType(KeyType.RANGE).build())
            .projection(allProjection).provisionedThroughput(gsiThroughput).build());

      gsxs.add(GlobalSecondaryIndex.builder().indexName("gs3").keySchema(
            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("hk").keyType(KeyType.RANGE).build())
            .projection(allProjection).provisionedThroughput(gsiThroughput).build());

      try(DynamoDbClient client = DynamoDb.buildDynamoClient("northwind")) {

         CreateTableRequest request = CreateTableRequest.builder()
                 .globalSecondaryIndexes(gsxs)
                 .localSecondaryIndexes(lsxs)
                 .tableName("test-northwind")
                 .keySchema(keys)
                 .attributeDefinitions(attrs)
                 .provisionedThroughput(ProvisionedThroughput.builder()
                         .readCapacityUnits(5L)
                         .writeCapacityUnits(5L)
                         .build())
                 .build();


         client.createTable(request);

         try (DynamoDbWaiter waiter = DynamoDbWaiter.builder().client(client).build()) {
            waiter.waitUntilTableExists(DescribeTableRequest.builder().tableName("test-northwind").build());
         }
      }
   }

}
