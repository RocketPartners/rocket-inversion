/*
 * Copyright (c) 2015-2018 Rocket Partners, LLC
 * http://rocketpartners.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */
package io.rocketpartners.cloud.action.dynamo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.rocketpartners.cloud.model.ApiException;
import io.rocketpartners.cloud.model.Collection;
import io.rocketpartners.cloud.model.Column;
import io.rocketpartners.cloud.model.Db;
import io.rocketpartners.cloud.model.Entity;
import io.rocketpartners.cloud.model.Index;
import io.rocketpartners.cloud.model.Results;
import io.rocketpartners.cloud.model.SC;
import io.rocketpartners.cloud.model.Table;
import io.rocketpartners.cloud.rql.Term;
import io.rocketpartners.cloud.service.Chain;
import io.rocketpartners.cloud.utils.Rows.Row;
import io.rocketpartners.cloud.utils.Utils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDb extends Db<DynamoDb>
{
   protected String       awsAccessKey = null;
   protected String       awsSecretKey = null;
   protected String       awsRegion    = "us-east-1";

   /**
    * A CSV of pipe delimited collection name to table name pairs.
    *
    * Example: dynamodb.tables=promo|promo-dev,loyalty-punchcard|loyalty-punchcard-dev
    *
    * Or if the collection name is the name as the table name you can just send a the name
    *
    * Example: dynamodb.includeTables=orders,users,events
    */
   protected String       includeTables;

   /**
    * Use to config which row is used to build the column/attribute model  (otherwise first row of scan will be used)
    *
    * FORMAT: collection name | primaryKey | sortKey (optional)
    */
   protected String       blueprintRow;

   protected int          batchMax     = 20;

   private DynamoDbClient dynamoClient = null;

   public DynamoDb()
   {
      this.withType("dynamodb");
   }

   public DynamoDb(String name, String includeTables)
   {
      this();
      this.name = name;
      this.includeTables = includeTables;
   }

   @Override
   public Results<Row> select(Table table, List<Term> columnMappedTerms) throws Exception
   {
      DynamoDbQuery query = new DynamoDbQuery(table, columnMappedTerms).withDynamoClient(getDynamoClient(), table.getName());
      return query.doSelect();
   }

   @Override
   public String upsert(Table table, Map<String, Object> row) throws Exception
   {
      List<String> keys = upsert(table, Arrays.asList(row));
      if (keys != null && keys.size() > 0)
         return keys.get(0);

      return null;
   }

   @Override
   public List<String> upsert(Table table, List<Map<String, Object>> rows) throws Exception
   {
      DynamoDbClient dynamoClient = getDynamoClient();
      List keys = new ArrayList();
      List<WriteRequest> writeRequests = new LinkedList<WriteRequest>();

      for (int i = 0; i < rows.size(); i++)
      {
         Map<String, Object> row = rows.get(i);

         String key = table.encodeKey(row);
         keys.add(key);

         for (String attr : (List<String>)new ArrayList(row.keySet()))
         {
            if (Utils.empty(row.get(attr)))
               row.remove(attr);
         }

         if (i > 0 && i % batchMax == 0)
         {
            //write a batch to dynamo
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(table.getName(), writeRequests);
            dynamoClient.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
            writeRequests.clear();
         }
         //add to the current row to batch
         Map<String, AttributeValue> item = DynamoV2Utils.toItemMap(row);
         Chain.debug("DynamoDb", "PutRequest", item);
         PutRequest put = PutRequest.builder().item(item).build();
         writeRequests.add(WriteRequest.builder().putRequest(put).build());
      }

      if (writeRequests.size() > 0)
      {
         Map<String, List<WriteRequest>> requestItems = new HashMap<>();
         requestItems.put(table.getName(), writeRequests);
         getDynamoClient().batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());
         writeRequests.clear();
      }

      return keys;
   }

   @Override
   public void delete(Table table, String entityKey) throws Exception
   {
      Row key = table.decodeKey(entityKey);

      Map<String, AttributeValue> keyMap = new HashMap<>();
      keyMap.put(key.getKey(0), DynamoV2Utils.toAttributeValue(key.get(0)));

      if (key.size() == 2)
      {
         keyMap.put(key.getKey(1), DynamoV2Utils.toAttributeValue(key.get(1)));
      }
      else if (key.size() > 2)
      {
         throw new ApiException(SC.SC_400_BAD_REQUEST, "A dynamo delete must have a hash key and an optional sortKey and that is it: '" + entityKey + "'");
      }

      getDynamoClient().deleteItem(DeleteItemRequest.builder()
         .tableName(table.getName())
         .key(keyMap)
         .build());
   }

   @Override
   protected void startup0()
   {
      if (includeTables != null)
      {
         Map<String, String[]> blueprintRowMap = new HashMap<>();
         if (blueprintRow != null)
         {
            String[] parts = blueprintRow.split(",");
            for (String part : parts)
            {
               String[] arr = part.split("\\|");
               String collection = arr[0];
               blueprintRowMap.put(collection, arr);
            }
         }

         String[] parts = includeTables.split(",");
         for (String part : parts)
         {
            String[] arr = part.split("\\|");
            String collectionName = arr[0];
            String tableName = collectionName;
            if (arr.length > 1)
            {
               tableName = arr[1];
            }
            else
            {
               collectionName = beautifyCollectionName(collectionName);
            }

            Table table = buildTable(tableName, blueprintRowMap.get(collectionName));
            withTable(table);

            Collection collection = buildCollection(collectionName, table);
            api.withCollection(collection);
         }

      }
      else
      {
         log.warn("DynamoDb must have 'tableMappings' configured to be used");
      }

   }

   Table buildTable(String tableName, String[] bluePrintArr)
   {
      DynamoDbClient dynamoClient = getDynamoClient();

      Table table = new Table(this, tableName);

      TableDescription tableDescription = dynamoClient.describeTable(
         DescribeTableRequest.builder().tableName(tableName).build()
      ).table();

      for (AttributeDefinition attr : tableDescription.attributeDefinitions())
      {
         table.makeColumn(attr.attributeName(), attr.attributeTypeAsString());
      }

      DynamoDbIndex index = new DynamoDbIndex(table, DynamoDbIndex.PRIMARY_INDEX, DynamoDbIndex.PRIMARY_TYPE);

      List<KeySchemaElement> keySchema = tableDescription.keySchema();
      for (KeySchemaElement keyInfo : keySchema)
      {
         if (keyInfo.keyTypeAsString().equalsIgnoreCase("HASH"))
         {
            index.witHashKey(table.getColumn(keyInfo.attributeName()));
         }
         else if (keyInfo.keyTypeAsString().equalsIgnoreCase("RANGE"))
         {
            index.withSortKey(table.getColumn(keyInfo.attributeName()));
         }
      }

      if (tableDescription.globalSecondaryIndexes() != null)
      {
         for (GlobalSecondaryIndexDescription indexDesc : tableDescription.globalSecondaryIndexes())
         {
            addTableIndex(DynamoDbIndex.GLOBAL_SECONDARY_TYPE, indexDesc.indexName(), indexDesc.keySchema(), table);
         }
      }

      if (tableDescription.localSecondaryIndexes() != null)
      {
         for (LocalSecondaryIndexDescription indexDesc : tableDescription.localSecondaryIndexes())
         {
            addTableIndex(DynamoDbIndex.LOCAL_SECONDARY_TYPE, indexDesc.indexName(), indexDesc.keySchema(), table);
         }
      }

      return table;
   }

   protected Collection buildCollection(String collectionName, Table table)
   {
      Collection collection = new Collection();
      collection.withName(beautifyCollectionName(collectionName));
      collection.withTable(table);

      Entity entity = collection.getEntity();

      for (Column col : table.getColumns())
      {
         entity.getAttribute(col.getName()).withName(beautifyAttributeName(col.getName()));
      }

      if (getCollectionPath() != null)
         collection.withIncludePaths(getCollectionPath());

      return collection;
   }

   protected void addTableIndex(String type, String indexName, List<KeySchemaElement> keySchemaList, Table table)
   {
      DynamoDbIndex index = new DynamoDbIndex(table, indexName, type);

      for (KeySchemaElement keyInfo : keySchemaList)
      {
         Column column = table.getColumn(keyInfo.attributeName());

         index.withColumn(column);

         if (keyInfo.keyTypeAsString().equalsIgnoreCase("HASH"))
         {
            index.witHashKey(table.getColumn(keyInfo.attributeName()));
         }

         else if (keyInfo.keyTypeAsString().equalsIgnoreCase("RANGE"))
         {
            index.withSortKey(table.getColumn(keyInfo.attributeName()));
         }
      }

      table.withIndex(index);
   }

   public DynamoDbClient getDynamoClient()
   {
      if (this.dynamoClient == null)
      {
         synchronized (this)
         {
            if (this.dynamoClient == null)
            {
               this.dynamoClient = buildDynamoClient(name + ".", awsRegion, awsAccessKey, awsSecretKey);
            }
         }
      }

      return dynamoClient;
   }

   public DynamoDb withIncludeTables(String includeTables)
   {
      this.includeTables = includeTables;
      return this;
   }

   public DynamoDb withBlueprintRow(String blueprintRow)
   {
      this.blueprintRow = blueprintRow;
      return this;
   }

   public DynamoDb withAwsRegion(String awsRegion)
   {
      this.awsRegion = awsRegion;
      return this;
   }

   public DynamoDb withAwsAccessKey(String awsAccessKey)
   {
      this.awsAccessKey = awsAccessKey;
      return this;
   }

   public DynamoDb withAwsSecretKey(String awsSecretKey)
   {
      this.awsSecretKey = awsSecretKey;
      return this;
   }

   @Override
   public String toString()
   {
      return this.getClass().getSimpleName() + " - " + this.getName() + " - " + this.getTables();
   }

   /**
    * Used to keep track of Hash and Sort keys for a dynamo index.
    *
    * @author kfrankic
    *
    */
   public static class DynamoDbIndex extends Index
   {
      public static final String PRIMARY_INDEX         = "Primary Index";

      public static final String PRIMARY_TYPE          = "primary";
      public static final String LOCAL_SECONDARY_TYPE  = "localsecondary";
      public static final String GLOBAL_SECONDARY_TYPE = "globalsecondary";

      protected Column           hashKey               = null;
      protected Column           sortKey               = null;

      public DynamoDbIndex()
      {
         super();
      }

      public DynamoDbIndex(Table table, String name, String type)
      {
         super(table, name, type);
      }

      public DynamoDbIndex(Table table, String name, String type, Column pk, Column sk)
      {
         super(table, name, type);

         this.hashKey = pk;
         this.sortKey = sk;
      }

      public boolean isLocalIndex()
      {
         return LOCAL_SECONDARY_TYPE.equalsIgnoreCase(type);
      }

      public boolean isPrimaryIndex()
      {
         return PRIMARY_TYPE.equalsIgnoreCase(type);
      }

      public boolean isGlobalSecondary()
      {
         return !isLocalIndex() && !isPrimaryIndex();
      }

      public Column getHashKey()
      {
         return hashKey;
      }

      public String getHashKeyName()
      {
         return hashKey != null ? hashKey.getName() : null;
      }

      public DynamoDbIndex witHashKey(Column hashKey)
      {
         this.hashKey = hashKey;
         withColumn(hashKey);
         return this;
      }

      public Column getSortKey()
      {
         return sortKey;
      }

      public String getSortKeyName()
      {
         return sortKey != null ? sortKey.getName() : null;
      }

      public DynamoDbIndex withSortKey(Column sortKey)
      {
         this.sortKey = sortKey;
         withColumn(sortKey);
         return null;
      }

   }

   public static DynamoDbIndex findIndexByName(Table table, String name)
   {
      if (table != null && table.getIndexes() != null)
      {
         for (DynamoDbIndex index : (List<DynamoDbIndex>) (List<?>) table.getIndexes())
         {
            if (index.getName().equals(name))
            {
               return index;
            }
         }
      }
      return null;
   }

   /*
    * These match the string that dynamo uses for these types.
    * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html
    */
   protected static String getTypeStringFromObject(Object obj)
   {
      if (obj instanceof Number)
      {
         return "N";
      }
      else if (obj instanceof Boolean)
      {
         return "BOOL";
      }
      else
      {
         return "S";
      }
   }

   @Override
   public Object cast(String type, Object value)
   {
      try
      {
         if (value == null)
            return null;

         if (type == null)
         {
            try
            {
               if (value.toString().indexOf(".") < 0)
               {
                  value = Long.parseLong(value.toString());
               }
               else
               {
                  value = Double.parseDouble(value.toString());
               }

               return value;
            }
            catch (Exception ex)
            {

            }
            return value.toString();
         }

         switch (type)
         {
            case "S":
               return value.toString();

            case "N":
               if (value.toString().indexOf(".") < 0)
                  return Long.parseLong(value.toString());
               else
                  Double.parseDouble(value.toString());
            case "BOOL":
               return Boolean.parseBoolean(value.toString());

            default :
               return value.toString();
         }
      }
      catch (Exception ex)
      {
         throw new RuntimeException("Error casting '" + value + "' to type '" + type + "'", ex);
      }
   }

   public static DynamoDbClient buildDynamoClient(String prefix)
   {
      return buildDynamoClient(prefix, null, null, null);
   }

   public static DynamoDbClient buildDynamoClient(String prefix, String awsRegion, String awsAccessKey, String awsSecretKey)
   {
      awsRegion = Utils.findSysEnvPropStr(prefix + ".awsRegion", awsRegion);
      awsAccessKey = Utils.findSysEnvPropStr(prefix + ".awsAccessKey", awsAccessKey);
      awsSecretKey = Utils.findSysEnvPropStr(prefix + ".awsSecretKey", awsSecretKey);

      DynamoDbClientBuilder builder = DynamoDbClient.builder();
      if (!Utils.empty(awsRegion))
      {
         builder.region(Region.of(awsRegion));
      }
      if (!Utils.empty(awsAccessKey) && !Utils.empty(awsSecretKey))
      {
         AwsBasicCredentials creds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);
         builder.credentialsProvider(StaticCredentialsProvider.create(creds));
      }

      return builder.build();
   }

}
