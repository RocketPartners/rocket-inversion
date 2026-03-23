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
package io.rcktapp.api.handler.dynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Request;
import io.rcktapp.api.Response;
import io.rcktapp.api.SC;
import io.rcktapp.api.Table;
import io.rcktapp.api.service.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * @author tc-rocket
 *
 * Endpoint/Action Config
 *  - appendTenantIdToPk :        Enables appending the tenant id to the primary key
 *                                FORMAT: collection name (comma separated)
 *  - conditionalWriteConf :      Allows a conditional write expression to be configured for a dynamo table
 *                                FORMAT: collection name | withConditionExpression | payload fields  (comma seperated)  - EXAMPLE: promos|attribute_not_exists(primarykey) OR enddate <= :enddate|enddate
 *
 */
public class DynamoDbPostHandler extends DynamoDbHandler
{

   @Override
   public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {
      Collection collection = api.getCollection(req.getCollectionKey(), DynamoDb.class);
      Table table = collection.getEntity().getTable();
      DynamoDb db = (DynamoDb) table.getDb();
      DynamoDbClient dynamoClient = db.getDynamoDbClient();
      String tableName = table.getName();
      DynamoIndex dynamoIdx = DynamoDb.findIndexByName(table, DynamoDb.PRIMARY_INDEX);
      String pk = dynamoIdx.getPartitionKey();
      boolean appendTenantIdToPk = isAppendTenantIdToPk(chain, collection.getName());
      ConditionalWriteConf conditionalWriteConf = getConditionalWriteConf(collection, chain);

      Object tenantIdOrCode = null;
      if (req.getApi().isMultiTenant())
      {
         tenantIdOrCode = req.removeParam("tenantId");
         if (tenantIdOrCode != null)
         {
            tenantIdOrCode = Integer.parseInt((String) tenantIdOrCode);
         }
         else
         {
            tenantIdOrCode = req.getTenantCode();
         }
      }

      // using this instead of the built in req.getJson(), because JSObject converts everything to strings even if they are sent up as a number
      Object payloadObj = jsonStringToObject(req.getBody());

      if (payloadObj instanceof List)
      {
         List l = (List) payloadObj;
         for (Object obj : l)
         {
            putMapToDynamo((Map) obj, dynamoClient, tableName, pk, tenantIdOrCode, req.getApi().isMultiTenant(), appendTenantIdToPk, conditionalWriteConf);
         }
      }
      else if (payloadObj instanceof Map)
      {
         putMapToDynamo((Map) payloadObj, dynamoClient, tableName, pk, tenantIdOrCode, req.getApi().isMultiTenant(), appendTenantIdToPk, conditionalWriteConf);
      }

      res.setStatus(SC.SC_201_CREATED);

   }

   void putMapToDynamo(Map json, DynamoDbClient dynamoClient, String tableName, String pk, Object tenantIdOrCode, boolean isMultiTenant, boolean appendTenantIdToPk, ConditionalWriteConf conditionalWriteConf)
   {
      try
      {

         Map m = new HashMap<>(json);
         m.values().removeIf(v -> v == null);

         if (isMultiTenant && tenantIdOrCode != null)
         {
            m.put("tenantid", tenantIdOrCode);
            if (appendTenantIdToPk)
            {
               // add the tenantCode to the primary key
               String pkVal = (String) m.get(pk);
               m.put(pk, addTenantIdToKey(tenantIdOrCode, pkVal));
            }
         }

         Map<String, AttributeValue> item = DynamoV2Utils.toItemMap(m);

         PutItemRequest.Builder putBuilder = PutItemRequest.builder()
            .tableName(tableName)
            .item(item);

         if (conditionalWriteConf != null)
         {
            putBuilder.conditionExpression(conditionalWriteConf.expression);
            if (!conditionalWriteConf.fields.isEmpty())
            {
               Map<String, AttributeValue> expressionValues = new HashMap<>();
               for (String field : conditionalWriteConf.fields)
               {
                  expressionValues.put(":" + field, DynamoV2Utils.toAttributeValue(m.get(field)));
               }

               putBuilder.expressionAttributeValues(expressionValues);
            }
         }

         dynamoClient.putItem(putBuilder.build());
      }
      catch (ConditionalCheckFailedException ccfe)
      {
         // catch this and do nothing.
         // this just means the that conditional write wasn't satisfied
         // so the record was not written
      }
   }

   ConditionalWriteConf getConditionalWriteConf(Collection collection, Chain chain)
   {
      Set<String> condWriteConfgSet = chain.getConfigSet("conditionalWriteConf");

      if (condWriteConfgSet != null && !condWriteConfgSet.isEmpty())
      {
         for (String conf : condWriteConfgSet)
         {
            String[] arr = conf.split("\\|");
            String collectionName = arr[0];

            if (collection.getName().equalsIgnoreCase(collectionName))
            {
               ConditionalWriteConf conditionalWriteConf = new ConditionalWriteConf();
               conditionalWriteConf.expression = arr[1];

               if (arr.length > 2)
               {
                  for (int i = 2; i < arr.length; i++)
                  {
                     conditionalWriteConf.fields.add(arr[i]);
                  }
               }

               return conditionalWriteConf;

            }
         }
      }

      return null;

   }

   static class ConditionalWriteConf
   {
      String       expression;
      List<String> fields = new ArrayList<String>();
   }
}
