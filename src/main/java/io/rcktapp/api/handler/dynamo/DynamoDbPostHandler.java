/**
 * 
 */
package io.rcktapp.api.handler.dynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

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
      Collection collection = findCollectionOrThrow404(api, chain, req);
      Table table = collection.getEntity().getTable();
      DynamoDb db = (DynamoDb) table.getDb();
      com.amazonaws.services.dynamodbv2.document.Table dynamoTable = db.getDynamoTable(table.getName());
      String pk = DynamoDb.findPartitionKeyName(table);
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
            putMapToDynamo((Map) obj, dynamoTable, pk, tenantIdOrCode, req.getApi().isMultiTenant(), appendTenantIdToPk, conditionalWriteConf);
         }
      }
      else if (payloadObj instanceof Map)
      {
         putMapToDynamo((Map) payloadObj, dynamoTable, pk, tenantIdOrCode, req.getApi().isMultiTenant(), appendTenantIdToPk, conditionalWriteConf);
      }

      res.setStatus(SC.SC_200_OK);

   }

   void putMapToDynamo(Map json, com.amazonaws.services.dynamodbv2.document.Table dynamoTable, String pk, Object tenantIdOrCode, boolean isMultiTenant, boolean appendTenantIdToPk, ConditionalWriteConf conditionalWriteConf)
   {
      try
      {

         Map m = new HashMap<>(json);

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

         Item item = Item.fromMap(m);

         PutItemSpec putItemSpec = new PutItemSpec().withItem(item);

         if (conditionalWriteConf != null)
         {
            putItemSpec = putItemSpec.withConditionExpression(conditionalWriteConf.expression);
            if (!conditionalWriteConf.fields.isEmpty())
            {
               Map<String, Object> valueMap = new HashMap<>();
               for (String field : conditionalWriteConf.fields)
               {
                  valueMap.put(":" + field, m.get(field));
               }

               putItemSpec = putItemSpec.withValueMap(valueMap);
            }
         }

         dynamoTable.putItem(putItemSpec);
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
