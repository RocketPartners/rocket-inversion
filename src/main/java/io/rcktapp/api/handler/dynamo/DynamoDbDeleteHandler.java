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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.ApiException;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;

/**
 * @author tc-rocket
 *
 */
public class DynamoDbDeleteHandler extends DynamoDbHandler
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
      String sk = dynamoIdx.getSortKey();
      boolean appendTenantIdToPk = isAppendTenantIdToPk(chain, collection.getName());

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
            deleteMapFromDynamo((Map) obj, dynamoClient, tableName, pk, sk, tenantIdOrCode, api.isMultiTenant(), appendTenantIdToPk);
         }
      }
      else if (payloadObj instanceof Map)
      {
         deleteMapFromDynamo((Map) payloadObj, dynamoClient, tableName, pk, sk, tenantIdOrCode, api.isMultiTenant(), appendTenantIdToPk);
      }

      res.setStatus(SC.SC_200_OK);

   }

   void deleteMapFromDynamo(Map json, DynamoDbClient dynamoClient, String tableName, String pk, String sk, Object tenantIdOrCode, boolean isMultiTenant, boolean appendTenantIdToPk)
   {
      try
      {
         Map m = new HashMap<>(json);

         if (!m.containsKey(pk) || (sk != null && !m.containsKey(sk)))
         {
            String msg = "The JSON body must contain a '" + pk + "' field";
            if (sk != null)
            {
               msg = msg + " and a '" + sk + "' field.";
            }

            throw new ApiException(SC.SC_400_BAD_REQUEST, msg);
         }

         String pkValue = (String) m.get(pk);
         if (isMultiTenant && appendTenantIdToPk)
         {
            pkValue = addTenantIdToKey(tenantIdOrCode, pkValue);
         }

         Map<String, AttributeValue> keyMap = new HashMap<>();
         keyMap.put(pk, AttributeValue.builder().s(pkValue).build());
         if (sk != null)
         {
            keyMap.put(sk, DynamoV2Utils.toAttributeValue(m.get(sk)));
         }

         DeleteItemRequest.Builder deleteBuilder = DeleteItemRequest.builder()
            .tableName(tableName)
            .key(keyMap);

         if (isMultiTenant)
         {
            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":val", DynamoV2Utils.toAttributeValue(tenantIdOrCode));
            deleteBuilder.conditionExpression("tenantid = :val")
                         .expressionAttributeValues(expressionValues);
         }

         dynamoClient.deleteItem(deleteBuilder.build());
      }
      catch (ConditionalCheckFailedException ccfe)
      {
         // catch this and do nothing.
         // this just means the that conditional check wasn't satisfied
         // so the record was not deleted
         // This is probably because the the record doesn't exist
      }

   }

}
