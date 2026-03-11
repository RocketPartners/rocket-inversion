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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Request;
import io.rcktapp.api.Response;
import io.rcktapp.api.Table;
import io.rcktapp.api.service.Service;
import io.rcktapp.rql.Order;
import io.rcktapp.rql.Predicate;
import io.rcktapp.rql.Rql;
import io.rcktapp.rql.dynamo.DynamoExpression;
import io.rcktapp.rql.dynamo.DynamoRql;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * @author tc-rocket
 *
 * Endpoint/Action Config
 *  - appendTenantIdToPk :        Enables appending the tenant id to the primary key
 *                                FORMAT: collection name (comma separated)
 *
 */
public class DynamoDbGetHandler extends DynamoDbHandler
{

   protected String nextKeyDelimeter = "~";

   @Override
   public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {
      Collection collection = api.getCollection(req.getCollectionKey(), DynamoDb.class);
      Table table = collection.getEntity().getTable();
      DynamoDb db = (DynamoDb) table.getDb();
      DynamoDbClient dynamoClient = db.getDynamoDbClient();
      String tableName = table.getName();

      String tenantIdOrCode = null;
      if (req.getApi().isMultiTenant())
      {
         tenantIdOrCode = req.removeParam("tenantId");
         if (tenantIdOrCode == null)
         {
            tenantIdOrCode = req.getTenantCode();
         }
      }

      int pageSize = req.getParam("pagesize") != null ? Integer.parseInt(req.removeParam("pagesize")) : 100;
      String next = req.removeParam("next");

      Set<String> includes = new HashSet<>(splitToList(req.removeParam("includes")));
      Set<String> excludes = new HashSet<>(splitToList(req.removeParam("excludes")));

      DynamoRql rql = (DynamoRql) Rql.getRql(db.getType());
      DynamoExpression dynamoExpression = rql.buildDynamoExpression(req.getParams(), table);
      Order order = dynamoExpression.getOrder();

      DynamoIndex dynamoIdx = dynamoExpression.getIndex();
      String pk = dynamoIdx.getPartitionKey();
      String sk = dynamoIdx.getSortKey();
      boolean appendTenantIdToPk = !dynamoIdx.getType().equals(DynamoDb.GLOBAL_SECONDARY_TYPE) ? isAppendTenantIdToPk(chain, collection.getName()) : false;

      if (chain.getRequest().isDebug())
      {
         res.debug("Dynamo Table:       " + table.getName() + ", PK: " + pk + ", SK: " + sk);

         List<DynamoIndex> lsIndexes = DynamoDb.findIndexesByType(table, DynamoDb.GLOBAL_SECONDARY_TYPE);

         if (!lsIndexes.isEmpty())
         {
            res.debug("Global Sec Indexes:  " + lsIndexes.stream().map(i -> i.getName()).collect(Collectors.joining(",")));
         }

         lsIndexes = DynamoDb.findIndexesByType(table, DynamoDb.LOCAL_SECONDARY_TYPE);

         if (!lsIndexes.isEmpty())
         {
            res.debug("Local Sec Indexes:  " + lsIndexes.stream().map(i -> i.getName()).collect(Collectors.joining(",")));
         }

      }

      Map<String, AttributeValue> exclusiveStartKey = null;

      if (next != null)
      {
         exclusiveStartKey = new HashMap<>();
         String[] sArr = next.split(nextKeyDelimeter);
         String nextPkVal = sArr[0];
         if (api.isMultiTenant() && appendTenantIdToPk)
         {
            nextPkVal = addTenantIdToKey(tenantIdOrCode, nextPkVal);
         }
         exclusiveStartKey.put(pk, AttributeValue.builder().s(nextPkVal).build());

         if (sArr.length > 1)
         {
            Object castVal = DynamoDb.cast(sArr[1], sk, table);
            exclusiveStartKey.put(sk, DynamoV2Utils.toAttributeValue(castVal));
         }

         if (sArr.length > 2 && order != null && !sk.equals(order.col))
         {
            Object castVal = DynamoDb.cast(sArr[2], order.col, table);
            exclusiveStartKey.put(order.col, DynamoV2Utils.toAttributeValue(castVal));
         }
      }

      Object partitionKeyValue = null;
      Predicate pkPred = dynamoExpression.getExcludedPredicate(pk);
      if (pkPred != null)
      {
         partitionKeyValue = pkPred.getTerms().get(1).getToken();
         if (api.isMultiTenant() && appendTenantIdToPk)
         {
            partitionKeyValue = addTenantIdToKey(tenantIdOrCode, partitionKeyValue.toString());
         }
         else
            partitionKeyValue = DynamoDb.cast(pkPred.getTerms().get(1).getToken(), pk, table);
      }

      DynamoResult dynamoResult = null;
      if (partitionKeyValue != null)
      {
         // Query
         dynamoResult = doQuery(dynamoExpression, dynamoClient, tableName, chain, res, pageSize, exclusiveStartKey, pk, partitionKeyValue);
      }
      else
      {
         // Scan
         dynamoResult = doScan(dynamoExpression, dynamoClient, tableName, chain, res, pageSize, exclusiveStartKey);
      }

      String returnNext = null;
      if (dynamoResult != null && dynamoResult.lastKey != null && !dynamoResult.lastKey.isEmpty())
      {
         returnNext = dynamoResult.lastKey.get(pk).s();
         if (api.isMultiTenant() && appendTenantIdToPk)
         {
            returnNext = removeTenantIdFromKey(tenantIdOrCode, returnNext);
         }

         if (dynamoResult.lastKey.get(sk) != null)
         {
            String sortKeyVal = DynamoDb.attributeValueAsString(dynamoResult.lastKey.get(sk), sk, table);
            returnNext = returnNext + nextKeyDelimeter + sortKeyVal;
         }

         if (order != null && !order.col.equals(sk) && dynamoResult.lastKey.get(order.col) != null)
         {
            String sortKeyVal = DynamoDb.attributeValueAsString(dynamoResult.lastKey.get(order.col), order.col, table);
            returnNext = returnNext + nextKeyDelimeter + sortKeyVal;
         }

      }

      JSArray returnData = new JSArray();
      if (dynamoResult != null && !dynamoResult.items.isEmpty())
      {
         for (Map map : dynamoResult.items)
         {
            if (api.isMultiTenant() && appendTenantIdToPk)
            {
               String pkValue = (String) map.get(pk);
               map.put(pk, removeTenantIdFromKey(tenantIdOrCode, pkValue));
            }

            returnData.add(new JSObject(includeExclude(map, includes, excludes)));
         }
      }

      JSObject meta = new JSObject("pageSize", pageSize, "results", returnData.asList().size());
      if (returnNext != null)
      {
         meta.put("next", returnNext);
      }
      JSObject wrapper = new JSObject("meta", meta, "data", returnData);
      res.setJson(wrapper);

   }

   DynamoResult doQuery(DynamoExpression dynamoExpression, DynamoDbClient dynamoClient, String tableName, Chain chain, Response res, int pageSize, Map<String, AttributeValue> exclusiveStartKey, String pk, Object primaryKeyValue)
   {
      String filterExpressionStr = dynamoExpression.buildExpression();
      Order order = dynamoExpression.getOrder();
      String orderCol = order != null ? order.col : "";
      String orderDir = order != null ? order.dir : null;

      if (chain.getRequest().isDebug())
      {
         res.debug("Query Type:         Query");
         res.debug("Partition Key:      " + pk + " = " + primaryKeyValue);
      }

      // Build key condition expression
      Map<String, String> expressionNames = new HashMap<>();
      Map<String, AttributeValue> expressionValues = new HashMap<>();

      expressionNames.put("#pk", pk);
      expressionValues.put(":pkval", DynamoV2Utils.toAttributeValue(primaryKeyValue));
      String keyConditionExpression = "#pk = :pkval";

      Predicate skPred = dynamoExpression.getExcludedPredicate(orderCol);
      if (skPred != null)
      {
         KeyConditionFragment kcf = DynamoDb.predicateToKeyConditionFragment(skPred, dynamoExpression.getTable());
         keyConditionExpression = keyConditionExpression + " AND " + kcf.getExpression();
         expressionNames.put(kcf.getNameKey(), kcf.getNameValue());
         expressionValues.put(kcf.getValueKey(), DynamoV2Utils.toAttributeValue(kcf.getValueObject()));

         if (chain.getRequest().isDebug())
         {
            res.debug("Sort Key:           " + kcf.getNameValue() + " " + kcf.getExpression() + " " + kcf.getValueObject());
         }
      }

      QueryRequest.Builder queryBuilder = QueryRequest.builder()
         .tableName(tableName)
         .keyConditionExpression(keyConditionExpression)
         .expressionAttributeNames(expressionNames)
         .expressionAttributeValues(expressionValues)
         .limit(pageSize);

      if (dynamoExpression.getIndex() != null && !dynamoExpression.getIndex().getType().equals(DynamoDb.PRIMARY_TYPE))
      {
         queryBuilder.indexName(dynamoExpression.getIndex().getName());
         if (chain.getRequest().isDebug())
         {
            res.debug("Index:              " + dynamoExpression.getIndex().getName());
         }
      }

      if (orderDir != null)
      {
         boolean scanForward = !orderDir.equalsIgnoreCase("DESC");
         queryBuilder.scanIndexForward(scanForward);
         if (chain.getRequest().isDebug())
         {
            res.debug("Sorting By:         " + orderCol + " " + orderDir);
         }
      }

      if (exclusiveStartKey != null)
      {
         queryBuilder.exclusiveStartKey(exclusiveStartKey);
      }

      if (!dynamoExpression.getFields().isEmpty())
      {
         // Merge filter expression attribute names with key condition names
         Map<String, String> mergedNames = new HashMap<>(expressionNames);
         mergedNames.putAll(dynamoExpression.getFields());
         queryBuilder.expressionAttributeNames(mergedNames);

         // Merge filter expression attribute values with key condition values
         if (!dynamoExpression.getArgs().isEmpty())
         {
            Map<String, AttributeValue> mergedValues = new HashMap<>(expressionValues);
            mergedValues.putAll(DynamoV2Utils.toExpressionAttributeValues(dynamoExpression.getArgs()));
            queryBuilder.expressionAttributeValues(mergedValues);
         }

         queryBuilder.filterExpression(filterExpressionStr);

         if (chain.getRequest().isDebug())
         {
            res.debug("Filter:");
            res.debug(filterExpressionStr);
            res.debug(dynamoExpression.getFields());
            res.debug(filterArgsToString(dynamoExpression.getArgs()));
         }
      }

      QueryResponse queryResponse = dynamoClient.query(queryBuilder.build());

      List<Map> items = new ArrayList<>();
      Map<String, AttributeValue> lastKey = null;

      if (queryResponse.hasItems())
      {
         for (Map<String, AttributeValue> item : queryResponse.items())
         {
            items.add(DynamoV2Utils.fromItemMap(item));
         }
      }

      if (queryResponse.lastEvaluatedKey() != null && !queryResponse.lastEvaluatedKey().isEmpty())
      {
         lastKey = queryResponse.lastEvaluatedKey();
      }

      return new DynamoResult(items, lastKey);

   }

   DynamoResult doScan(DynamoExpression dynamoExpression, DynamoDbClient dynamoClient, String tableName, Chain chain, Response res, int pageSize, Map<String, AttributeValue> exclusiveStartKey)
   {
      String expressionStr = dynamoExpression.buildExpression();

      if (chain.getRequest().isDebug())
      {
         res.debug("Query Type:         Scan");
      }

      ScanRequest.Builder scanBuilder = ScanRequest.builder()
         .tableName(tableName)
         .limit(pageSize);

      if (!dynamoExpression.getFields().isEmpty())
      {
         scanBuilder.filterExpression(expressionStr)
                    .expressionAttributeNames(dynamoExpression.getFields());

         if (!dynamoExpression.getArgs().isEmpty())
         {
            scanBuilder.expressionAttributeValues(DynamoV2Utils.toExpressionAttributeValues(dynamoExpression.getArgs()));
         }

         if (chain.getRequest().isDebug())
         {
            res.debug("Filter:");
            res.debug(expressionStr);
            res.debug(dynamoExpression.getFields());
            res.debug(filterArgsToString(dynamoExpression.getArgs()));
         }
      }

      if (exclusiveStartKey != null)
      {
         scanBuilder.exclusiveStartKey(exclusiveStartKey);
      }

      ScanResponse scanResponse = dynamoClient.scan(scanBuilder.build());

      List<Map> items = new ArrayList<>();
      Map<String, AttributeValue> lastKey = null;

      if (scanResponse.hasItems())
      {
         for (Map<String, AttributeValue> item : scanResponse.items())
         {
            items.add(DynamoV2Utils.fromItemMap(item));
         }
      }

      if (scanResponse.lastEvaluatedKey() != null && !scanResponse.lastEvaluatedKey().isEmpty())
      {
         lastKey = scanResponse.lastEvaluatedKey();
      }

      return new DynamoResult(items, lastKey);

   }

   String filterArgsToString(Map<String, Object> args)
   {
      if (args != null)
      {
         String s = "{";
         int cnt = 0;
         for (String k : args.keySet())
         {
            s = s + k + "=" + args.get(k) + " (" + DynamoDb.getTypeStringFromObject(args.get(k)) + ")";
            if (cnt < args.keySet().size() - 1)
            {
               s = s + ", ";
            }

            cnt++;
         }
         s = s + "}";
         return s;
      }
      return "null";
   }

   Map includeExclude(Map m, Set<String> includes, Set<String> excludes)
   {
      if (m != null)
      {
         if (!includes.isEmpty())
         {
            Map newMap = new HashMap<>();
            for (String include : includes)
            {
               if (m.containsKey(include))
               {
                  newMap.put(include, m.get(include));
               }
            }
            m = newMap;
         }
         else if (!excludes.isEmpty())
         {
            for (String exclude : excludes)
            {
               m.remove(exclude);
            }
         }
      }
      return m;
   }

   public void setNextKeyDelimeter(String nextKeyDelimeter)
   {
      this.nextKeyDelimeter = nextKeyDelimeter;
   }

   static class DynamoResult
   {
      List<Map>                   items;
      Map<String, AttributeValue> lastKey;

      public DynamoResult(List<Map> items, Map<String, AttributeValue> lastKey)
      {
         super();
         this.items = items;
         this.lastKey = lastKey;
      }

      public List<Map> getItems()
      {
         return items;
      }

      public Map<String, AttributeValue> getLastKey()
      {
         return lastKey;
      }

   }

}
