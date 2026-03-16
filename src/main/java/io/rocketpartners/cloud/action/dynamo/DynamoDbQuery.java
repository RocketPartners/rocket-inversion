/**
 *
 */
package io.rocketpartners.cloud.action.dynamo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rocketpartners.cloud.action.dynamo.DynamoDb.DynamoDbIndex;
import io.rocketpartners.cloud.model.ApiException;
import io.rocketpartners.cloud.model.Column;
import io.rocketpartners.cloud.model.Results;
import io.rocketpartners.cloud.model.SC;
import io.rocketpartners.cloud.model.Table;
import io.rocketpartners.cloud.rql.Group;
import io.rocketpartners.cloud.rql.Order;
import io.rocketpartners.cloud.rql.Page;
import io.rocketpartners.cloud.rql.Query;
import io.rocketpartners.cloud.rql.Select;
import io.rocketpartners.cloud.rql.Term;
import io.rocketpartners.cloud.rql.Where;
import io.rocketpartners.cloud.service.Chain;
import io.rocketpartners.cloud.utils.Rows.Row;
import io.rocketpartners.cloud.utils.Utils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * @author tc-rocket, wells
 *
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
 *
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
 *
 * @see https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryingJavaDocumentAPI.html
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#FilteringResults
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
 *
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.KeyConditions.html
 *
 *
 */
public class DynamoDbQuery extends Query<DynamoDbQuery, DynamoDb, Table, Select<Select<Select, DynamoDbQuery>, DynamoDbQuery>, Where<Where<Where, DynamoDbQuery>, DynamoDbQuery>, Group<Group<Group, DynamoDbQuery>, DynamoDbQuery>, Order<Order<Order, DynamoDbQuery>, DynamoDbQuery>, Page<Page<Page, DynamoDbQuery>, DynamoDbQuery>>
{

   public static Map<String, String> OPERATOR_MAP = new HashMap<>();
   public static Map<String, String> FUNCTION_MAP = new HashMap<>();
   static
   {
      OPERATOR_MAP.put("eq", "=");
      OPERATOR_MAP.put("ne", "<>");
      OPERATOR_MAP.put("gt", ">");
      OPERATOR_MAP.put("ge", ">=");
      OPERATOR_MAP.put("lt", "<");
      OPERATOR_MAP.put("le", "<=");

      FUNCTION_MAP.put("w", "contains");
      FUNCTION_MAP.put("sw", "begins_with");
      FUNCTION_MAP.put("attribute_not_exists", "attribute_not_exists");
      FUNCTION_MAP.put("attribute_exists", "attribute_exists");
   }

   DynamoDbClient dynamoClient = null;
   String         tableName    = null;
   DynamoDbIndex  index;

   Term           partKey      = null;
   Term           sortKey      = null;

   public DynamoDbQuery(Table table, List<Term> terms)
   {
      super(table);
      where().clearFunctions();

      where().withFunctions("_key", "eq", "ne", "gt", "ge", "lt", "le", "w", "sw", "nn", "n", "emp", "nemp", "in", "out", "and", "or", "not", "attribute_not_exists", "attribute_exists");
      super.withTerms(terms);
   }

   protected boolean addTerm(String token, Term term)
   {
      index = null;

      if (term.hasToken("n", "nn", "emp", "nemp"))
      {
         if (term.size() > 1)
            throw new ApiException(SC.SC_400_BAD_REQUEST, "The n() and nn() functions only take one column name arg.");

         if (term.hasToken("n", "emp"))
         {
            Term eqNull = Term.term(term.getParent(), "eq", term.getTerm(0), "null");
            Term attrNotExists = Term.term(null, "attribute_not_exists", term.getTerm(0));

            term = Term.term(term.getParent(),  "or",  attrNotExists, eqNull);
         }
         else if (term.hasToken("nn", "nemp"))
         {
            Term neNull = Term.term(term.getParent(), "ne", term.getTerm(0), "null");
            Term attrExists = Term.term(null, "attribute_exists", term.getTerm(0));
            term = Term.term(term.getParent(), "and", attrExists, neNull);
         }
      }
      if (term.hasToken("sw"))//sw (startswith) includes a implicit trailing wild card
      {
         String val = term.getTerm(1).getToken();
         while (val != null && val.endsWith("*"))
         {
            val = val.substring(0, val.length() - 1);
         }
         term.getTerm(1).withToken(val);
      }

      return super.addTerm(token, term);
   }

   public DynamoDbClient getDynamoClient()
   {
      return dynamoClient;
   }

   public DynamoDbQuery withDynamoClient(DynamoDbClient dynamoClient, String tableName)
   {
      this.dynamoClient = dynamoClient;
      this.tableName = tableName;
      return this;
   }

   protected Results<Row> doSelect() throws Exception
   {
      Results result = new Results(this);

      DynamoDbIndex index = getIndex();

      Map nameMap = new HashMap();
      Map valueMap = new HashMap();

      StringBuffer keyExpr = new StringBuffer("");
      StringBuffer filterExpr = new StringBuffer("");

      if (index != null && index.isPrimaryIndex() && partKey != null && sortKey != null && sortKey.hasToken("eq") && sortKey.getTerm(1).isLeaf())
      {
         // GetItem case
         String partKeyCol = partKey.getToken(0);
         String type = table.getColumn(partKeyCol).getType();
         Object partKeyVal = db().cast(type, partKey.getToken(1));

         String sortKeyCol = sortKey.getToken(0);
         Object sortKeyVal = db.cast(table.getColumn(sortKeyCol).getType(), sortKey.getToken(1));

         Chain.debug("DynamoDb  GetItemSpec partKeyCol=" + partKeyCol + " partKeyVal=" + partKeyVal + " sortKeyCol=" + sortKeyCol + " sortKeyVal=" + sortKeyVal);

         Map<String, AttributeValue> keyMap = new HashMap<>();
         keyMap.put(partKeyCol, DynamoV2Utils.toAttributeValue(partKeyVal));
         keyMap.put(sortKeyCol, DynamoV2Utils.toAttributeValue(sortKeyVal));

         GetItemResponse getResponse = dynamoClient.getItem(GetItemRequest.builder()
            .tableName(tableName)
            .key(keyMap)
            .build());

         if (getResponse.hasItem() && !getResponse.item().isEmpty())
         {
            result.withRow(DynamoV2Utils.fromItemMap(getResponse.item()));
         }

         return result;
      }

      if (partKey != null)
      {
         toString(keyExpr, partKey, nameMap, valueMap);
      }

      if (sortKey != null)
      {
         toString(keyExpr, sortKey, nameMap, valueMap);
      }

      for (Term term : where().getTerms())
      {
         if (term == partKey || term == sortKey)
            continue;
         toString(filterExpr, term, nameMap, valueMap);
      }

      boolean doQuery = partKey != null && partKey.getTerm(1).isLeaf();

      StringBuffer debug = new StringBuffer("DynamoDb  ").append(doQuery ? "QuerySpec" : "ScanSpec").append(index != null ? ":'" + index.getName() + "'" : "");

      int pageSize = page().getPageSize();
      debug.append(" maxPageSize=" + pageSize);

      boolean scanIndexForward = order().isAsc(0);
      debug.append(" scanIndexForward=" + scanIndexForward);

      debug.append(" nameMap=" + nameMap + " valueMap=" + valueMap);
      debug.append(" keyConditionExpression='" + keyExpr + "'");
      debug.append(" filterExpression='" + filterExpr + "'");

      String projectionExpression = null;
      List columns = select().getColumnNames();
      if (columns.size() > 0)
         projectionExpression = Utils.implode(",", columns);

      debug.append(" projectionExpression='" + (projectionExpression != null ? projectionExpression : "") + "'");

      Chain.debug(debug);

      // Convert valueMap from raw objects to AttributeValue
      Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
      for (Object entry : valueMap.entrySet())
      {
         Map.Entry<String, Object> e = (Map.Entry<String, Object>) entry;
         expressionAttributeValues.put(e.getKey(), DynamoV2Utils.toAttributeValue(e.getValue()));
      }

      if (doQuery)
      {
         QueryRequest.Builder queryBuilder = QueryRequest.builder()
            .tableName(tableName)
            .limit(pageSize)
            .scanIndexForward(order().isAsc(0));

         if (index != null && !index.isPrimaryIndex())
         {
            queryBuilder.indexName(index.getName());
         }

         Term after = page().getAfter();
         if (after != null)
         {
            Column afterHashKeyCol = table().getColumn(after.getToken(0));
            Column afterSortKeyCol = after.size() > 2 ? table().getColumn(after.getToken(2)) : null;

            if (afterHashKeyCol == null || (after.size() > 2 && afterSortKeyCol == null))
               throw new ApiException(SC.SC_400_BAD_REQUEST, "Invalid column in 'after' key: " + after);

            Object hashValue = db.cast(afterHashKeyCol, after.getToken(1));
            Object sortValue = afterSortKeyCol != null ? db.cast(afterSortKeyCol, after.getToken(3)) : null;

            Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();
            exclusiveStartKey.put(afterHashKeyCol.getName(), DynamoV2Utils.toAttributeValue(hashValue));
            if (afterSortKeyCol != null)
            {
               exclusiveStartKey.put(afterSortKeyCol.getName(), DynamoV2Utils.toAttributeValue(sortValue));
            }
            queryBuilder.exclusiveStartKey(exclusiveStartKey);
         }

         if (!Utils.empty(projectionExpression))
         {
            queryBuilder.projectionExpression(projectionExpression);
         }

         if (keyExpr.length() > 0)
         {
            queryBuilder.keyConditionExpression(keyExpr.toString());
         }

         if (filterExpr.length() > 0)
         {
            queryBuilder.filterExpression(filterExpr.toString());
         }

         if (nameMap.size() > 0)
         {
            queryBuilder.expressionAttributeNames(nameMap);
         }

         if (expressionAttributeValues.size() > 0)
         {
            queryBuilder.expressionAttributeValues(expressionAttributeValues);
         }

         // Client-side pagination loop to match v1 Document API maxResultSize behavior.
         // SDK v2 limit only caps items evaluated per call, not total results returned.
         Map<String, AttributeValue> lastEvaluatedKey = null;
         int totalCollected = 0;

         do
         {
            QueryResponse queryResponse = dynamoClient.query(queryBuilder.build());

            if (queryResponse.hasItems())
            {
               for (Map<String, AttributeValue> item : queryResponse.items())
               {
                  result.withRow(DynamoV2Utils.fromItemMap(item));
                  totalCollected++;
               }
            }

            lastEvaluatedKey = queryResponse.lastEvaluatedKey();

            if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty() && totalCollected < pageSize)
            {
               queryBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            else
            {
               break;
            }
         }
         while (true);

         result.withNext(after(index, lastEvaluatedKey));
      }
      else
      {
         ScanRequest.Builder scanBuilder = ScanRequest.builder()
            .tableName(tableName)
            .limit(pageSize);

         if (index != null && !index.isPrimaryIndex())
         {
            scanBuilder.indexName(index.getName());
         }

         Term after = page().getAfter();
         if (after != null)
         {
            Column afterHashKeyCol = table().getColumn(after.getToken(0));
            Column afterSortKeyCol = after.size() > 2 ? table().getColumn(after.getToken(2)) : null;

            if (afterHashKeyCol == null || (after.size() > 2 && afterSortKeyCol == null))
               throw new ApiException(SC.SC_400_BAD_REQUEST, "Invalid column in 'after' key: " + after);

            Object hashValue = db.cast(afterHashKeyCol, after.getToken(1));
            Object sortValue = afterSortKeyCol != null ? db.cast(afterSortKeyCol, after.getToken(3)) : null;

            Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();
            exclusiveStartKey.put(afterHashKeyCol.getName(), DynamoV2Utils.toAttributeValue(hashValue));
            if (afterSortKeyCol != null)
            {
               exclusiveStartKey.put(afterSortKeyCol.getName(), DynamoV2Utils.toAttributeValue(sortValue));
            }
            scanBuilder.exclusiveStartKey(exclusiveStartKey);
         }

         if (!Utils.empty(projectionExpression))
         {
            scanBuilder.projectionExpression(projectionExpression);
         }

         if (filterExpr.length() > 0)
         {
            scanBuilder.filterExpression(filterExpr.toString());
         }
         if (nameMap.size() > 0)
         {
            scanBuilder.expressionAttributeNames(nameMap);
         }

         if (expressionAttributeValues.size() > 0)
         {
            scanBuilder.expressionAttributeValues(expressionAttributeValues);
         }

         // Client-side pagination loop to match v1 Document API maxResultSize behavior.
         Map<String, AttributeValue> lastEvaluatedKey = null;
         int totalCollected = 0;

         do
         {
            ScanResponse scanResponse = dynamoClient.scan(scanBuilder.build());

            if (scanResponse.hasItems())
            {
               for (Map<String, AttributeValue> item : scanResponse.items())
               {
                  result.withRow(DynamoV2Utils.fromItemMap(item));
                  totalCollected++;
               }
            }

            lastEvaluatedKey = scanResponse.lastEvaluatedKey();

            if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty() && totalCollected < pageSize)
            {
               scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            else
            {
               break;
            }
         }
         while (true);

         result.withNext(after(index, lastEvaluatedKey));
      }

      return result;
   }

   protected List<Term> after(DynamoDbIndex index, java.util.Map<String, AttributeValue> attrs)
   {
      if (attrs == null || attrs.isEmpty())
         return Collections.EMPTY_LIST;

      Term after = Term.term(null, "after");

      if (index != null)
      {
         after.withTerm(Term.term(after, index.getHashKeyName()));
         after.withTerm(Term.term(after, getValue(attrs.get(index.getHashKeyName())).toString()));

         if (index.getSortKey() != null)
         {
            after.withTerm(Term.term(after, index.getSortKeyName()));
            after.withTerm(Term.term(after, getValue(attrs.get(index.getSortKeyName())).toString()));
         }
      }
      else
      {
         for (String key : attrs.keySet())
         {
            after.withTerm(Term.term(after, key));
            after.withTerm(Term.term(after, getValue(attrs.get(key)).toString()));
         }
      }
      return Arrays.asList(after);
   }

   protected Object getValue(AttributeValue v)
   {
      switch (v.type())
      {
         case S:    return v.s();
         case N:    return v.n();
         case B:    return v.b();
         case SS:   return v.ss();
         case NS:   return v.ns();
         case BS:   return v.bs();
         case M:    return v.m();
         case L:    return v.l();
         case NUL:  return v.nul();
         case BOOL: return v.bool();
         default:
            throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Unable to get value from AttributeValue: " + v);
      }
   }

   /**
    * @return the best fit index to use for the query based on the params supplied
    */
   public DynamoDbIndex getIndex()
   {
      //if the users requested a sort, you need to find an index with that sort k      String sortBy = order.getProperty(0);
      String sortBy = order.getProperty(0);

      Term after = page().getAfter();
      if (after != null)
      {
         Column afterHashKeyCol = table().getColumn(after.getToken(0));
         Column afterSortKeyCol = after.size() > 2 ? table().getColumn(after.getToken(2)) : null;

         for (io.rocketpartners.cloud.model.Index idx : table.getIndexes())
         {
            DynamoDbIndex didx = (DynamoDbIndex) idx;
            if (didx.getHashKey() == afterHashKeyCol && didx.getSortKey() == afterSortKeyCol)
            {
               index = didx;

               partKey = findTerm(afterHashKeyCol.getName(), "eq");

               if (partKey == null)
                  continue;

               if (afterSortKeyCol != null)
               {
                  sortKey = findTerm(afterSortKeyCol.getName(), "eq");
                  if (sortKey == null)
                     sortKey = findTerm(afterSortKeyCol.getName(), "gt", "ne", "gt", "ge", "lt", "le", "w", "sw", "nn", "n", "emp", "nemp", "in", "out");
               }

               break;
            }
         }

         if (sortBy != null && (afterSortKeyCol == null || !sortBy.equalsIgnoreCase(afterSortKeyCol.getName())))
         {
            //TODO make test
            throw new ApiException("The requested sort key does not match the supplied 'after' continuation token.");
         }
      }

      if (index == null)
      {
         DynamoDbIndex foundIndex = null;
         Term foundPartKey = null;
         Term foundSortKey = null;

         for (io.rocketpartners.cloud.model.Index idx : table().getIndexes())
         {
            DynamoDbIndex index = (DynamoDbIndex) idx;

            String partCol = index.getHashKey().getName();
            String sortCol = index.getSortKey() != null ? index.getSortKey().getName() : null;

            if (sortBy != null && !sortBy.equalsIgnoreCase(sortCol))
               continue; //incompatible index. if a sort was requested, can't choose an index that has a different sort

            Term partKey = findTerm(partCol, "eq");

            if (partKey == null && sortBy == null)
               continue;

            Term sortKey = findTerm(sortCol, "eq");

            if (sortKey == null)
               sortKey = findTerm(sortCol, "gt", "ne", "gt", "ge", "lt", "le", "w", "sw", "nn", "n", "emp", "nemp", "in", "out");

            boolean use = false;
            if (foundPartKey == null && partKey != null)
               use = true;

            else if (sortKey == null && foundSortKey != null)
               use = false; //if you already have an index with a sort key match, don't replace it

            else if (foundIndex == null //
                  || (sortKey != null && foundSortKey == null) //
                  || (sortKey != null && sortKey.hasToken("eq") && !foundSortKey.hasToken("eq"))) //the new sort key has an equality condition
               use = true;

            if (use)
            {
               foundIndex = index;
               foundPartKey = partKey;
               foundSortKey = sortKey;
            }
         }

         if (sortBy != null && foundIndex == null)
         {
            //TODO: create test case to trigger this exception
            throw new ApiException(SC.SC_400_BAD_REQUEST, "Unable to find valid index to query.  The requested sort field '" + sortBy + " must be the sort key of the primary index, the sort key of a global secondary index, or a local secondary secondary index.");
         }

         if (foundPartKey == null && sortBy != null && !order.isAsc(0))
         {
            //an inverse/descending sort can only be run on a QuerySpec which requires a partition key.
            throw new ApiException(SC.SC_400_BAD_REQUEST, "Unable to find valid index to query.  A descending sort on '" + sortBy + " is only possible when a partition key value is supplied.");
         }

         this.index = foundIndex;
         this.partKey = foundPartKey;
         this.sortKey = foundSortKey;
      }
      return index;
   }

   /**
    * Finds the primary or a secondary index to use based on
    * what parameters were passed in.
    */
   public Term getPartKey()
   {
      if (index == null)
      {
         getIndex();
      }
      return partKey;
   }

   public Term getSortKey()
   {
      if (index == null)
      {
         getIndex();
      }
      return sortKey;
   }

   String toString(StringBuffer buff, Term term, Map nameMap, Map valueMap)
   {
      space(buff);

      String lc = term.getToken().toLowerCase();
      String op = OPERATOR_MAP.get(lc);
      String func = FUNCTION_MAP.get(lc);

      if (term.hasToken("not"))
      {
         if (buff.length() > 0)
            space(buff).append("and ");

         buff.append("(NOT ").append(toString(new StringBuffer(""), term.getTerm(1), nameMap, valueMap)).append(")");
      }
      else if (term.hasToken("and", "or"))
      {
         buff.append("(");
         for (int i = 0; i < term.getNumTerms(); i++)
         {
            buff.append(toString(new StringBuffer(""), term.getTerm(i), nameMap, valueMap));
            if (i < term.getNumTerms() - 1)
               space(buff).append(term.getToken()).append(" ");
         }
         buff.append(")");
      }
      else if (term.hasToken("in", "out"))
      {
         String col = term.getToken(0);
         String nameKey = "#var" + (nameMap.size() + 1);
         nameMap.put(nameKey, col);

         if (buff.length() > 0)
            space(buff).append("and ");

         buff.append("(");
         buff.append(term.hasToken("out") ? "NOT " : "");
         buff.append(nameKey).append(" IN (");
         for (int i = 1; i < term.size(); i++)
         {
            if (i > 1)
               buff.append(", ");

            buff.append(toString(new StringBuffer(""), term.getTerm(i), nameMap, valueMap));

         }
         buff.append("))");
      }
      else if (op != null)
      {
         String col = term.getToken(0);

         String nameKey = "#var" + (nameMap.size() + 1);
         nameMap.put(nameKey, col);

         String expr = toString(new StringBuffer(""), term.getTerm(1), nameMap, valueMap);

         if (buff.length() > 0)
            space(buff).append("and ");

         buff.append("(").append(nameKey).append(" ").append(op).append(" ").append(expr).append(")");
      }
      else if (func != null)
      {
         if (buff.length() > 0)
            space(buff).append("and ");

         String col = term.getToken(0);

         String nameKey = "#var" + (nameMap.size() + 1);
         nameMap.put(nameKey, col);

         if (term.size() > 1)
         {
            String expr = toString(new StringBuffer(""), term.getTerm(1), nameMap, valueMap);
            space(buff).append(func).append("(").append(nameKey).append(",").append(expr).append(")");
         }
         else
         {
            space(buff).append(func).append("(").append(nameKey).append(")");
         }
      }
      else if (term.isLeaf())
      {
         String colName = term.getParent().getToken(0);

         Object value = term.getToken();
         Column col = table.getColumn(colName);
         value = db.cast(col, term.getToken());

         if ("null".equalsIgnoreCase(value + ""))
            value = null;

         String key = ":val" + (valueMap.size() + 1);
         valueMap.put(key, value);

         space(buff).append(key);
      }

      return buff.toString();
   }

   StringBuffer space(StringBuffer buff)
   {
      if (buff.length() > 0 && buff.charAt(buff.length() - 1) != ' ')
         buff.append(' ');

      return buff;
   }

}
