/*
 * Copyright (c) 2015-2019 Rocket Partners, LLC
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
package io.rocketpartners.cloud.demo;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

import io.rocketpartners.cloud.action.dynamo.DynamoDb;

/**
 *  
 */

public class DynamoDemo
{
   private String tableName;

   public DynamoDemo()
   {
      this.tableName = "inversion-demo";
   }

   public DynamoDemo(String tableName)
   {
      this.tableName = tableName;
   }

   public Table buildTable() throws Exception
   {
      List<AttributeDefinition> attrs = new ArrayList<>();

      attrs.add(new AttributeDefinition().withAttributeName("hk").withAttributeType("S"));
      attrs.add(new AttributeDefinition().withAttributeName("sk").withAttributeType("S"));

      List<KeySchemaElement> keys = new ArrayList<>();
      keys.add(new KeySchemaElement().withAttributeName("hk").withKeyType(KeyType.HASH));
      keys.add(new KeySchemaElement().withAttributeName("sk").withKeyType(KeyType.RANGE));

      List<GlobalSecondaryIndex> gsxs = new ArrayList<>();
      gsxs.add(new GlobalSecondaryIndex().withIndexName("gs1")//
                                         .withKeySchema(// 
                                               new KeySchemaElement()//
                                                                     .withAttributeName("sk").withKeyType(KeyType.HASH),
                                               new KeySchemaElement()//
                                                                     .withAttributeName("hk").withKeyType(KeyType.RANGE)));

      for (GlobalSecondaryIndex gsx : gsxs)
      {
         gsx.setProjection(new Projection().withProjectionType(ProjectionType.ALL));
         gsx.withProvisionedThroughput(new ProvisionedThroughput()//
                                                                  .withReadCapacityUnits(5L)//
                                                                  .withWriteCapacityUnits(5L));
      }

      AmazonDynamoDB client = DynamoDb.buildDynamoClient(tableName);

      DynamoDB ddb = new DynamoDB(client);

      CreateTableRequest request = new CreateTableRequest()//
                                                           .withGlobalSecondaryIndexes(gsxs)//
                                                           .withTableName(tableName)//
                                                           .withKeySchema(keys)//
                                                           .withAttributeDefinitions(attrs)//
                                                           .withProvisionedThroughput(new ProvisionedThroughput()//
                                                                                                                 .withReadCapacityUnits(5L)//
                                                                                                                 .withWriteCapacityUnits(5L));

      Table table = ddb.createTable(request);

      try
      {
         table.waitForActive();
      }
      // TODO -> Why is it handled like this?
      catch (Exception ex)
      {
         table.waitForActive();
      }
      return table;
   }

//   public void deleteTable() throws Exception
//   {
//      try
//      {
//         final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
//         ddb.deleteTable(tableName);
//      }
//
//      catch (AmazonServiceException e)
//      {
//         System.err.println(e.getErrorMessage());
//         System.exit(1);
//      }
//   }
}
