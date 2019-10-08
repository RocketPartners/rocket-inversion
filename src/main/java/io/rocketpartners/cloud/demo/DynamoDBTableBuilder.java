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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;

import io.rocketpartners.cloud.action.dynamo.DynamoDb;

/**
 * This simple demo launches an API that exposes SQL database tables 
 * as REST collection endpoints.  The demo supports full GET,PUT,POST,DELETE
 * operations with an extensive Resource Query Language (RQL) for GET
 * requests.
 * <br>  
 * The demo connects to an in memory H2 sql db that gets initialized from
 * scratch each time this demo is run.  That means you can fully explore
 * modifying operations (PUT,POST,DELETE) and 'break' whatever you want
 * then restart and have a clean demo app again.
 * <br>
 * If you want to explore your own JDBC DB, you can swap the "withDb()"
 * line below with the commented out one and fill in your connection info.
 * Currently, Inversion only ships with MySQL drivers out of the box but
 * has SQL syntax support for MySQL, SqlServer, and PostgreSQL.
 * <br>
 * Northwind is a demo db that has shipped with various Microsoft products
 * for years.  Some of its table designs seem strange or antiquated 
 * compared to modern conventions but it makes a great demo and test
 * specifically because it shows how Inversion can accommodate a broad
 * range of database design patterns.  
 * 
 * @see Demo1SqlDbNorthwind.ddl for more details on the db
 * @see https://github.com/RocketPartners/rocket-inversion for more information 
 *      on building awesome APIs with Inversion
 *  
 * @author wells
 *
 */
public class DynamoDBTableBuilder
{
   public static Table buildTable() throws Exception
   {
      List<AttributeDefinition> attrs = new ArrayList<>();

      attrs.add(new AttributeDefinition().withAttributeName("hk").withAttributeType("S"));
      attrs.add(new AttributeDefinition().withAttributeName("sk").withAttributeType("S"));

      List<KeySchemaElement> keys = new ArrayList<>();
      keys.add(new KeySchemaElement().withAttributeName("hk").withKeyType(KeyType.HASH));
      keys.add(new KeySchemaElement().withAttributeName("sk").withKeyType(KeyType.RANGE));

      List<GlobalSecondaryIndex> gsxs = new ArrayList();
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
                                                                  .withWriteCapacityUnits(5L));;
      }

      // TODO -> Where to add TTL attribute?
      TimeToLiveSpecification ttl = new TimeToLiveSpecification().withEnabled(true).withAttributeName("ttl");

      AmazonDynamoDB client = DynamoDb.buildDynamoClient("urls-and-otps");

      DynamoDB ddb = new DynamoDB(client);

      CreateTableRequest request = new CreateTableRequest()//
                                                           .withGlobalSecondaryIndexes(gsxs)//
                                                           .withTableName("urls-and-otps")//
                                                           .withKeySchema(keys)//
                                                           .withAttributeDefinitions(attrs)//
                                                           .withProvisionedThroughput(new ProvisionedThroughput()//
                                                                                                                 .withReadCapacityUnits(5L)//
                                                                                                                 .withWriteCapacityUnits(5L));

      // .withBillingMode("PAY_PER_REQUEST");

      Table table = ddb.createTable(request);

      try
      {
         table.waitForActive();
      }
      catch (Exception ex)
      {
         table.waitForActive();
      }

      return table;
   }
}
