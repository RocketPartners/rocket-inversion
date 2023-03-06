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
package io.rocketpartners.cloud.action.elastic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.rocketpartners.cloud.action.elastic.v03x.dsl.BoolQuery;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.ElasticQuery;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.ElasticRql;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.NestedQuery;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.Order;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.QueryDsl;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.Range;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.Term;
import io.rocketpartners.cloud.action.elastic.v03x.dsl.Wildcard;
import io.rocketpartners.cloud.action.elastic.v03x.rql.Rql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author kfrankic
 *
 */
public class TestElasticRql
{
   private static Logger log = LoggerFactory.getLogger(TestElasticRql.class);

   static
   {
      try
      {
         Class.forName(ElasticRql.class.getName());
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
      }
   }

   @Test
   public void testMapper() throws Exception
   {
      // This test is an attempt to take class attributes(BCD) and set them all 
      // on another attribute(A), as though those that attribute(A) is an object 
      // made up of those attributes(BCD).  attribute.
      ObjectMapper mapper = new ObjectMapper();
      Range range = new Range("testRange");
      range.setGt(25);
      String json = mapper.writeValueAsString(range);

      assertNotNull("json should not be empty.", json);
      assertEquals("{\"testRange\":{\"gt\":25}}", json);

   }

   @Test
   public void startsWith()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("sw(city,Chand,Atl)"));

         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool()); // A bool is used because comma separated values are valid for all 'with' functions

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(2, dsl.getBool().getShould().size());
         assertTrue(dsl.getBool().getShould().get(0) instanceof Wildcard);

         Wildcard wildcard = (Wildcard) dsl.getBool().getShould().get(0);

         assertEquals("city", wildcard.getName());
         assertEquals("Chand*", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"wildcard\":{\"city\":\"Chand*\"}},{\"wildcard\":{\"city\":\"Atl*\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void with()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("w(city,andl)"));

         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool()); // A bool is used because comma separated values are valid for all 'with' functions

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(1, dsl.getBool().getShould().size());
         assertTrue(dsl.getBool().getShould().get(0) instanceof Wildcard);

         Wildcard wildcard = (Wildcard) dsl.getBool().getShould().get(0);

         assertEquals("city", wildcard.getName());
         assertEquals("*andl*", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull(json, "json should not be empty.");
         assertEquals("{\"bool\":{\"should\":[{\"wildcard\":{\"city\":\"*andl*\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void with01()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("w(name,'nestl','f\\'')"));
         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool());  // A bool is used because comma separated values are valid for all 'with' functions

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(2, dsl.getBool().getShould().size());
         assertTrue(dsl.getBool().getShould().get(0) instanceof Wildcard);

         Wildcard wildcard = (Wildcard)dsl.getBool().getShould().get(0);

         assertEquals("name", wildcard.getName());
         assertEquals("*nestl*", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         wildcard = (Wildcard)dsl.getBool().getShould().get(1);

         assertEquals("name", wildcard.getName());
         assertEquals("*f'*", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"wildcard\":{\"name\":\"*nestl*\"}},{\"wildcard\":{\"name\":\"*f'*\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void without()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("wo(city,h)"));

         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool());

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getShould());
         assertNull(dsl.getBool().getMust());
         assertNotNull(dsl.getBool().getMustNot());

         assertEquals(1, dsl.getBool().getMustNot().size());
         assertEquals(Wildcard.class, dsl.getBool().getMustNot().get(0).getClass());
         assertEquals("city", ((Wildcard) dsl.getBool().getMustNot().get(0)).getName());
         assertEquals("*h*", ((Wildcard) dsl.getBool().getMustNot().get(0)).getValue());
         assertNull(((Wildcard) dsl.getBool().getMustNot().get(0)).getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must_not\":[{\"wildcard\":{\"city\":\"*h*\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void endsWith()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("ew(city,andler)"));

         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool()); // A bool is used because comma separated values are valid for all 'with' functions

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(1, dsl.getBool().getShould().size());
         assertTrue(dsl.getBool().getShould().get(0) instanceof Wildcard);

         Wildcard wildcard = (Wildcard) dsl.getBool().getShould().get(0);

         assertEquals("city", wildcard.getName());
         assertEquals("*andler", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"wildcard\":{\"city\":\"*andler\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void contains()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("w(city,andl)"));

         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool()); // A bool is used because comma separated values are valid for all 'with' functions

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(1, dsl.getBool().getShould().size());
         assertTrue(dsl.getBool().getShould().get(0) instanceof Wildcard);

         Wildcard wildcard = (Wildcard) dsl.getBool().getShould().get(0);

         assertEquals("city", wildcard.getName());
         assertEquals("*andl*", wildcard.getValue());
         assertNull(wildcard.getNestedPath());

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"wildcard\":{\"city\":\"*andl*\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void empty()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("emp(state)"));

         // TODO update this test

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"term\":{\"state\":\"\"}},{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"state\"}}]}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void fuzzySearch()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("search(keywords,Tim)"));

         // TODO update this test

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"fuzzy\":{\"keywords\":\"Tim\"}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void notEmpty()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("nemp(state)"));

         // TODO update this test

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must\":[{\"bool\":{\"must_not\":[{\"term\":{\"state\":\"\"}}]}},{\"bool\":{\"must\":[{\"exists\":{\"field\":\"state\"}}]}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void notNull()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("nn(state)"));

         // TODO update this test

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must\":[{\"exists\":{\"field\":\"state\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void isNull()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("n(state)"));

         // TODO update this test

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"state\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void simpleSort()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("order=test"));

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl.toDslMap());

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"size\":100,\"query\":{\"bool\":{}},\"from\":0,\"sort\":[{\"test\":{\"missing\":\"_first\",\"order\":\"ASC\"}},{\"id\":{\"missing\":\"_first\",\"order\":\"asc\"}}]}", json);


         Order order = dsl.getOrder();
         assertNotNull(order);
         assertEquals(2, order.getOrderList().size());
         Map<String, String> orderMap = order.getOrderList().get(0);
         assertEquals("ASC", orderMap.get("test"));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void multiSort()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("sort=test,-test2,+test3"));

         ObjectMapper mapper = new ObjectMapper();
         String json = mapper.writeValueAsString(dsl.toDslMap());

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"size\":100,\"query\":{\"bool\":{}},\"from\":0,\"sort\":[{\"test\":{\"missing\":\"_first\",\"order\":\"ASC\"}},{\"test2\":{\"missing\":\"_last\",\"order\":\"DESC\"}},{\"test3\":{\"missing\":\"_first\",\"order\":\"ASC\"}},{\"id\":{\"missing\":\"_first\",\"order\":\"asc\"}}]}", json);

         Order order = dsl.getOrder();
         assertNotNull(order);
         assertEquals(4, order.getOrderList().size());
         Map<String, String> orderMap = order.getOrderList().get(0);
         assertEquals("ASC", orderMap.get("test"));
         orderMap = order.getOrderList().get(1);
         assertEquals("DESC", orderMap.get("test2"));
         orderMap = order.getOrderList().get(2);
         assertEquals("ASC", orderMap.get("test3"));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testGt()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("gt(hispanicRank,25)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"range\":{\"hispanicRank\":{\"gt\":\"25\"}}}", json);

         assertNull(dsl.getBool());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNotNull(dsl.getRange());
         assertEquals("hispanicRank", (dsl.getRange().getName()));
         assertEquals("25", (dsl.getRange().getGt()));
         assertNull((dsl.getRange().getGte()));
         assertNull((dsl.getRange().getLte()));
         assertNull((dsl.getRange().getLt()));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testGte()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("ge(hispanicRank,25)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"range\":{\"hispanicRank\":{\"gte\":\"25\"}}}", json);

         assertNull(dsl.getBool());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNotNull(dsl.getRange());
         assertEquals("hispanicRank", (dsl.getRange().getName()));
         assertEquals("25", (dsl.getRange().getGte()));
         assertNull((dsl.getRange().getGt()));
         assertNull((dsl.getRange().getLte()));
         assertNull((dsl.getRange().getLt()));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testNestedGt()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("gt(players.registerNum,3)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"nested\":{\"path\":\"players\",\"query\":{\"range\":{\"players.registerNum\":{\"gt\":\"3\"}}}}}", json);

         assertNull(dsl.getBool());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getRange());
         assertNotNull(dsl.getNested());
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testEq1()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("eq(city,CHANDLER)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"term\":{\"city\":\"CHANDLER\"}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getBool());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getTerm());
         assertEquals("city", (dsl.getTerm().getName()));
         assertEquals("CHANDLER", (dsl.getTerm().getValueList().get(0)));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testEq2()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("eq(uninstalled,true)"));
         ObjectMapper mapper = new ObjectMapper();

         assertNull(dsl.getRange());
         assertNull(dsl.getBool());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getTerm());
         assertEquals("uninstalled", (dsl.getTerm().getName()));
         assertEquals("true", (dsl.getTerm().getValueList().get(0)));

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"term\":{\"uninstalled\":\"true\"}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testNotEq()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("ne(hispanicRank,25)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must_not\":[{\"term\":{\"hispanicRank\":\"25\"}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());

         assertNotNull(dsl.getBool());
         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getShould());
         assertNotNull(dsl.getBool().getMustNot());

         assertEquals(1, (dsl.getBool().getMustNot().size()));
         assertEquals(Term.class, (dsl.getBool().getMustNot().get(0).getClass()));
         assertEquals("hispanicRank", (((Term) dsl.getBool().getMustNot().get(0))).getName());
         assertEquals("25", (((Term) dsl.getBool().getMustNot().get(0))).getValueList().get(0));

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testCompoundNotEq()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(ne(hispanicRank,95),gt(hispanicRank,93))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"range\":{\"hispanicRank\":{\"gt\":\"93\"}}}],\"must_not\":[{\"term\":{\"hispanicRank\":\"95\"}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());

         assertNotNull(dsl.getBool());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getShould());
         assertNotNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getFilter());

         assertEquals(1, (dsl.getBool().getMustNot().size()));

         assertEquals(Term.class, (dsl.getBool().getMustNot().get(0).getClass()));
         assertEquals("hispanicRank", ((Term) dsl.getBool().getMustNot().get(0)).getName());
         assertEquals("95", ((Term) dsl.getBool().getMustNot().get(0)).getValueList().get(0));
         assertEquals(1, (dsl.getBool().getFilter().size()));
         assertEquals(Range.class, (dsl.getBool().getFilter().get(0).getClass()));
         assertEquals("hispanicRank", ((Range) dsl.getBool().getFilter().get(0)).getName());
         assertEquals("93", ((Range) dsl.getBool().getFilter().get(0)).getGt());

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void simpleAnd()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("ne(hispanicRank,95)&gt(hispanicRank,93)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"range\":{\"hispanicRank\":{\"gt\":\"93\"}}}],\"must_not\":[{\"term\":{\"hispanicRank\":\"95\"}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());

         assertNotNull(dsl.getBool());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getShould());
         assertNotNull(dsl.getBool().getMustNot());
         assertNotNull(dsl.getBool().getFilter());

         assertEquals(1, (dsl.getBool().getMustNot().size()));

         assertEquals(Term.class, (dsl.getBool().getMustNot().get(0).getClass()));
         assertEquals("hispanicRank", ((Term) dsl.getBool().getMustNot().get(0)).getName());
         assertEquals("95", ((Term) dsl.getBool().getMustNot().get(0)).getValueList().get(0));

         assertEquals(1, (dsl.getBool().getFilter().size()));
         assertEquals(Range.class, (dsl.getBool().getFilter().get(0).getClass()));
         assertEquals("hispanicRank", ((Range) dsl.getBool().getFilter().get(0)).getName());
         assertEquals("93", ((Range) dsl.getBool().getFilter().get(0)).getGt());

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void orFunction()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("or(eq(name,fwqa),eq(name,cheetos)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"term\":{\"name\":\"fwqa\"}},{\"term\":{\"name\":\"cheetos\"}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNotNull(dsl.getBool());

         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getMustNot());
         assertNull(dsl.getBool().getFilter());
         assertNotNull(dsl.getBool().getShould());

         assertEquals(2, (dsl.getBool().getShould().size()));

         assertEquals(Term.class, (dsl.getBool().getShould().get(0).getClass()));
         assertEquals("name", ((Term) dsl.getBool().getShould().get(0)).getName());
         assertEquals("fwqa", ((Term) dsl.getBool().getShould().get(0)).getValueList().get(0));

         assertEquals(Term.class, (dsl.getBool().getShould().get(1).getClass()));
         assertEquals("name", ((Term) dsl.getBool().getShould().get(1)).getName());
         assertEquals("cheetos", ((Term) dsl.getBool().getShould().get(1)).getValueList().get(0));

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void inFunction()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("in(city,Chicago,Tempe,Chandler)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"terms\":{\"city\":[\"Chicago\",\"Tempe\",\"Chandler\"]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getBool());
         assertNotNull(dsl.getTerms());

         assertEquals("city", dsl.getTerms().getName());
         assertEquals(3, dsl.getTerms().getValueList().size());

         assertEquals("Chicago", dsl.getTerms().getValueList().get(0));
         assertEquals("Tempe", dsl.getTerms().getValueList().get(1));
         assertEquals("Chandler", dsl.getTerms().getValueList().get(2));

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void outFunction()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("out(city,Chicago,Tempe,Chandler)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"must_not\":[{\"terms\":{\"city\":[\"Chicago\",\"Tempe\",\"Chandler\"]}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getTerms());
         assertNotNull(dsl.getBool());

         assertNull(dsl.getBool().getFilter());
         assertNull(dsl.getBool().getMust());
         assertNull(dsl.getBool().getShould());
         assertNotNull(dsl.getBool().getMustNot());

         assertEquals(1, dsl.getBool().getMustNot().size());
         assertEquals(Term.class, dsl.getBool().getMustNot().get(0).getClass());
         assertEquals("city", ((Term) dsl.getBool().getMustNot().get(0)).getName());
         assertEquals(3, ((Term) dsl.getBool().getMustNot().get(0)).getValueList().size());

         assertEquals("Chicago", ((Term) dsl.getBool().getMustNot().get(0)).getValueList().get(0));
         assertEquals("Tempe", ((Term) dsl.getBool().getMustNot().get(0)).getValueList().get(1));
         assertEquals("Chandler", ((Term) dsl.getBool().getMustNot().get(0)).getValueList().get(2));

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexAnd()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("eq(locationCode,270*)&eq(city,Chandler)&eq(address1,*McQueen*)"));

         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getRange());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         assertEquals(3, dsl.getBool().getFilter().size());

         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"wildcard\":{\"locationCode\":\"270*\"}},{\"term\":{\"city\":\"Chandler\"}},{\"wildcard\":{\"address1\":\"*McQueen*\"}}]}}", json);
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexSearch1()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(search(keywords,test),search(keywords,matt))"));

         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getRange());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"fuzzy\":{\"keywords\":\"test\"}},{\"fuzzy\":{\"keywords\":\"matt\"}}]}}", json);
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexSearch2()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("search(keywords,test)&search(keywords,matt)"));

         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getRange());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"fuzzy\":{\"keywords\":\"test\"}},{\"fuzzy\":{\"keywords\":\"matt\"}}]}}", json);
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexOr()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("or(eq(id,3),eq(name,*POST*))"));

         assertNull(dsl.getWildcard());
         assertNull(dsl.getTerm());
         assertNull(dsl.getRange());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"should\":[{\"term\":{\"id\":\"3\"}},{\"wildcard\":{\"name\":\"*POST*\"}}]}}", json);
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testWildcard()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("eq(address1,*GILBERT*)"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"wildcard\":{\"address1\":\"*GILBERT*\"}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getBool());
         assertNotNull(dsl.getWildcard());
         assertEquals("address1", (dsl.getWildcard().getName()));
         assertEquals("*GILBERT*", (dsl.getWildcard().getValue()));

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testAndTerms()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(eq(locationCode,9187),eq(city,CHANDLER))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"term\":{\"locationCode\":\"9187\"}},{\"term\":{\"city\":\"CHANDLER\"}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool());
         assertTrue(dsl.getBool().getFilter().size() == 2);
         assertTrue(dsl.getBool().getFilter().get(0) instanceof Term);
         assertEquals("locationCode", ((Term) dsl.getBool().getFilter().get(0)).getName());
         assertEquals("9187", ((Term) dsl.getBool().getFilter().get(0)).getValueList().get(0));
         assertTrue(dsl.getBool().getFilter().get(1) instanceof Term);
         assertEquals("city", ((Term) dsl.getBool().getFilter().get(1)).getName());
         assertEquals("CHANDLER", ((Term) dsl.getBool().getFilter().get(1)).getValueList().get(0));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void testAndWildcard()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(eq(address1,*GILBERT*),eq(city,CHANDLER))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"wildcard\":{\"address1\":\"*GILBERT*\"}},{\"term\":{\"city\":\"CHANDLER\"}}]}}", json);
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool());
         assertTrue(dsl.getBool().getFilter().size() == 2);
         assertTrue(dsl.getBool().getFilter().get(0) instanceof Wildcard);
         assertEquals("address1", ((Wildcard) dsl.getBool().getFilter().get(0)).getName());
         assertEquals("*GILBERT*", ((Wildcard) dsl.getBool().getFilter().get(0)).getValue());
         assertTrue(dsl.getBool().getFilter().get(1) instanceof Term);
         assertEquals("city", ((Term) dsl.getBool().getFilter().get(1)).getName());
         assertEquals("CHANDLER", ((Term) dsl.getBool().getFilter().get(1)).getValueList().get(0));
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void smallCompoundQuery()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(gt(hispanicRank,25),le(hispanicRank,40))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"range\":{\"hispanicRank\":{\"gt\":\"25\"}}},{\"range\":{\"hispanicRank\":{\"lte\":\"40\"}}}]}}", json);
         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNotNull(dsl.getBool());
         assertTrue(dsl.getBool().getFilter().size() == 2);
         assertTrue(dsl.getBool().getFilter().get(0) instanceof Range);
         assertEquals("hispanicRank", ((Range) dsl.getBool().getFilter().get(0)).getName());
         assertNull(((Range) dsl.getBool().getFilter().get(0)).getGte());
         assertNull(((Range) dsl.getBool().getFilter().get(0)).getLte());
         assertNull(((Range) dsl.getBool().getFilter().get(0)).getLt());
         assertEquals("25", ((Range) dsl.getBool().getFilter().get(0)).getGt());
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void largeCompoundQuery()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(and(eq(locationCode,270*),eq(city,Chandler)),and(eq(address1,*McQueen*)))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"bool\":{\"filter\":[{\"wildcard\":{\"locationCode\":\"270*\"}},{\"term\":{\"city\":\"Chandler\"}}]}},{\"bool\":{\"filter\":[{\"wildcard\":{\"address1\":\"*McQueen*\"}}]}}]}}", json);
      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void simpleNestedQuery()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("gt(players.registerNum,5)"));
         ObjectMapper mapper = new ObjectMapper();

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getBool());
         assertNull(dsl.getNestedPath());
         assertNotNull(dsl.getNested());
         NestedQuery nested = (NestedQuery) dsl.getNested();
         assertEquals("players", nested.getPath());
         assertNotNull(nested.getQuery());

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"nested\":{\"path\":\"players\",\"query\":{\"range\":{\"players.registerNum\":{\"gt\":\"5\"}}}}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexNestedQuery1()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(eq(keywords.name,color),eq(keywords.value,33))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"keywords\",\"query\":{\"bool\":{\"filter\":[{\"term\":{\"keywords.name\":\"color\"}},{\"term\":{\"keywords.value\":\"33\"}}]}}}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         BoolQuery bool = dsl.getBool();
         assertNull(bool.getShould());
         assertNull(bool.getMustNot());
         assertNull(bool.getMust());
         assertNotNull(bool.getFilter());

         List<ElasticQuery> boolFilterList = bool.getFilter();
         assertTrue(boolFilterList.size() == 1);
         assertTrue(boolFilterList.get(0) instanceof NestedQuery);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexNestedQuery2()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(eq(keywords.name,age),gt(keywords.value,30))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"keywords\",\"query\":{\"bool\":{\"filter\":[{\"term\":{\"keywords.name\":\"age\"}},{\"range\":{\"keywords.value\":{\"gt\":\"30\"}}}]}}}}]}}", json);

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getNestedPath());
         assertNull(dsl.getNested());
         assertNotNull(dsl.getBool());

         BoolQuery bool = dsl.getBool();
         assertNull(bool.getShould());
         assertNull(bool.getMustNot());
         assertNull(bool.getMust());
         assertNotNull(bool.getFilter());

         List<ElasticQuery> boolFilterList = bool.getFilter();
         assertTrue(boolFilterList.size() == 1);
         assertTrue(boolFilterList.get(0) instanceof NestedQuery);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexNestedQuery3()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(eq(keywords.name,items.name),w(keywords.value,Powerade))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"keywords\",\"query\":{\"bool\":{\"filter\":[{\"term\":{\"keywords.name\":\"items.name\"}},{\"bool\":{\"should\":[{\"wildcard\":{\"keywords.value\":\"*Powerade*\"}}]}}]}}}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void complexNestedQuery4()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(w(keywords.value,Powerade),eq(keywords.name,items.name))"));
         ObjectMapper mapper = new ObjectMapper();

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"keywords\",\"query\":{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"wildcard\":{\"keywords.value\":\"*Powerade*\"}}]}},{\"term\":{\"keywords.name\":\"items.name\"}}]}}}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void compoundNestedQuery1()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(gt(players.registerNum,5),eq(city,Chandler))"));
         ObjectMapper mapper = new ObjectMapper();

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNotNull(dsl.getBool());

         BoolQuery bool = dsl.getBool();
         assertNull(bool.getNestedPath());
         assertNull(bool.getMust());
         assertNull(bool.getMustNot());
         assertNull(bool.getShould());
         assertNotNull(bool.getFilter());
         assertEquals(2, bool.getFilter().size());
         assertEquals(NestedQuery.class, bool.getFilter().get(0).getClass());
         assertEquals(Term.class, bool.getFilter().get(1).getClass());

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"players\",\"query\":{\"range\":{\"players.registerNum\":{\"gt\":\"5\"}}}}},{\"term\":{\"city\":\"Chandler\"}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   @Test
   public void compoundNestedQuery2()
   {
      try
      {
         QueryDsl dsl = ((ElasticRql) Rql.getRql("elastic")).toQueryDsl(split("and(and(eq(players.deleted,true),eq(city,PHOENIX)),and(eq(address1,*VALLEY*)))"));
         ObjectMapper mapper = new ObjectMapper();

         assertNull(dsl.getRange());
         assertNull(dsl.getTerm());
         assertNull(dsl.getWildcard());
         assertNull(dsl.getNested());
         assertNull(dsl.getNestedPath());
         assertNotNull(dsl.getBool());

         BoolQuery bool = dsl.getBool();
         assertNull(bool.getNestedPath());
         assertNull(bool.getMust());
         assertNull(bool.getMustNot());
         assertNull(bool.getShould());
         assertNotNull(bool.getFilter());
         assertEquals(2, bool.getFilter().size());
         assertEquals(BoolQuery.class, bool.getFilter().get(0).getClass());
         assertEquals(BoolQuery.class, bool.getFilter().get(1).getClass());

         String json = mapper.writeValueAsString(dsl);

         assertNotNull("json should not be empty.", json);
         assertEquals("{\"bool\":{\"filter\":[{\"bool\":{\"filter\":[{\"nested\":{\"path\":\"players\",\"query\":{\"term\":{\"players.deleted\":\"true\"}}}},{\"term\":{\"city\":\"PHOENIX\"}}]}},{\"bool\":{\"filter\":[{\"wildcard\":{\"address1\":\"*VALLEY*\"}}]}}]}}", json);

      }
      catch (Exception e)
      {
         log.debug("derp! ", e);
         fail();
      }
   }

   static LinkedHashMap split(String queryString)
   {
      LinkedHashMap map = new LinkedHashMap();

      String[] terms = queryString.split("&");
      for (String term : terms)
      {
         int eqIdx = term.indexOf('=');
         if (eqIdx < 0)
         {
            map.put(term, null);
         }
         else
         {
            String value = term.substring(eqIdx + 1, term.length());
            term = term.substring(0, eqIdx);
            map.put(term, value);
         }
      }
      return map;
   }

}
