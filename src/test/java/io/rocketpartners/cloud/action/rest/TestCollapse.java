package io.rocketpartners.cloud.action.rest;

<<<<<<< HEAD:src/test/java/io/rocketpartners/cloud/action/rest/TestCollapse.java
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import io.rocketpartners.cloud.model.JSArray;
import io.rocketpartners.cloud.model.JSNode;
import io.rocketpartners.cloud.utils.Utils;
import junit.framework.TestCase;
=======
import io.forty11.web.js.JS;
import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.handler.sql.SqlPostHandler;
import org.junit.jupiter.api.Test;
>>>>>>> 5e8a99b (this is now 0.3.4):src/test/java/io/rcktapp/rql/TestCollapse.java

import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCollapse
{
   @Test
   public void testCollapses1()
   {
      JSNode parent = new JSNode();
      parent.put("name", "testing");

      JSNode child1 = new JSNode();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSNode child2 = new JSNode();
      parent.put("child2", child2);

      child2.put("href", "http://child2");
      child2.put("name", "child2");

      JSNode collapsed = Utils.parseJsonMap(parent.toString());

<<<<<<< HEAD:src/test/java/io/rocketpartners/cloud/action/rest/TestCollapse.java
      RestPostAction.collapse(collapsed, false, new HashSet(Arrays.asList("child2")), "");
=======
      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("child2")), "");
>>>>>>> 5e8a99b (this is now 0.3.4):src/test/java/io/rcktapp/rql/TestCollapse.java

      JSNode benchmark = Utils.parseJsonMap(parent.toString());
      benchmark = Utils.parseJsonMap(parent.toString());
      benchmark.remove("child2");
      benchmark.put("child2", new JSNode("href", "http://child2"));

       assertEquals(benchmark.toString(), collapsed.toString());

   }

   @Test
   public void testCollapses2()
   {
      JSNode parent = new JSNode();
      parent.put("name", "testing");

      JSNode child1 = new JSNode();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSArray arrChildren = new JSArray();
      for (int i = 0; i < 5; i++)
      {
         arrChildren.add(new JSNode("href", "href://child" + i, "name", "child" + i));
      }

      parent.put("arrChildren", arrChildren);

      JSNode collapsed = Utils.parseJsonMap(parent.toString());

<<<<<<< HEAD:src/test/java/io/rocketpartners/cloud/action/rest/TestCollapse.java
      RestPostAction.collapse(collapsed, false, new HashSet(Arrays.asList("arrChildren")), "");
=======
      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("arrChildren")), "");
>>>>>>> 5e8a99b (this is now 0.3.4):src/test/java/io/rcktapp/rql/TestCollapse.java

      JSNode benchmark = Utils.parseJsonMap(parent.toString());
      benchmark = Utils.parseJsonMap(parent.toString());
      benchmark.remove("arrChildren");
      arrChildren = new JSArray();
      for (int i = 0; i < 5; i++)
      {
         arrChildren.add(new JSNode("href", "href://child" + i));
      }
      benchmark.put("arrChildren", arrChildren);

       assertEquals(benchmark.toString(), collapsed.toString());

   }

   @Test
   public void testCollapses3()
   {
      JSNode parent = new JSNode();
      parent.put("name", "testing");

      JSNode child1 = new JSNode();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSNode child2 = new JSNode();
      parent.put("child2", child2);
      child2.put("href", "http://child2");
      child2.put("name", "child2");

      JSNode child3 = new JSNode();
      child2.put("child3", child3);
      child3.put("href", "http://child3");
      child3.put("name", "child3");

      JSNode collapsed = Utils.parseJsonMap(parent.toString());

<<<<<<< HEAD:src/test/java/io/rocketpartners/cloud/action/rest/TestCollapse.java
      RestPostAction.collapse(collapsed, false, new HashSet(Arrays.asList("child2.child3")), "");
=======
      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("child2.child3")), "");
>>>>>>> 5e8a99b (this is now 0.3.4):src/test/java/io/rcktapp/rql/TestCollapse.java

      JSNode benchmark = Utils.parseJsonMap(parent.toString());
      benchmark = Utils.parseJsonMap(parent.toString());
      benchmark.getNode("child2").getNode("child3").remove("name");

       assertEquals(benchmark.toString(), collapsed.toString());

   }

}
