package io.rcktapp.rql;

import io.forty11.web.js.JS;
import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.handler.sql.SqlPostHandler;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCollapse
{
   public static void main(String[] args)
   {
      TestCollapse test = new TestCollapse();
      test.testCollapses1();
      test.testCollapses2();
      test.testCollapses3();
   }

   @Test
   public void testCollapses1()
   {
      JSObject parent = new JSObject();
      parent.put("name", "testing");

      JSObject child1 = new JSObject();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSObject child2 = new JSObject();
      parent.put("child2", child2);

      child2.put("href", "http://child2");
      child2.put("name", "child2");

      JSObject collapsed = JS.toJSObject(parent.toString());

      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("child2")), "");

      JSObject benchmark = JS.toJSObject(parent.toString());
      benchmark = JS.toJSObject(parent.toString());
      benchmark.remove("child2");
      benchmark.put("child2", new JSObject("href", "http://child2"));

       assertEquals(benchmark.toString(), collapsed.toString());

   }

   @Test
   public void testCollapses2()
   {
      JSObject parent = new JSObject();
      parent.put("name", "testing");

      JSObject child1 = new JSObject();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSArray arrChildren = new JSArray();
      for (int i = 0; i < 5; i++)
      {
         arrChildren.add(new JSObject("href", "href://child" + i, "name", "child" + i));
      }

      parent.put("arrChildren", arrChildren);

      JSObject collapsed = JS.toJSObject(parent.toString());

      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("arrChildren")), "");

      JSObject benchmark = JS.toJSObject(parent.toString());
      benchmark = JS.toJSObject(parent.toString());
      benchmark.remove("arrChildren");
      arrChildren = new JSArray();
      for (int i = 0; i < 5; i++)
      {
         arrChildren.add(new JSObject("href", "href://child" + i));
      }
      benchmark.put("arrChildren", arrChildren);

       assertEquals(benchmark.toString(), collapsed.toString());

   }

   @Test
   public void testCollapses3()
   {
      JSObject parent = new JSObject();
      parent.put("name", "testing");

      JSObject child1 = new JSObject();
      parent.put("child1", child1);
      child1.put("href", "http://child1");
      child1.put("name", "child1");

      JSObject child2 = new JSObject();
      parent.put("child2", child2);
      child2.put("href", "http://child2");
      child2.put("name", "child2");

      JSObject child3 = new JSObject();
      child2.put("child3", child3);
      child3.put("href", "http://child3");
      child3.put("name", "child3");

      JSObject collapsed = JS.toJSObject(parent.toString());

      SqlPostHandler.collapse(collapsed, false, new HashSet<>(Collections.singletonList("child2.child3")), "");

      JSObject benchmark = JS.toJSObject(parent.toString());
      benchmark = JS.toJSObject(parent.toString());
      benchmark.getObject("child2").getObject("child3").remove("name");

       assertEquals(benchmark.toString(), collapsed.toString());

   }

}
