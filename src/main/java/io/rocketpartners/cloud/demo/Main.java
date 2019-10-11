package io.rocketpartners.cloud.demo;

//import io.rocketpartners.cloud.action.rest.RestAction;
//import io.rocketpartners.cloud.action.sql.H2SqlDb;
//import io.rocketpartners.cloud.model.Api;

public class Main
{
   public static void main(String[] args) throws Exception
   {
      DynamoDemo demo = new DynamoDemo();

      demo.buildTable();
   }

   //   public static Api buildApi()
   //   {
   //      return new Api()//
   //                      .withName("northwind")//
   //                      .withDb(new H2SqlDb("db", "DemoSqlDbNorthwind1.db", Demo001SqlDbNorthwind.class.getResource("northwind.h2.ddl").toString()))//
   //                      //.withDb(new SqlDb("db", "YOUR_JDBC_DRIVER", "YOUR_JDBC_URL", "YOUR_JDBC_USERNAME", "YOUR_JDBC_PASSWORD")))//
   //                      .withEndpoint("GET,PUT,POST,DELETE", "/*", new RestAction());
   //   }
}
