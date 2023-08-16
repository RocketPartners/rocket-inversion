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
package io.rcktapp.api.handler.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.rcktapp.api.ApiException;
import io.rcktapp.api.Attribute;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Column;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Relationship;
import io.rcktapp.api.SC;
import io.rcktapp.api.Table;
import io.rcktapp.rql.sql.SqlRql;
import lombok.extern.slf4j.Slf4j;
import org.atteo.evo.inflector.English;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

@Slf4j
public class SqlDb extends Db
{
   static
   {
      try
      {
         //bootstraps the SqlRql type
         Class.forName(SqlRql.class.getName());
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
      }
   }

   public static final int MIN_POOL_SIZE            = 5;
   public static final int MAX_POOL_SIZE            = 10;

   boolean                 shutdown                 = false;

   ComboPooledDataSource   pool                     = null;

   protected String        driver                   = null;
   protected String        url                      = null;
   protected String        roUrl                    = null;
   protected String        user                     = null;
   protected String        pass                     = null;
   protected String        ignoreTablePrefixes      = "";
   protected int           poolMin                  = MIN_POOL_SIZE;
   protected int           poolMax                  = MAX_POOL_SIZE;
   protected int           idleConnectionTestPeriod = 3600; // in seconds

   // set this to false to turn off SQL_CALC_FOUND_ROWS and SELECT FOUND_ROWS()
   // Only impacts 'mysql' types
   protected boolean       calcRowsFound            = true;

   @Override
   public String getType()
   {
      if (type != null)
         return type;

      if (driver != null)
      {
         if (driver.indexOf("mysql") >= 0)
            return "mysql";

         if (driver.indexOf("postgres") >= 0)
            return "postgres";

         if (driver.indexOf("redshift") >= 0)
            return "redshift";
      }

      return null;
   }

   public void shutdown()
   {
      shutdown = true;

      synchronized (this)
      {
         pool.close();
      }
   }

   /**
    * @deprecated  As of release 0.3.12, replaced by {@link #getConnection(boolean)}
    */
   @Deprecated(since = "0.3.12", forRemoval = true)
   public Connection getConnection() throws ApiException
   {
      return getConnection(true);
   }

   public Connection getConnection(boolean writable) throws ApiException
   {
      try
      {
         Connection conn = ConnectionLocal.getConnection(this);
         if (conn == null && !shutdown)
         {
            if (pool == null)
            {
               synchronized (this)
               {
                  if (pool == null && !shutdown)
                  {
                     String driver = getDriver();
                     String url = writable ? getUrl() : getRoUrl();
                     String user = getUser();
                     String password = getPass();
                     int minPoolSize = getPoolMin();
                     int maxPoolSize = getPoolMax();
                     int idleTestPeriod = getIdleConnectionTestPeriod();

                     pool = new ComboPooledDataSource();
                     pool.setDriverClass(driver);
                     pool.setJdbcUrl(url);
                     pool.setUser(user);
                     pool.setPassword(password);
                     pool.setInitialPoolSize(minPoolSize);
                     pool.setMinPoolSize(minPoolSize);
                     pool.setMaxPoolSize(maxPoolSize);

                     pool.setIdleConnectionTestPeriod(idleTestPeriod);
                     //                     if (idleTestPeriod > 0)
                     //                        pool.setTestConnectionOnCheckin(true);
                  }
               }
            }

            conn = pool.getConnection();
            conn.setAutoCommit(false);

            ConnectionLocal.putConnection(this, conn);
         }

         return conn;
      }
      catch (Exception ex)
      {
         log.error("Unable to get DB connection", ex);
         throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Unable to get DB connection", ex);
      }
   }

   public static class ConnectionLocal
   {
      static ThreadLocal<Map<Db, Connection>> connections = new ThreadLocal();

      public static Map<Db, Connection> getConnections()
      {
         return connections.get();
      }

      public static Connection getConnection(Db db)
      {
         Map<Db, Connection> conns = connections.get();
         if (conns == null)
         {
            conns = new HashMap();
            connections.set(conns);
         }

         return conns.get(db);
      }

      public static void putConnection(Db db, Connection connection)
      {
         Map<Db, Connection> conns = connections.get();
         if (conns == null)
         {
            conns = new HashMap();
            connections.set(conns);
         }
         conns.put(db, connection);
      }

      public static void commit() throws Exception
      {
         Exception toThrow = null;
         Map<Db, Connection> conns = connections.get();
         if (conns != null)
         {
            for (Db db : (List<Db>) new ArrayList(conns.keySet()))
            {
               Connection conn = conns.get(db);
               try
               {
                  conn.commit();
               }
               catch (Exception ex)
               {
                  if (toThrow != null)
                     toThrow = ex;
               }
            }
         }

         if (toThrow != null)
            throw toThrow;
      }

      public static void rollback() throws Exception
      {
         Exception toThrow = null;
         Map<Db, Connection> conns = connections.get();
         if (conns != null)
         {
            for (Db db : (List<Db>) new ArrayList(conns.keySet()))
            {
               Connection conn = conns.get(db);
               try
               {
                  conn.rollback();
               }
               catch (Exception ex)
               {
                  if (toThrow != null)
                     toThrow = ex;
               }
            }
         }

         if (toThrow != null)
            throw toThrow;
      }

      public static void close() throws Exception
      {
         Exception toThrow = null;
         Map<Db, Connection> conns = connections.get();
         if (conns != null)
         {
            for (Db db : (List<Db>) new ArrayList(conns.keySet()))
            {
               Connection conn = conns.get(db);
               try
               {
                  conn.close();
               }
               catch (Exception ex)
               {
                  if (toThrow != null)
                     toThrow = ex;
               }
            }
         }

         connections.remove();

         if (toThrow != null)
            throw toThrow;
      }
   }

   public void bootstrapApi() throws Exception
   {
      reflectDb();
      configApi();
   }

   public void reflectDb() throws Exception
   {
      if (!isBootstrap())
      {
         return;
      }

      Executor dbMetadataExecutorPool = Executors.newFixedThreadPool(50);
      String driver = getDriver();
      Class.forName(driver);
      try (Connection apiConn = DriverManager.getConnection(getRoUrl(), getUser(), getPass()))
      {

         DatabaseMetaData dbmd = apiConn.getMetaData();

         //-- only here to map jdbc type integer codes to strings ex "4" to "BIGINT" or whatever it is
         Map<String, String> types = new HashMap<String, String>();
         for (Field field : Types.class.getFields())
         {
            types.put(field.get(null) + "", field.getName());
         }
         //--

         //-- the first loop through is going to construct all of the
         //-- Tbl and Col objects.  There will be a second loop through
         //-- that caputres all of the foreign key relationships.  You
         //-- have to do the fk loop second becuase the reference pk
         //-- object needs to exist so that it can be set on the fk Col
            Map<String, CompletableFuture<Table>> tableFutures = new HashMap<>();
            try (ResultSet rs = dbmd.getTables(null, "public", "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next())
                {
                    String tableCat = rs.getString("TABLE_CAT");
                    String tableSchem = rs.getString("TABLE_SCHEM");
                    String tableName = rs.getString("TABLE_NAME");
                    //String tableType = rs.getString("TABLE_TYPE");

                    if (Stream.of(ignoreTablePrefixes.split(",")).filter(Predicate.not(String::isEmpty)).anyMatch(tableName::startsWith)) continue;

                    tableFutures.put(tableName, CompletableFuture.supplyAsync(() -> {
                        Table table = new Table(this, tableName);
                        try (Connection tableConnection = DriverManager.getConnection(getRoUrl(), getUser(), getPass())) {
                            DatabaseMetaData databaseMetaData = tableConnection.getMetaData();
                            try (ResultSet colsRs = databaseMetaData.getColumns(tableCat, tableSchem, tableName, "%")) {

                                while (colsRs.next())
                                {
                                    String colName = colsRs.getString("COLUMN_NAME");
                                    Object type = colsRs.getString("DATA_TYPE");
                                    String colType = types.get(type);

                                    boolean nullable = colsRs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;

                                    Column column = new Column(table, colName, colType, nullable);
                                    table.addColumn(column);

                                    //               if (DELETED_FLAGS.contains(colName.toLowerCase()))
                                    //               {
                                    //                  table.setDeletedFlag(column);
                                    //               }
                                }
                            }

                            ResultSet indexMd = databaseMetaData.getIndexInfo(tableConnection.getCatalog(), null, tableName, true, false);
                            while (indexMd.next()) {
                                String colName = indexMd.getString("COLUMN_NAME");
                                for (Column c : table.getColumns()) {
                                    if (c.getName().equalsIgnoreCase(colName)) {
                                        c.setUnique(true);
                                    }
                                }
                            }
                            indexMd.close();
                            log.info("{} table processing {}", getType(), table.getName());
                            return table;
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }, dbMetadataExecutorPool));

                }
            }

            log.info("{} building foreign key relationships", getType());
            //-- now link all of the fks to pks
            //-- this is done after the first loop
            //-- so that all of the tbls/cols are
            //-- created first and are there to
            //-- be connected
            List<CompletableFuture<?>> keyFutures = new ArrayList<>();
            try (ResultSet foreignKeyTablesRS = dbmd.getTables(null, "public", "%", new String[]{"TABLE"}))  {
                while (foreignKeyTablesRS.next())
                {
                    String tableName = foreignKeyTablesRS.getString("TABLE_NAME");

                    if (Stream.of(ignoreTablePrefixes.split(",")).filter(Predicate.not(String::isEmpty)).anyMatch(tableName::startsWith)) continue;

                    keyFutures.add(CompletableFuture.supplyAsync(() -> {
                        try (Connection keyConnection = DriverManager.getConnection(getRoUrl(), getUser(), getPass())) {
                            DatabaseMetaData databaseMetaData = keyConnection.getMetaData();

                            try (ResultSet keyMd = databaseMetaData.getImportedKeys(keyConnection.getCatalog(), null, tableName)) {
                                while (keyMd.next())
                                {
                                    String fkTableName = keyMd.getString("FKTABLE_NAME");
                                    String fkColumnName = keyMd.getString("FKCOLUMN_NAME");
                                    String pkTableName = keyMd.getString("PKTABLE_NAME");
                                    String pkColumnName = keyMd.getString("PKCOLUMN_NAME");

                                    Column fk = null;
                                    Table fkTable = tableFutures.get(fkTableName).get();
                                    for (Column c : fkTable.getColumns())
                                        if (c.getName().equalsIgnoreCase(fkColumnName))
                                            fk = c;
                                    if (fk == null)
                                        throw new RuntimeException("fk column not found: " + fkTableName + "." + fkColumnName);
                                   Table pkTable = tableFutures.get(pkTableName).get();
                                    for (Column pk : pkTable.getColumns())
                                        if (pk.getName().equalsIgnoreCase(pkColumnName))
                                            fk.setPk(pk);

                                    //log.info(fkTableName + "." + fkColumnName + " -> " + pkTableName + "." + pkColumnName);
                                }
                                log.info("{} foreign key processing for table {}", getType(), tableName);
                                return null;
                            } catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }, dbMetadataExecutorPool));
                }
            }

         for (CompletableFuture<?> keyFuture : keyFutures) {
            keyFuture.get();
         }
         for (CompletableFuture<Table> tableCompletableFuture : tableFutures.values()) {
            addTable(tableCompletableFuture.get());
         }

         log.info("finally, many-to-many");

         //-- if a table has two columns and both are foreign keys
         //-- then it is a relationship table for MANY_TO_MANY relationships
         for (Table table : getTables())
         {
            List<Column> cols = table.getColumns();
            if (cols.size() == 2 && cols.get(0).isFk() && cols.get(1).isFk())
            {
               table.setLinkTbl(true);
            }
         }
      }
   }

   public void configApi() throws Exception
   {
      for (Table t : getTables())
      {
         List<Column> cols = t.getColumns();

         Collection collection = new Collection();
         String collectionName = t.getName();

         collectionName = Character.toLowerCase(collectionName.charAt(0)) + collectionName.substring(1, collectionName.length());

         if (!collectionName.endsWith("s"))
            collectionName = English.plural(collectionName);

         collection.setName(collectionName);

         Entity entity = new Entity();
         entity.setTbl(t);
         entity.setHint(t.getName());

         entity.setCollection(collection);
         collection.setEntity(entity);

         for (Column col : cols)
         {
            if (col.getPk() == null)
            {
               Attribute attr = new Attribute();
               attr.setEntity(entity);
               attr.setName(col.getName());
               attr.setColumn(col);
               attr.setHint(col.getTable().getName() + "." + col.getName());
               attr.setType(col.getType());

               entity.addAttribute(attr);

               if (col.isUnique() && entity.getKey() == null)
               {
                  entity.setKey(attr);
               }
            }
         }

         api.addCollection(collection);
         collection.setApi(api);

      }

      //-- Now go back through and create relationships for all foreign keys
      //-- two relationships objects are created for every relationship type
      //-- representing both sides of the relationship...ONE_TO_MANY also
      //-- creates a MANY_TO_ONE and there are always two for a MANY_TO_MANY.
      //-- API designers may want to represent one or both directions of the
      //-- relationship in their API and/or the names of the JSON properties
      //-- for the relationships will probably be different
      for (Table t : getTables())
      {
         if (t.isLinkTbl())
         {
            Column fkCol1 = t.getColumns().get(0);
            Column fkCol2 = t.getColumns().get(1);

            //MANY_TO_MANY one way
            {
               Entity pkEntity = api.getEntity(fkCol1.getPk().getTable());
               Relationship r = new Relationship();
               pkEntity.addRelationship(r);
               r.setEntity(pkEntity);
               Entity related = api.getEntity(fkCol2.getPk().getTable());
               r.setRelated(related);

               String hint = "MANY_TO_MANY - ";
               hint += fkCol1.getPk().getTable().getName() + "." + fkCol1.getPk().getName();
               hint += " <- " + fkCol1.getTable().getName() + "." + fkCol1.getName() + ":" + fkCol2.getName();
               hint += " -> " + fkCol2.getPk().getTable().getName() + "." + fkCol2.getPk().getName();

               r.setHint(hint);
               r.setType(Relationship.REL_MANY_TO_MANY);
               r.setFkCol1(fkCol1);
               r.setFkCol2(fkCol2);

               //Collection related = api.getCollection(fkCol2.getTbl());
               r.setName(makeRelationshipName(r));
            }

            //MANY_TO_MANY the other way
            {
               Entity pkEntity = api.getEntity(fkCol2.getPk().getTable());
               Relationship r = new Relationship();
               pkEntity.addRelationship(r);
               r.setEntity(pkEntity);

               Entity related = api.getEntity(fkCol1.getPk().getTable());
               r.setRelated(related);

               //r.setRelated(api.getEntity(fkCol1.getTable()));

               String hint = "MANY_TO_MANY - ";
               hint += fkCol2.getPk().getTable().getName() + "." + fkCol2.getPk().getName();
               hint += " <- " + fkCol2.getTable().getName() + "." + fkCol2.getName() + ":" + fkCol1.getName();
               hint += " -> " + fkCol1.getPk().getTable().getName() + "." + fkCol1.getPk().getName();

               r.setHint(hint);
               r.setType(Relationship.REL_MANY_TO_MANY);
               r.setFkCol1(fkCol2);
               r.setFkCol2(fkCol1);

               r.setName(makeRelationshipName(r));
            }
         }
         else
         {
            for (Column col : t.getColumns())
            {
               if (col.isFk())
               {
                  Column fkCol = col;
                  Table fkTbl = fkCol.getTable();
                  Entity fkEntity = api.getEntity(fkTbl);

                  Column pkCol = col.getPk();
                  Table pkTbl = pkCol.getTable();
                  Entity pkEntity = api.getEntity(pkTbl);

                  if (pkEntity == null)
                  {
                     System.err.println("Unknown Entity for table: " + pkTbl);
                     continue;
                  }

                  //ONE_TO_MANY
                  {
                     Relationship r = new Relationship();

                     //TODO:this name may not be specific enough or certain types
                     //of relationships. For example where an entity is related
                     //to another entity twice
                     r.setHint("MANY_TO_ONE - " + pkTbl.getName() + "." + pkCol.getName() + " <- " + fkTbl.getName() + "." + fkCol.getName());
                     r.setType(Relationship.REL_MANY_TO_ONE);
                     r.setFkCol1(fkCol);
                     r.setEntity(pkEntity);
                     r.setRelated(fkEntity);
                     r.setName(makeRelationshipName(r));
                     pkEntity.addRelationship(r);
                  }

                  //MANY_TO_ONE
                  {
                     Relationship r = new Relationship();
                     r.setHint("ONE_TO_MANY - " + fkTbl.getName() + "." + fkCol.getName() + " -> " + pkTbl.getName() + "." + pkCol.getName());
                     r.setType(Relationship.REL_ONE_TO_MANY);
                     r.setFkCol1(fkCol);
                     r.setEntity(fkEntity);
                     r.setRelated(pkEntity);
                     r.setName(makeRelationshipName(r));
                     fkEntity.addRelationship(r);
                  }
               }
            }
         }
      }
   }

   public String getDriver()
   {
      return driver;
   }

   public void setDriver(String driver)
   {
      this.driver = driver;
   }

   public String getRoUrl(){
      if (roUrl == null)
         return getUrl();
      return roUrl;
   }

   public String getUrl()
   {
      return url;
   }

   public void setUrl(String url)
   {
      this.url = url;
   }

   public String getUser()
   {
      return user;
   }

   public void setUser(String user)
   {
      this.user = user;
   }

   public String getPass()
   {
      return pass;
   }

   public void setPass(String pass)
   {
      this.pass = pass;
   }

   public int getPoolMin()
   {
      return poolMin;
   }

   public void setPoolMin(int poolMin)
   {
      this.poolMin = poolMin;
   }

   public int getPoolMax()
   {
      return poolMax;
   }

   public void setPoolMax(int poolMax)
   {
      this.poolMax = poolMax;
   }

   public int getIdleConnectionTestPeriod()
   {
      return idleConnectionTestPeriod;
   }

   public void setIdleConnectionTestPeriod(int idleConnectionTestPeriod)
   {
      this.idleConnectionTestPeriod = idleConnectionTestPeriod;
   }

   public boolean isCalcRowsFound()
   {
      if (driver.indexOf("mysql") < 0)
         return false;

      return calcRowsFound;
   }

   public void setCalcRowsFound(boolean calcRowsFound)
   {
      this.calcRowsFound = calcRowsFound;
   }

}
