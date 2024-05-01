package io.rcktapp.api.handler.sql;

import io.forty11.j.J;
import io.forty11.sql.Sql;
import io.forty11.web.Url;
import io.rcktapp.api.*;

import io.rcktapp.api.Collection;
import io.rcktapp.api.service.Service;

import io.rcktapp.rql.Replacer;
import io.rcktapp.rql.Rql;
import io.rcktapp.rql.Stmt;
import io.rcktapp.rql.sql.SqlRql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SqlGetHandlerTest {

    @Mock
    Service service;

    @Mock
    Api api;

    @Mock
    Endpoint endpoint;

    @Mock
    Action action;

    @Mock
    Chain chain;

    @Mock
    Response response;

    @Mock
    Stmt stmt;

    @Mock
    SqlDb sqlDb;

    @Mock
    SqlRql sqlRql;

    @Mock
    Connection connection;

    @Mock
    SqlGetHandler handler;

    @Captor
    ArgumentCaptor<List> queryParametersCaptor;

    @Captor
    ArgumentCaptor<String> sqlCaptor;
    
    @Test
    public void testSqlGetHandler_queryParametersAreProperlyPassedToQueryObjectsMethod() throws Exception {

        Url url = new Url("http://localhost:8080/api/lift/us/plugins/updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)");

        String passedQuery = "updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)";
        String collectionName = "plugins";
        String expectedQuery = " SELECT * FROM " + collectionName + " WHERE plugins IN (?,?,?,?,?) ";
        String[] inClause = {"updatexml(rand()", "concat(CHAR(126)", "user()", "CHAR(126))", "null)"};

        // Setup request object and sql table objects to properly reflect
        Request request = new Request(url, "GET", new HashMap<>(), new HashMap<>(), "");
        Service.ApiMatch apiMatch = new Service.ApiMatch(api, endpoint, "GET", request.getUrl(), "http://localhost:8080/api/lift/us/", collectionName + "/" + passedQuery + "/");
        Collection collection = new Collection();
        Entity entity = new Entity(new Table(sqlDb, collectionName));
        Attribute attribute = new Attribute();

        attribute.setName(collectionName);
        entity.setKey(attribute);
        collection.setEntity(entity);
        request.setApiMatch(apiMatch);

        // Mocked methods that return local objects used in the future
        when(chain.getService()).thenReturn(service);
        when(service.getDb(request.getApi(), request.getCollectionKey(), SqlDb.class)).thenReturn(sqlDb);
        when(sqlDb.getConnection(false)).thenReturn(connection);

        try (MockedStatic<Rql> rqlMockedStatic = mockStatic(Rql.class);
             MockedStatic<Sql> sqlMockedStatic = mockStatic(Sql.class)) {

            rqlMockedStatic.when(() -> Rql.getRql(sqlDb.getType())).thenReturn(sqlRql);

            when(api.getCollection(request.getCollectionKey(), SqlDb.class)).thenReturn(collection);
            sqlMockedStatic.when(() -> Sql.getInClauseStr(J.explode(",", Sql.check(request.getEntityKey())))).thenReturn(passedQuery);

            // When we call the service method on the mocked handler actually call the real method
            doCallRealMethod().when(handler).service(service, api, endpoint, action, chain, request, response);

            // More mocked methods that return objects reused locally
            when(sqlRql.asCol("plugins")).thenReturn(collectionName);
            sqlMockedStatic.when(() -> Sql.check(collectionName)).thenReturn(collectionName);
            sqlMockedStatic.when(() -> Sql.getQuestionMarkStr(inClause)).thenReturn("?,?,?,?,?");
            when(sqlRql.createStmt(any(), any(), any(), any(Replacer.class))).thenReturn(stmt);
            when(chain.getConfig("maxRows", 0)).thenReturn(0);

            // Call the actual method on the mocked handler
            handler.service(service, api, endpoint, action, chain, request, response);

            // Verify that the passed in query contains no SQL functions
            verify(sqlRql).createStmt(sqlCaptor.capture(), any(), any(), any(Replacer.class));
            assertEquals(expectedQuery, sqlCaptor.getValue());

            // Verify that the invocation of queryObjects contains the proper query parameters so that query is properly parametrized with the expected values
            verify(handler).queryObjects(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), queryParametersCaptor.capture());
            assertEquals(Arrays.asList(inClause), queryParametersCaptor.getValue());
        }
    }
}
