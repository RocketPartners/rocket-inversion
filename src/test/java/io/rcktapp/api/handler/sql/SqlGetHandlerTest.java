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
    Collection collection;

    @Mock
    Entity entity;

    @Mock
    Table table;

    @Mock
    Attribute attribute;

    @Mock
    Stmt statement;

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
    
    @Captor
    ArgumentCaptor<Map<String, String>> rqlParamsCaptor;

    @Test
    public void testSqlGetHandler_queryParametersAreProperlyPassedToQueryObjectsMethod() throws Exception {

        Url url = new Url("http://localhost:8080/api/lift/us/plugins/updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)");

        String passedQuery = "updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)";
        String collectionName = "plugins";
        String[] inClause = {"updatexml(rand()", "concat(CHAR(126)", "user()", "CHAR(126))", "null)"};

        // Setup request object with proper api configuration
        Request request = new Request(url, "GET", new HashMap<>(), new HashMap<>(), "");
        Service.ApiMatch apiMatch = new Service.ApiMatch(api, endpoint, "GET", request.getUrl(), "http://localhost:8080/api/lift/us/", collectionName + "/" + passedQuery + "/");
        request.setApiMatch(apiMatch);

        mockStatic(Rql.class);
        mockStatic(Sql.class);

        // Mocked methods that return local objects used in the future
        when(chain.getService()).thenReturn(service);
        when(service.getDb(request.getApi(), request.getCollectionKey(), SqlDb.class)).thenReturn(sqlDb);
        when(sqlDb.getConnection(false)).thenReturn(connection);
        when(Rql.getRql(sqlDb.getType())).thenReturn(sqlRql);
        when(api.getCollection(request.getCollectionKey(), SqlDb.class)).thenReturn(collection);
        when(collection.getEntity()).thenReturn(entity);
        when(entity.getTable()).thenReturn(table);
        when(entity.getKey()).thenReturn(attribute);
        when(attribute.getName()).thenReturn(collectionName);
        when(Sql.getInClauseStr(J.explode(",", Sql.check(request.getEntityKey())))).thenReturn(passedQuery);

        // When we call the service method on the mocked handler actually call the real method
        doCallRealMethod().when(handler).service(service, api, endpoint, action, chain, request, response);

        // More mocked methods that return objects reused locally
        when(sqlRql.asCol(table.getName())).thenReturn("Plugins");
        when(Sql.getQuestionMarkStr(inClause)).thenReturn("?,?,?,?,?");
        when(sqlRql.createStmt(eq(" SELECT * FROM Plugins WHERE null IN (?,?,?,?,?) "), any(), eq(request.getParams()), any(Replacer.class))).thenReturn(statement);
        when(chain.getConfig("maxRows", 0)).thenReturn(0);

        // Call the actual method on the mocked handler
        handler.service(service, api, endpoint, action, chain, request, response);

        // Verify that the invocation of queryObjects contains the proper parameters.
        verify(handler).queryObjects(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), queryParametersCaptor.capture());

        // Assert that the query parameters captured are equal to the expected list of parameters from the passed in URL
        assertEquals(queryParametersCaptor.getValue(), Arrays.asList(inClause));
    }
}
