package io.rcktapp.api.handler.sql;

import io.forty11.j.J;
import io.forty11.sql.Sql;
import io.forty11.web.Url;
import io.rcktapp.api.*;

import io.rcktapp.api.Collection;
import io.rcktapp.api.handler.sql.SqlDb;
import io.rcktapp.api.handler.sql.SqlGetHandler;
import io.rcktapp.api.service.Service;

import io.rcktapp.rql.Replacer;
import io.rcktapp.rql.Rql;
import io.rcktapp.rql.Stmt;
import io.rcktapp.rql.sql.SqlRql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Array;
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
    ArgumentCaptor<List> captor;

    @Test
    public void testSqlGetHandler_properParmasPassedToQueryMethod() throws Exception {
        Url url = new Url("http://localhost:8080/api/lift/us/plugins/updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)");

        Request request = new Request(url, "GET", new HashMap<>(), new HashMap<>(), "");
        Service.ApiMatch apiMatch = new Service.ApiMatch(api, endpoint, "GET", request.getUrl(), "http://localhost:8080/api/lift/us/", "plugins/updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)/");

        request.setApiMatch(apiMatch);
        collection.setName("plugins");

        mockStatic(Rql.class);
        mockStatic(Sql.class);

        when(chain.getService()).thenReturn(service);
        when(service.getDb(request.getApi(), request.getCollectionKey(), SqlDb.class)).thenReturn(sqlDb);
        when(sqlDb.getConnection()).thenReturn(connection);
        when(Rql.getRql(sqlDb.getType())).thenReturn(sqlRql);
        when(api.getCollection(request.getCollectionKey(), SqlDb.class)).thenReturn(collection);
        when(collection.getEntity()).thenReturn(entity);
        when(entity.getTable()).thenReturn(table);
        when(entity.getKey()).thenReturn(attribute);
        when(attribute.getName()).thenReturn("plugins");
        when(Sql.getInClauseStr(J.explode(",", Sql.check(request.getEntityKey())))).thenReturn("updatexml(rand(),concat(CHAR(126),user(),CHAR(126)),null)");

        doCallRealMethod().when(handler).service(service,api,endpoint,action,chain,request,response);
        when(sqlRql.asCol(table.getName())).thenReturn("Plugins");

        String [] inClause = {"updatexml(rand()", "concat(CHAR(126)", "user()", "CHAR(126))", "null)"};

        when(Sql.getQuestionMarkStr(inClause)).thenReturn("?,?,?,?,?");
        when(sqlRql.createStmt(eq(" SELECT * FROM Plugins WHERE null IN (?,?,?,?,?) "), any(), eq(request.getParams()), any(Replacer.class))).thenReturn(statement);
        when(chain.getConfig("maxRows", 0)).thenReturn(0);

        handler.service(service,api,endpoint,action,chain,request,response);

        verify(handler).queryObjects(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), captor.capture());
        assertEquals(captor.getValue(), Arrays.asList(inClause));
    }
}
