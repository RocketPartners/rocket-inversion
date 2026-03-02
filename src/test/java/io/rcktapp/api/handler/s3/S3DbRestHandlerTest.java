package io.rcktapp.api.handler.s3;

import io.forty11.web.Url;
import io.forty11.web.js.JS;
import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.*;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Request.Upload;
import io.rcktapp.api.service.Service;
import io.rcktapp.rql.Rql;
import io.rcktapp.rql.s3.S3Rql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3DbRestHandlerTest {

    @Mock
    private Service service;

    @Mock
    private Api api;

    @Mock
    private Endpoint endpoint;

    @Mock
    private Action action;

    @Mock
    private Chain chain;

    @Mock
    private Request request;

    private Response response;

    @Mock
    private S3Db s3Db;

    @Mock
    private Table table;

    @Mock
    private Entity entity;

    @Mock
    private Collection collection;

    @Mock
    private S3Rql s3Rql;

    @InjectMocks
    private S3DbRestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new S3DbRestHandler();
        lenient().when(collection.getEntity()).thenReturn(entity);
        lenient().when(entity.getTable()).thenReturn(table);
        lenient().when(table.getDb()).thenReturn(s3Db);
        response = new Response();
    }

    @Test
    void testService_unsupportedMethod_throwsException() {
        when(request.getMethod()).thenReturn("DELETE");

        ApiException exception = assertThrows(ApiException.class, () -> handler.service(service, api, endpoint, action, chain, request, response));

        assertEquals(SC.SC_400_BAD_REQUEST, exception.getStatus());
        assertTrue(exception.getMessage().contains("S3 handler only supports GET and POST/PUT"));
    }

    @Test
    void testDoGet_setsInputStream() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "test-key", 100, true, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            ResponseInputStream<GetObjectResponse> mockStream = mock(ResponseInputStream.class);
            GetObjectResponse mockResponse = mock(GetObjectResponse.class);

            when(mockStream.response()).thenReturn(mockResponse);
            when(mockResponse.contentType()).thenReturn("text/plain");
            when(mockResponse.contentLength()).thenReturn(1024L);
            when(s3Db.getDownload(any())).thenReturn(mockStream);

            handler.service(service, api, endpoint, action, chain, request, response);

            assertEquals(mockStream, response.getInputStream());
            assertEquals("text/plain", response.getContentType());
        }
    }

    @Test
    void testDoGet_s3NotModifiedReturns204() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "test-key", 100, true, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            S3Exception s3Exception = (S3Exception) S3Exception.builder()
                    .statusCode(304)
                    .message("Not Modified")
                    .build();

            when(s3Db.getDownload(any())).thenThrow(s3Exception);

            handler.service(service, api, endpoint, action, chain, request, response);

            assertEquals(SC.SC_204_NO_CONTENT, response.getStatus());
        }
    }

    @Test
    void testDoGet_s3ExceptionReturns204() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "test-key", 100, true, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            S3Exception s3Exception = (S3Exception) S3Exception.builder()
                    .statusCode(404)
                    .message("Not Found")
                    .build();

            when(s3Db.getDownload(any())).thenThrow(s3Exception);

            assertThrows(S3Exception.class, () -> handler.service(service, api, endpoint, action, chain, request, response));
        }
    }

    @Test
    void testDoGet_withKeyParameter_retrievesMetadata() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "test-key.txt", 100, false, true, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getSubpath()).thenReturn("/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            HeadObjectResponse headResponse = HeadObjectResponse.builder()
                    .contentLength(1024L)
                    .contentType("text/plain")
                    .eTag("test-etag")
                    .lastModified(Instant.now())
                    .build();

            when(s3Db.headObject(any())).thenReturn(headResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            assertEquals(SC.SC_200_OK, response.getStatus());
            JSObject json = response.getJson();

            assertNotNull(json);
            assertEquals("http://localhost/api/s3test-bucket/test-key.txt", json.getString("href"));
            assertEquals("test-etag", json.getString("eTag"));
            assertEquals(0, json.getObject("userMetadata").keys().size());
        }
    }

    @Test
    void testDoGet_listObjects_returnsJsonWithMetadata() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, null, 100, false, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);
        when(request.getApiUrl()).thenReturn("http://localhost");
        when(request.getPath()).thenReturn("/api/s3/test-bucket/");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.isDefaultDownload()).thenReturn(false);

            ListObjectsResponse listResponse = ListObjectsResponse.builder()
                    .maxKeys(100)
                    .isTruncated(false)
                    .contents(Collections.singletonList(S3Object.builder().key("test-object.txt").eTag("test-etag").lastModified(Instant.now()).build()))
                    .commonPrefixes(Collections.singletonList(CommonPrefix.builder().prefix("dir/").build()))
                    .build();

            when(s3Db.getCoreMetaData(any())).thenReturn(listResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            JSArray data = response.getJson().getArray("data");
            assertEquals(2, data.length());

            // response data should be alphabetical
            assertEquals("http://localhost/api/s3/test-bucket/dir/", data.getObject(0).getString("href"));
            assertFalse((Boolean) data.getObject(0).get("isFile"));
            assertEquals("http://localhost/api/s3/test-bucket/test-object.txt", data.getObject(1).getString("href"));
            assertTrue((Boolean) data.getObject(1).get("isFile"));
        }
    }

    @Test
    void testDoPost_withUpload_savesFile() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "uploaded-file.txt", null, false, false, null);
        Upload upload = mock(Upload.class);
        List<Upload> uploads = new ArrayList<>();
        uploads.add(upload);

        byte[] testData = "test file content".getBytes();
        when(upload.getInputStream()).thenReturn(new ByteArrayInputStream(testData));
        when(upload.getFileSize()).thenReturn((long) testData.length);

        when(request.getMethod()).thenReturn("POST");
        when(request.getUploads()).thenReturn(uploads);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            PutObjectResponse putResponse = PutObjectResponse.builder().build();
            when(s3Db.saveFile(any(), anyString(), anyString(), any(), anyLong(), anyMap()))
                    .thenReturn(putResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            verify(s3Db).saveFile(any(), anyString(), anyString(), any(), anyLong(), anyMap());
            assertEquals(SC.SC_200_OK, response.getStatus());
            assertEquals("http://localhost/api/s3/test-bucketuploaded-file.txt", response.getJson().getString("href"));
        }
    }

    @Test
    void testDoPost_withMetadata_parsesAndSavesMetadata() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "file.txt", null, false, false, null);
        Upload upload = mock(Upload.class);
        List<Upload> uploads = new ArrayList<>();
        uploads.add(upload);

        byte[] testData = "test".getBytes();
        when(upload.getInputStream()).thenReturn(new ByteArrayInputStream(testData));
        when(upload.getFileSize()).thenReturn((long) testData.length);

        String metaJson = "{\"content-type\":\"application/json\",\"author\":\"test-user\"}";

        when(request.getMethod()).thenReturn("POST");
        when(request.getUploads()).thenReturn(uploads);
        when(request.getParam("meta")).thenReturn(metaJson);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class);
             MockedStatic<JS> jsMock = mockStatic(JS.class)) {

            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            JSObject metaObject = new JSObject();
            metaObject.put("content-type", "application/json");
            metaObject.put("author", "test-user");

            jsMock.when(() -> JS.toJSObject(metaJson)).thenReturn(metaObject);

            PutObjectResponse putResponse = PutObjectResponse.builder().build();
            when(s3Db.saveFile(any(), anyString(), anyString(), anyString(), anyLong(), anyMap()))
                    .thenReturn(putResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            ArgumentCaptor<Map<String, String>> metaCaptor = ArgumentCaptor.forClass(Map.class);
            verify(s3Db).saveFile(any(), anyString(), anyString(), anyString(), anyLong(), metaCaptor.capture());
            assertEquals("test-user", metaCaptor.getValue().get("author"));
            assertEquals("application/json", metaCaptor.getValue().get("Content-Type"));
        }
    }

    @Test
    void testDoPost_withNullSaveResult_throwsException() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "file.txt", null, false, false, null);
        Upload upload = mock(Upload.class);
        List<Upload> uploads = new ArrayList<>();
        uploads.add(upload);

        when(request.getMethod()).thenReturn("POST");
        when(upload.getInputStream()).thenReturn(new ByteArrayInputStream("test".getBytes()));
        when(upload.getFileSize()).thenReturn(4L);
        when(request.getUploads()).thenReturn(uploads);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.saveFile(any(), anyString(), anyString(), any(), anyLong(), anyMap()))
                    .thenReturn(null);

            ApiException exception = assertThrows(ApiException.class, () -> handler.service(service, api, endpoint, action, chain, request, response));

            assertEquals(SC.SC_500_INTERNAL_SERVER_ERROR, exception.getStatus());
            assertTrue(exception.getMessage().contains("Failed to POST/PUT file to s3"));
        }
    }

    @Test
    void testDoPut_updatesMetadata() throws Exception {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(collection);
        when(request.getCollectionKey()).thenReturn("test-bucket");

        JSObject metaJson = new JSObject();
        metaJson.put("name", "updated-file.txt");
        metaJson.put("custom-field", "custom-value");

        when(request.getMethod()).thenReturn("PUT");
        when(request.getJson()).thenReturn(metaJson);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");
        when(table.getName()).thenReturn("test-bucket");

        CopyObjectResponse copyResult = CopyObjectResponse.builder()
                .copyObjectResult(
                        CopyObjectResult.builder()
                                .eTag("new-etag")
                                .lastModified(Instant.now())
                                .build())
                .versionId("new-version")
                .build();

        when(s3Db.updateObject(anyString(), anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(copyResult);

        handler.service(service, api, endpoint, action, chain, request, response);

        verify(s3Db).updateObject(eq("test-bucket"), eq("updated-file.txt"),
                eq("test-bucket"), eq("updated-file.txt"), anyMap());
        assertEquals(SC.SC_200_OK, response.getStatus());
        JSObject json = response.getJson();
        assertEquals("http://localhost/api/s3/test-bucketupdated-file.txt", json.getString("href"));
        assertEquals("new-etag", json.getString("etag"));
        assertEquals("new-version", json.getString("versionId"));
        assertNotNull(json.get("lastModified"));
    }

    @Test
    void testDoPut_withoutName_throwsException() {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(collection);
        when(request.getCollectionKey()).thenReturn("test-bucket");

        JSObject metaJson = new JSObject();
        metaJson.put("custom-field", "value");

        when(request.getMethod()).thenReturn("PUT");
        when(request.getJson()).thenReturn(metaJson);

        ApiException exception = assertThrows(ApiException.class, () ->  handler.service(service, api, endpoint, action, chain, request, response));

        assertTrue(exception.getMessage().contains("name"));
    }

    @Test
    void testFindCollectionOrThrow404_withNullCollection_throwsException() {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(null);
        when(request.getCollectionKey()).thenReturn("non-existent");
        when(request.getMethod()).thenReturn("GET");

        try {
            handler.service(service, api, endpoint, action, chain, request, response);
            fail("Should have thrown ApiException");
        } catch (Exception e) {
            if (!(e instanceof ApiException)) {
                fail("Should have thrown ApiException");
            }
        }
    }

    @Test
    void testBuildListObj_createsCorrectStructure() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, null, 10, false, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);
        when(request.getPath()).thenReturn("/api/s3/test-bucket/");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.isDefaultDownload()).thenReturn(false);

            S3Object s3Object = S3Object.builder()
                    .key("test-file.txt")
                    .size(1024L)
                    .lastModified(Instant.now())
                    .build();

            ListObjectsResponse listResponse = ListObjectsResponse.builder()
                    .maxKeys(10)
                    .isTruncated(false)
                    .contents(Collections.singletonList(s3Object))
                    .commonPrefixes(new ArrayList<>())
                    .build();

            when(s3Db.getCoreMetaData(any())).thenReturn(listResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            JSObject json = response.getJson();
            JSArray data = (JSArray) json.get("data");
            assertEquals(1, data.length());
            assertEquals("http://localhost/api/s3/test-bucket/test-file.txt", data.getObject(0).getString("href"));
            assertEquals(1024L, data.getObject(0).get("size"));
            assertTrue((Boolean) data.getObject(0).get("isFile"));
            assertNotNull(data.getObject(0).getString("lastModified"));
        }
    }

    @Test
    void testMaxRowsConfiguration() {
        S3DbRestHandler customHandler = new S3DbRestHandler();
        customHandler.maxRows = 50;
        assertEquals(50, customHandler.maxRows);

        customHandler.maxRows = 1000;
        assertEquals(1000, customHandler.maxRows);
    }

    @Test
    void testS3DatePathConfiguration() {
        S3DbRestHandler customHandler = new S3DbRestHandler();
        assertNull(customHandler.s3DatePath);

        customHandler.s3DatePath = "yyyy/MM/dd";
        assertEquals("yyyy/MM/dd", customHandler.s3DatePath);
    }

    @Test
    void testDoGet_withDefaultDownload_downloadsFile() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "file.txt", 100, false, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.isDefaultDownload()).thenReturn(true);

            ResponseInputStream<GetObjectResponse> mockStream = mock(ResponseInputStream.class);
            GetObjectResponse mockResponse = mock(GetObjectResponse.class);

            when(mockStream.response()).thenReturn(mockResponse);
            when(mockResponse.contentType()).thenReturn("application/pdf");
            when(mockResponse.contentLength()).thenReturn(2048L);
            when(s3Db.getDownload(any())).thenReturn(mockStream);

            handler.service(service, api, endpoint, action, chain, request, response);

            assertNotNull(response.getInputStream());
            assertEquals("application/pdf", response.getContentType());
        }
    }

    @Test
    void testDoGet_listObjectsWithPagination_includesNextMarker() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, null, 100, false, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);
        when(request.getUrl()).thenReturn(new Url("http://localhost/api/s3/test-bucket?pageSize=100"));

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.isDefaultDownload()).thenReturn(false);

            ListObjectsResponse listResponse = ListObjectsResponse.builder()
                    .maxKeys(100)
                    .isTruncated(true)
                    .nextMarker("next-page-marker")
                    .contents(new ArrayList<>())
                    .commonPrefixes(new ArrayList<>())
                    .build();

            when(s3Db.getCoreMetaData(any())).thenReturn(listResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            JSObject json = response.getJson();
            JSObject meta = (JSObject) json.get("meta");
            assertNotNull(meta.get("next"));
            assertTrue(meta.get("next").toString().contains("marker=next-page-marker"));
        }
    }

    @Test
    void testDoGet_headObjectFails_fallsBackToList() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "nonexistent-file.txt", 100, false, true, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);
            when(s3Db.headObject(any())).thenThrow(new RuntimeException("Not found"));

            ListObjectsResponse listResponse = ListObjectsResponse.builder()
                    .maxKeys(100)
                    .isTruncated(false)
                    .contents(Collections.singletonList(S3Object.builder().key("test-object.txt").eTag("test-etag").lastModified(Instant.now()).build()))
                    .commonPrefixes(Collections.singletonList(CommonPrefix.builder().prefix("dir/").build()))
                    .build();

            when(s3Db.getCoreMetaData(any())).thenReturn(listResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            verify(s3Db).headObject(any());
            verify(s3Db).getCoreMetaData(any());

            assertEquals(2, response.getJson().getArray("data").length());
        }
    }

    @Test
    void testDoPost_withS3DatePath_buildsDateBasedPath() throws Exception {
        setupCommonMocks();

        handler.s3DatePath = "yyyy/MM/dd";

        S3Request s3Request = new S3Request("test-bucket", null, "file.txt", null, false, false, null);
        Upload upload = mock(Upload.class);
        List<Upload> uploads = new ArrayList<>();
        uploads.add(upload);

        byte[] testData = "test".getBytes();
        when(upload.getInputStream()).thenReturn(new ByteArrayInputStream(testData));
        when(upload.getFileSize()).thenReturn((long) testData.length);

        when(request.getMethod()).thenReturn("POST");
        when(request.getUploads()).thenReturn(uploads);
        when(request.getSubpath()).thenReturn("/test-bucket/");
        when(request.getParam("meta")).thenReturn(null);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            PutObjectResponse putResponse = PutObjectResponse.builder().build();
            when(s3Db.saveFile(any(), anyString(), anyString(), any(), anyLong(), anyMap()))
                    .thenReturn(putResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
            verify(request).setSubpath(pathCaptor.capture());
            String subPath = pathCaptor.getValue();
            assertTrue(subPath.startsWith("/test-bucket/"));
            String datePart = subPath.substring("/test-bucket/".length());
            assertNotNull(new SimpleDateFormat("yyyy/MM/dd").parse(datePart));
        }
    }

    @Test
    void testDoPost_noUploads_completes() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, null, null, false, false, null);

        when(request.getMethod()).thenReturn("POST");
        when(request.getUploads()).thenReturn(new ArrayList<>());

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            handler.service(service, api, endpoint, action, chain, request, response);

            assertEquals(SC.SC_200_OK, response.getStatus());
        }
    }

    @Test
    void testDoPut_filtersReservedMetadataKeys() throws Exception {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(collection);
        when(request.getCollectionKey()).thenReturn("test-bucket");

        JSObject metaJson = new JSObject();
        metaJson.put("name", "file.txt");
        metaJson.put("tenantid", "tenant-123");  // Should be filtered
        metaJson.put("custom-field", "custom-value");

        when(request.getMethod()).thenReturn("PUT");
        when(request.getJson()).thenReturn(metaJson);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");
        when(table.getName()).thenReturn("test-bucket");

        CopyObjectResponse copyResult = CopyObjectResponse.builder()
                .copyObjectResult(
                        CopyObjectResult.builder()
                                .eTag("new-etag")
                                .lastModified(Instant.now())
                                .build())
                .versionId("new-version")
                .build();
        when(s3Db.updateObject(anyString(), anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(copyResult);


        handler.service(service, api, endpoint, action, chain, request, response);

        ArgumentCaptor<Map<String, String>> metaCaptor = ArgumentCaptor.forClass(Map.class);
        verify(s3Db).updateObject(anyString(), anyString(), anyString(), anyString(), metaCaptor.capture());

        Map<String, String> capturedMeta = metaCaptor.getValue();
        assertFalse(capturedMeta.containsKey("tenantid"), "tenantid should be filtered out");
        assertFalse(capturedMeta.containsKey("name"), "name should be filtered out");
        assertTrue(capturedMeta.containsKey("custom-field"));
    }

    @Test
    void testDoGet_customPageSize_usesProvidedValue() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, null, 50, false, false, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn("50");
        when(request.removeParam("pageSize")).thenReturn("50");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), eq(50))).thenReturn(s3Request);
            when(s3Db.isDefaultDownload()).thenReturn(false);

            ListObjectsResponse listResponse = ListObjectsResponse.builder()
                    .maxKeys(50)
                    .isTruncated(false)
                    .contents(new ArrayList<>())
                    .commonPrefixes(new ArrayList<>())
                    .build();

            when(s3Db.getCoreMetaData(any())).thenReturn(listResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            verify(s3Rql).buildS3Request(any(), any(), eq(50));
        }
    }

    @Test
    void testFindCollectionOrThrow404_withNonS3DbCollection_throwsException() {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(collection);
        when(request.getCollectionKey()).thenReturn("test-bucket");

        Db nonS3Db = mock(Db.class);
        when(table.getDb()).thenReturn(nonS3Db);

        when(request.getMethod()).thenReturn("GET");

        ApiException exception = assertThrows(ApiException.class, () ->
            handler.service(service, api, endpoint, action, chain, request, response));

        assertEquals(SC.SC_500_INTERNAL_SERVER_ERROR, exception.getStatus());
        assertTrue(exception.getMessage().contains("Bad server configuration"));
    }

    @Test
    void testConvertToJSObject_includesAllHeadObjectFields() throws Exception {
        setupCommonMocks();

        S3Request s3Request = new S3Request("test-bucket", null, "file.txt", 100, false, true, null);

        when(request.getMethod()).thenReturn("GET");
        when(request.getParam("pageSize")).thenReturn(null);
        when(request.getPath()).thenReturn("/api/s3/test-bucket");
        when(request.getSubpath()).thenReturn("/test-bucket");
        when(request.getApiUrl()).thenReturn("http://localhost");

        try (MockedStatic<Rql> rqlMock = mockStatic(Rql.class)) {
            rqlMock.when(() -> Rql.getRql(anyString())).thenReturn(s3Rql);
            when(s3Rql.buildS3Request(any(), any(), any())).thenReturn(s3Request);

            Map<String, String> userMetadata = new HashMap<>();
            userMetadata.put("custom-key", "custom-value");

            HeadObjectResponse headResponse = HeadObjectResponse.builder()
                    .contentLength(2048L)
                    .contentType("application/json")
                    .eTag("\"test-etag-quoted\"")
                    .lastModified(Instant.now())
                    .metadata(userMetadata)
                    .build();

            when(s3Db.headObject(any())).thenReturn(headResponse);

            handler.service(service, api, endpoint, action, chain, request, response);

            JSObject json = response.getJson();
            assertEquals(2048L, json.get("contentLength"));
            assertEquals("application/json", json.get("contentType"));
            assertEquals("test-etag-quoted", json.get("eTag")); // Should remove quotes
            assertNotNull(json.get("lastModified"));
            assertNotNull(json.get("userMetadata"));
        }
    }

    private void setupCommonMocks() {
        when(api.getCollection(anyString(), eq(S3Db.class))).thenReturn(collection);
        when(request.getCollectionKey()).thenReturn("test-bucket");
        when(s3Db.getType()).thenReturn("s3");
    }
}
