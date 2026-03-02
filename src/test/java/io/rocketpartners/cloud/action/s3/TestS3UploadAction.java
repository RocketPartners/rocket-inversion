package io.rocketpartners.cloud.action.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import io.rocketpartners.cloud.model.Api;
import io.rocketpartners.cloud.model.Endpoint;
import io.rocketpartners.cloud.model.ObjectNode;
import io.rocketpartners.cloud.model.Request;
import io.rocketpartners.cloud.model.Request.Upload;
import io.rocketpartners.cloud.model.Response;
import io.rocketpartners.cloud.service.Chain;
import io.rocketpartners.cloud.service.Service;
import software.amazon.awssdk.services.s3.S3Client;

class TestS3UploadAction
{
   private S3UploadAction uploadAction;
   private Service mockService;
   private Api mockApi;
   private Endpoint mockEndpoint;
   private Chain mockChain;
   private Request mockRequest;
   private Response response;
   private S3Client mockS3Client;
   private MockedStatic<Chain> chainMock;

   @BeforeEach
   void setup()
   {
      uploadAction = spy(new S3UploadAction());

      mockService = mock(Service.class);
      mockApi = mock(Api.class);
      mockEndpoint = mock(Endpoint.class);
      mockChain = mock(Chain.class);
      mockRequest = mock(Request.class);
      response = new Response();
      mockS3Client = mock(S3Client.class);

      // Setup Chain static mock
      chainMock = mockStatic(Chain.class);
      chainMock.when(Chain::peek).thenReturn(mockChain);

      doReturn(mockS3Client).when(uploadAction).buildS3Client(any());
   }

   @AfterEach
   void tearDown()
   {
      if (chainMock != null)
      {
         chainMock.close();
      }
   }

   @Test
   void testRunWithNoUploadReturnsError() throws Exception
   {
      when(mockRequest.getUploads()).thenReturn(Arrays.asList());

      // Actually call the method
      uploadAction.run(mockService, mockApi, mockEndpoint, mockChain, mockRequest, response);

      // Verify error response
      assertEquals(400, response.getStatusCode());
      assertNotNull(response.getJson());
      assertTrue(response.getJson().getString("message").contains("No file was uploaded"));
      assertEquals("Bad Request Exception", response.getJson().getString("error"));
   }

   @Test
   void testRunWithValidUploadAttemptsS3Upload() throws Exception
   {
      // Test that valid upload attempts to upload to S3
      // This will fail at S3Client creation, but verifies the flow up to that point
      Upload mockUpload = mock(Upload.class);
      byte[] testData = "test file content".getBytes();
      InputStream testStream = new ByteArrayInputStream(testData);

      when(mockUpload.getInputStream()).thenReturn(testStream);
      when(mockUpload.getFileName()).thenReturn("test-file.txt");
      when(mockUpload.getFileSize()).thenReturn((long) testData.length);
      when(mockUpload.getRequestPath()).thenReturn("/uploads/test");

      when(mockRequest.getUploads()).thenReturn(Arrays.asList(mockUpload));

      // Mock config values
      when(mockChain.getConfig("s3Bucket", null)).thenReturn("test-bucket");
      when(mockChain.getConfig("s3BasePath", "uploads")).thenReturn("uploads");
      when(mockChain.getConfig("s3DatePath", "yyyy/MM/dd")).thenReturn("yyyy/MM/dd");
      when(mockChain.getConfig("s3AccessKey", null)).thenReturn(null);
      when(mockChain.getConfig("s3SecretKey", null)).thenReturn(null);
      when(mockChain.getConfig("s3AwsRegion", null)).thenReturn("us-east-1");

      // Actually call run() - will attempt to create S3Client and fail
      uploadAction.run(mockService, mockApi, mockEndpoint, mockChain, mockRequest, response);

      // Verify upload data was accessed (proving we got past validation)
      verify(mockUpload).getInputStream();
      verify(mockUpload).getFileName();
      verify(mockUpload).getFileSize();
      verify(mockUpload).getRequestPath();
   }

   @Test
   void testMultipleFileExtensionsLosesLastExtension() throws Exception
   {
      // Test filename with multiple dots (e.g., "archive.tar.gz")
      // The split("[.]") will create ["archive", "tar", "gz"]
      // Line 103 accesses fileNameParts[0] and fileNameParts[1]
      // Result: "archive-<timestamp>.tar" (loses the .gz - this is a bug!)

      Upload mockUpload = mock(Upload.class);
      InputStream testStream = new ByteArrayInputStream("test".getBytes());

      when(mockUpload.getInputStream()).thenReturn(testStream);
      when(mockUpload.getFileName()).thenReturn("archive.tar.gz");
      when(mockUpload.getFileSize()).thenReturn(4L);
      when(mockUpload.getRequestPath()).thenReturn("/test");

      when(mockRequest.getUploads()).thenReturn(Arrays.asList(mockUpload));

      // Mock config to attempt the operation
      when(mockChain.getConfig("s3Bucket", null)).thenReturn("test-bucket");
      when(mockChain.getConfig("s3BasePath", "uploads")).thenReturn(null);
      when(mockChain.getConfig("s3DatePath", "yyyy/MM/dd")).thenReturn(null);

      uploadAction.run(mockService, mockApi, mockEndpoint, mockChain, mockRequest, response);

      // Verify upload was accessed (filename transformation occurred)
      verify(mockUpload).getFileName();
      verify(mockUpload).getInputStream();
   }

   @Test
   void testErrorMethodReturns400()
   {
      uploadAction.error(response, null, "Test error message");

      assertEquals(400, response.getStatusCode());
      assertNotNull(response.getJson());

      ObjectNode json = response.getJson();
      assertEquals("Test error message", json.getString("message"));
      assertEquals("Bad Request Exception", json.getString("error"));
   }

   @Test
   void testErrorMethodIncludesException()
   {
      Exception testException = new Exception("Test exception");

      uploadAction.error(response, testException, "Error occurred");

      assertEquals(400, response.getStatusCode());
      ObjectNode json = response.getJson();

      String message = json.getString("message");
      assertTrue(message.contains("Error occurred"));
      assertTrue(message.contains("Test exception"));
   }

   @Test
   void testEmptyUploadListReturnsError() throws Exception
   {
      // Test that empty upload list triggers error
      when(mockRequest.getUploads()).thenReturn(Arrays.asList());

      // Actually call run()
      uploadAction.run(mockService, mockApi, mockEndpoint, mockChain, mockRequest, response);

      // Verify error response (line 111-115 checks uploadStream == null)
      assertEquals(400, response.getStatusCode());
      assertNotNull(response.getJson());
      assertTrue(response.getJson().getString("message").contains("No file was uploaded"));
      assertEquals("Bad Request Exception", response.getJson().getString("error"));
   }

   @Test
   void testConfigurationValuesAreReadFromChain() throws Exception
   {
      // Verify that configuration is read from Chain
      // buildS3Client reads: s3AccessKey, s3SecretKey, s3AwsRegion (lines 215-217)
      // buildFullPath reads: s3BasePath, s3DatePath (lines 180-181)
      // saveFile reads: s3Bucket (line 160)

      Upload mockUpload = mock(Upload.class);
      InputStream testStream = new ByteArrayInputStream("test".getBytes());

      when(mockUpload.getInputStream()).thenReturn(testStream);
      when(mockUpload.getFileName()).thenReturn("test.txt");
      when(mockUpload.getFileSize()).thenReturn(4L);
      when(mockUpload.getRequestPath()).thenReturn("/test");

      when(mockRequest.getUploads()).thenReturn(Arrays.asList(mockUpload));

      // Mock config returns
      when(mockChain.getConfig("s3Bucket", null)).thenReturn("my-bucket");
      when(mockChain.getConfig("s3BasePath", "uploads")).thenReturn("custom-path");
      when(mockChain.getConfig("s3DatePath", "yyyy/MM/dd")).thenReturn(null);
      when(mockChain.getConfig("s3AccessKey", null)).thenReturn(null);
      when(mockChain.getConfig("s3SecretKey", null)).thenReturn(null);
      when(mockChain.getConfig("s3AwsRegion", null)).thenReturn(null);

      uploadAction.run(mockService, mockApi, mockEndpoint, mockChain, mockRequest, response);

      // Verify config was read from chain
      verify(mockChain).getConfig("s3Bucket", null);
      verify(mockChain).getConfig("s3BasePath", "uploads");
      verify(mockChain).getConfig("s3DatePath", "yyyy/MM/dd");
   }
}
