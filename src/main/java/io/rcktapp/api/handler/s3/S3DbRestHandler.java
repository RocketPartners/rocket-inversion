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
package io.rcktapp.api.handler.s3;

import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.forty11.web.js.JS;
import io.forty11.web.js.JSArray;
import io.forty11.web.js.JSObject;
import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.ApiException;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Handler;
import io.rcktapp.api.Request;
import io.rcktapp.api.Request.Upload;
import io.rcktapp.api.Response;
import io.rcktapp.api.SC;
import io.rcktapp.api.Table;
import io.rcktapp.api.service.Service;
import io.rcktapp.rql.Rql;
import io.rcktapp.rql.s3.S3Rql;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.IoUtils;

/**
 * Accepts RQL parameters and responds with json or files to the client.
 * Special request parameters used by the GET handler: 
 * 'download' attempts to download the specified key.
 * 'marker' determines where paging should begin
 * 
 * Supports simple RQL functions: eq & sw
 * 
 * TODO it would be awesome if a user could request several files to be downloaded.
 * The files would be zipped and returned to the client.  A zip would be named
 * either 'files.zip' for various files, or 'sw_x_files.zip' where files that
 * 'start with' x are zipped.
 * 
 * TODO what to do about buckets containing '.'s within the name? ex:
 * Missing parent for map compression: api.collections.s3db_files.liftck.coms
 * Missing parent for map compression: api.collections.s3db_static-pages.liftck.coms
 * Missing parent for map compression: s3db.tables.files.liftck.com
 * Missing parent for map compression: s3db.tables.static-pages.liftck.com
 * 
 * Mar 6, 2019 - If a json body is received, it is expected that a meta update should
 * occur.  If a multipart form is received, it is expected that a binary file
 * was sent and possibly json
 * 
 * @author kfrankic
 *
 */
public class S3DbRestHandler implements Handler
{
   ObjectMapper mapper  = new ObjectMapper();
   Logger       log     = LoggerFactory.getLogger(S3DbRestHandler.class);

   int          maxRows = 100;
   String       s3DatePath = null;

   @Override
   public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {
      String method = req.getMethod();
      if ("GET".equalsIgnoreCase(method))
      {
         doGet(service, api, endpoint, action, chain, req, res);
      }
      else if ("POST".equalsIgnoreCase(method))
      {
         doPost(service, api, endpoint, action, chain, req, res);
      }
      else if ("PUT".equalsIgnoreCase(method))
      {
         doPut(service, api, endpoint, action, chain, req, res);
      }
      else
      {
         throw new ApiException(SC.SC_400_BAD_REQUEST, "The S3 handler only supports GET and POST/PUT requests");
      }
   }

   /**
    * All objects within a bucket can be retrieved by hitting the bucket without any parameters.
    * http://hostname/us/s3/bucketName
    * 
    * To narrow a result list, a prefix can be specified.  Only objects within the bucket with the specified prefix
    * will be returned.  To limit the results to those within a 'folder' the prefix include the folder name and end 
    * with a '/'.  
    * http://hostname/us/s3/bucketName?sw(key,someFolder/)
    * The following example would return all objects within 'someFolder' that start with 'xyz'
    * http://hostname/us/s3/bucketName?sw(key,someFolder/xyz)
    *
    * To obtain 'extended' meta of an object, two different methods can be used.  A request with the 
    * following params: sw(key,helloThere/)&eq(key,filename.json) is the same as a request with only 
    * eq(key,helloThere/filename.json)
    * 
    * To download an object, the same request as retrieving 'extended' meta is used, but a 'download'
    * parameter must be included.
    * 
    */
   public void doGet(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {

      Collection collection = findCollectionOrThrow404(api, chain, req);
      Table table = collection.getEntity().getTable();
      S3Db db = (S3Db) table.getDb();

      Integer pageSize = req.getParam("pageSize") != null ? Integer.parseInt(req.removeParam("pageSize")) : maxRows;

      S3Rql rql = (S3Rql) Rql.getRql(db.getType());
      S3Request s3Req = rql.buildS3Request(req, table, pageSize);


      if (s3Req.isDownload() || (!s3Req.isMeta() && db.isDefaultDownload()))
      {
         // path == /s3/bucketName?eq(key,filename)&download

         // If a prefix exists, it must be tacked onto the key.
         ResponseInputStream<GetObjectResponse> s3File = null;
         try {
            s3File = db.getDownload(s3Req);
         } catch (S3Exception exception) {
            if (exception.statusCode() == 304) {
               // This is how the aws api reacts to downloading something that doesn't match its constraint. -> https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
               log.info("File {} from bucket {} was not modified since it was last retrieved", s3Req.getKey(), s3Req.getBucket());
            }
            log.error("Failed to download file {} from bucket {}", s3Req.getKey(), s3Req.getBucket(), exception);
         }

         if (s3File == null) {
           res.setStatus(SC.SC_204_NO_CONTENT);
           return;
         } else {
           GetObjectResponse response = s3File.response();
           // relies on Servlet to close the stream.
           res.setInputStream(s3File);
           res.setContentType(response.contentType());

           res.addHeader("Content-Type", response.contentType());
           res.addHeader("Content-Length", Long.toString(response.contentLength()));
         }
      }
      else if (s3Req.getKey() != null)
      {
         // Attempt to retrieve the extended meta for the key.
         // If that does not exist, attempt to get a list of objects using the key as a prefix.

         // path == /s3/bucketName?eq(key,filename)
         // path == /s3/bucketName/key
         // retrieve the extended meta data of a file.

         JSObject json = null;

         try
         {
            HeadObjectResponse headResponse = db.headObject(s3Req);

            json = convertToJSObject(headResponse);
            String pathPrefix = req.getPath().substring(0, req.getPath().indexOf(req.getSubpath()));
            json.put("href", req.getApiUrl() + pathPrefix + s3Req.getBucket() + "/" + s3Req.getKey());

            res.setJson(json);
         }
         catch (Exception e)
         {
            log.warn("Attempting to retrieve as list after failing to obtain extended meta for key: " + s3Req.getKey());
         }

         if (json == null)
         {
            // TODO is there a way to prevent the req from adding the '/' onto it's path?

            // The key does not exist.  Perhaps the request was intended to be for an objects listing...
            // FYI: below, a '/' is added to the prefix for two reasons:
            // 1) by default, Inversion adds a '/' to the end of the path which is then removed during stmt creation,
            // so we dont know if the '/' is intended or not.
            // 2) if a '/' is NOT tacked onto the prefix, then 'this' directory will be returned as a prefix along with all files
            // that start with this prefix...meaning, NO inner directories or files will be returned.
            // To work around this limitation, if the user wants to specify a directory & file prefix, the 'sw' function should 
            // be used.  ex: sw(key,media/c) will return all files/directories that are within the media folder and start with 'c'
            getObjectsList(req, res, new S3Request(s3Req.getBucket(), s3Req.getKey() + "/", s3Req.getSize(), false, s3Req.isMeta(), s3Req.getMarker(), req.getHeader("If-None-Match")), db, mapper);
         }

      }
      else
      {
         getObjectsList(req, res, s3Req, db, mapper);
      }

      res.setStatus(SC.SC_200_OK);
   }

   private JSObject convertToJSObject(HeadObjectResponse response) throws JsonProcessingException {
      JSObject js = new JSObject();
      js.put("deleteMarker", response.deleteMarker());
      js.put("acceptRanges", response.acceptRanges());
      js.put("expiration", response.expiration());
      js.put("restore", response.restore());
      js.put("archiveStatus", response.archiveStatus());
      js.put("lastModified", response.lastModified().toEpochMilli());
      js.put("contentLength", response.contentLength());
      js.put("checksumCRC32", response.checksumCRC32());
      js.put("checksumCRC32C", response.checksumCRC32C());
      js.put("checksumCRC64NVME", response.checksumCRC64NVME());
      js.put("checksumSHA1", response.checksumSHA1());
      js.put("checksumSHA256", response.checksumSHA256());
      js.put("checksumType", response.checksumType());
      js.put("eTag", response.eTag().replace("\"", "")); // etag can be surrounded in quotes
      js.put("missingMeta", response.missingMeta());
      js.put("versionId", response.versionId());
      js.put("cacheControl", response.cacheControl());
      js.put("contentDisposition", response.contentDisposition());
      js.put("contentEncoding", response.contentEncoding());
      js.put("contentLanguage", response.contentLanguage());
      js.put("contentType", response.contentType());
      js.put("contentRange", response.contentRange());
      js.put("expires", response.expires());
      js.put("websiteRedirectLocation", response.websiteRedirectLocation());
      js.put("serverSideEncryption", response.serverSideEncryption());
      js.put("sseCustomerAlgorithm", response.sseCustomerAlgorithm());
      js.put("sseCustomerKeyMD5", response.sseCustomerKeyMD5());
      js.put("ssekmsKeyId", response.ssekmsKeyId());
      js.put("bucketKeyEnabled", response.bucketKeyEnabled());
      js.put("storageClass", response.storageClass());
      js.put("requestCharged", response.requestCharged());
      js.put("replicationStatus", response.replicationStatus());
      js.put("partsCount", response.partsCount());
      js.put("tagCount", response.tagCount());
      js.put("objectLockMode", response.objectLockMode());
      js.put("objectLockRetainUntilDate", response.objectLockRetainUntilDate());
      js.put("objectLockLegalHoldStatus", response.objectLockLegalHoldStatus());
      js.put("expiresString", response.expiresString());
      js.put("userMetadata", JS.toJSObject(mapper.writeValueAsString(response.metadata())));
      return js;
   }

   private void getObjectsList(Request req, Response res, S3Request s3Req, S3Db db, ObjectMapper mapper) throws Exception
   {
      // path == /s3/bucketName
      // path == /s3/bucketName/inner/folder
      // retrieve as much meta data as possible about the files in the bucket

      ListObjectsResponse listing = db.getCoreMetaData(s3Req);

      JSObject json = new JSObject();

      // standard Inversion meta includes:
      // "rowCount": x, - there is currently no way of knowing this.
      // "pageNum": x, - can't know.
      // "pageSize": x, - know.
      // "pageCount": x, - can't know.
      // "prev": null, - could know, if passed as req param.
      // "next": "http://localhost:8080/api/lift/us/elastic/ads?&pageSize=100&sort=id&source=id,json.id,json.modifiedat&pageNum=2"
      JSObject jsMeta = new JSObject();
      jsMeta.put("pageSize", listing.maxKeys());
      jsMeta.put("prev", null);
      String nextMarker = "";
      if (listing.isTruncated())
      {
         String query = req.getUrl().getQuery();
         nextMarker = (query.length() == 0 ? ("?marker=" + listing.nextMarker()) : ("&marker=" + listing.nextMarker()));
      }
      jsMeta.put("next", listing.isTruncated() ? req.getUrl().toString() + nextMarker : null);
      json.put("meta", jsMeta);

      List<CommonPrefix> directoryList = listing.commonPrefixes();
      List<S3Object> fileList = new ArrayList<>(listing.contents()); // S3 SDK returns an unmodifiable list, but we remove from this fileList below

      JSArray data = new JSArray();

      // alphabetize the data returned to the client...
      while (!directoryList.isEmpty())
      {
         CommonPrefix directory = directoryList.get(0);
         if (!fileList.isEmpty())
         {
            S3Object file = fileList.get(0);
            if (directory.prefix().compareToIgnoreCase(file.key()) < 0)
            {
               // directory name comes before file name
               data.add(buildListObj(req.getApiUrl() + req.getPath() + directory, null, null, false));
               directoryList.remove(0);
            }
            else
            {
               // file name comes before directory
               data.add(buildListObj(req.getApiUrl() + req.getPath() + file.key(), Date.from(file.lastModified()), file.size(), true));
               fileList.remove(0);
            }
         }
         else
         {
            data.add(buildListObj(req.getApiUrl() + req.getPath() + directory, null, null, false));
            directoryList.remove(0);
         }
      }

      while (!fileList.isEmpty())
      {
         S3Object file = fileList.remove(0);
         data.add(buildListObj(req.getApiUrl() + req.getPath() + file.key(), Date.from(file.lastModified()), file.size(), true));
      }

      json.put("data", data);

      res.setJson(json);

   }

   /**
    * Use the 'sw' function to post a file to a specific location within a bucket; ex: sw(key, media/).  
    * Otherwise, the file will be saved to the specified bucket's root.  By default, the uploaded file 
    * name will be used when storing the file to s3.  A file name can specified by using the 'eq' function;
    * ex: eq(key, newFile.name)
    * 
    * Custom metadata can be added to the file by setting a 'header prefix' value during configuration of 
    * this class.  All headers sent by the client that use this prefix will be applied to the s3 object's
    * custom metadata.
    * 
    * **Note** That user-metadata for an object is limited by the HTTP requestheader limit. All HTTP 
    * headers included in a request (including usermetadata headers and other standard HTTP headers) must 
    * be less than 8KB
    * 
    * **NOTE** A 'header prefix' AND custom Content-Type header MUST be applied if you want the correct
    * content-type applied to the s3 object.  ex: headerPrefix = "s3-";  The request header would include:
    * s3-Content-Type=application/json if you wanted an object stored with a json content type.
    */
   private void doPost(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {

      Collection collection = findCollectionOrThrow404(api, chain, req);
      Table table = collection.getEntity().getTable();
      S3Db db = (S3Db) table.getDb();

      S3Rql rql = (S3Rql) Rql.getRql(db.getType());

      if (s3DatePath != null)
      {
         String datePath = new SimpleDateFormat(s3DatePath).format(new Date());
         String newPath = req.getSubpath() + datePath;
         req.setSubpath(newPath);
      }

      S3Request s3Req = rql.buildS3Request(req, table, null);

      String key = s3Req.getKey();

      List<Upload> uploads = req.getUploads();

      if (uploads.size() > 0)
      {
         DigestInputStream uploadStream = null;
         Upload upload = uploads.get(0);

         uploadStream = new DigestInputStream(upload.getInputStream(), MessageDigest.getInstance("MD5"));

         Map<String, String> meta = new HashMap<>();
         String contentType = null;

         String metaParam = req.getParam("meta");

         // set custom metadata for the file
         if (metaParam != null)
         {
            JSObject metaJs = JS.toJSObject(metaParam);
            Map<String, String> metaMap = metaJs.asMap();

            for (Map.Entry<String, String> entry : metaMap.entrySet())
            {
               String metaKey = entry.getKey();

               if (metaKey.equalsIgnoreCase("content-type"))
               {
                  meta.put("Content-Type", entry.getValue());
               }
               else if (metaKey.equalsIgnoreCase("name"))
               {
                  key = entry.getValue();
               }
               else
               {
                  meta.put(metaKey, entry.getValue());
               }
            }

         }

         PutObjectResponse result = db.saveFile(uploadStream, s3Req.getBucket(), key, contentType, upload.getFileSize(), meta);
         if (result == null) {
            throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Failed to POST/PUT file to s3: " + key);
         }

         // not including the result object as it contains confusing/pointless data.
         // such as a 'content-length' of 0, because it's the content-length of the response, not the 
         // size of the upload.
         JSObject json = new JSObject();

         json.put("href", req.getApiUrl() + req.getPath() + key);

         res.setJson(json);

      }

      res.setStatus(SC.SC_200_OK);
   }

   private void doPut(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
   {
      Collection collection = findCollectionOrThrow404(api, chain, req);
      Table table = collection.getEntity().getTable();
      S3Db db = (S3Db) table.getDb();

      // Only be updating the meta of a file at this time.  Renaming or moving a file 
      // should also be handled by db.updateObject() but neither are currently implemented.

      JSObject metaJson = req.getJson();

      String key = null;
      try
      {
         key = metaJson.getString("name");
      }
      catch (Exception e)
      {
         throw new ApiException("When updating metadata, a 'name' must be specified");
      }

      // All previous metadata will be wiped out.
      Map<String, String> meta = buildMetadata(metaJson);

      CopyObjectResult copy = db.updateObject(table.getName(), key, table.getName(), key, meta);

      // the copy result doesn't contain much helpful data.
      JSObject json = JS.toJSObject(mapper.writeValueAsString(copy));

      json.put("href", req.getApiUrl() + req.getPath() + key);

      res.setJson(json);
      res.setStatus(SC.SC_200_OK);

   }

   private Collection findCollectionOrThrow404(Api api, Chain chain, Request req) throws Exception
   {
      Collection collection = api.getCollection(req.getCollectionKey(), S3Db.class);

      if (collection == null)
      {
         throw new ApiException(SC.SC_404_NOT_FOUND, "An s3 bucket is not configured for this collection key, please edit your query or your config and try again.");
      }

      if (!(collection.getEntity().getTable().getDb() instanceof S3Db))
      {
         throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Bad server configuration. The endpoint is hitting the s3 handler, but this collection is not related to a s3db");
      }

      return collection;
   }

   private Map<String, String> buildMetadata(JSObject metaJs)
   {
      Map<String, String> meta = null;

      if (metaJs != null)
      {
         // All previous metadata will be wiped out.
         meta = new HashMap<>();

         Map<String, String> metaMap = metaJs.asMap();

         for (Map.Entry<String, String> entry : metaMap.entrySet())
         {
            String metaKey = entry.getKey();

            switch (metaKey.toLowerCase())
            {
               case "content-type":
                  meta.put("Content-Type", entry.getValue());
                  break;
               case "name":
               case "tenantid":
                  break;
               default :
                  meta.put(metaKey, entry.getValue());
            }
         }
      }

      return meta;
   }

   private JSObject buildListObj(String href, Date lastModified, Long size, boolean isFile)
   {
      JSObject jsObj = new JSObject();
      jsObj.put("href", href);
      if (lastModified != null)
         jsObj.put("lastModified", lastModified);
      if (size != null)
         jsObj.put("size", size);
      jsObj.put("isFile", isFile);

      return jsObj;
   }

   public static void main(String[] args)
   {
      System.out.println("hello".compareToIgnoreCase("world")); // -15
      System.out.println("world".compareToIgnoreCase("hello")); // 15
   }

}
