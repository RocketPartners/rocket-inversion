package io.rcktapp.api.handler.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.rcktapp.api.Attribute;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Column;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;
import io.rcktapp.rql.s3.S3Rql;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

/**
 * Bucket ~= Table
 * Bucket Object field key value ~= Column
 * Only mapping the key field since it is the only way to query anything within S3,
 * since, as of now, you can't request files by size, or content-type, or some
 * custom header.
 *
 * @author kfrankic
 *
 */
public class S3Db extends Db
{
   static
   {
      try
      {
         //bootstraps the S3Rql type
         Class.forName(S3Rql.class.getName());
      }
      catch (Exception ex)
      {
         ex.printStackTrace();
      }
   }

   protected String  accessKey       = null;
   protected String  secretKey       = null;
   protected String  awsRegion       = null;

   // If you want to limit which buckets are included, set this to a csv of bucket names.
   // Leave this null if you want to configure all buckets in the account.
   protected String  buckets         = null;

   // Set this to true if you would like the default behavior to download the file instead of returning the files meta data
   protected boolean defaultDownload = false;

   private S3Client client          = null;

   /**
    * @see io.rcktapp.api.Db#bootstrapApi()
    */
   @Override
   public void bootstrapApi() throws Exception
   {
      client = getS3Client();

      setType("s3");

      List<String> bucketNames = new ArrayList<>();
      if (buckets != null)
      {
         // use configured buckets only
         bucketNames.addAll(Arrays.asList(buckets.split(",")));
      }
      else
      {
         // get all the buckets this account has access to.
         List<Bucket> bucketList = client.listBuckets().buckets();
         for (Bucket bucket : bucketList)
         {
            bucketNames.add(bucket.name());
         }
      }

      for (String bucketName : bucketNames)
      {
         Table table = new Table(this, bucketName.trim());

         // Hardcoding 'key' as the only column as there is no useful way to use the other metadata
         // for querying
         // Other core metadata includes: eTag, size, lastModified, storageClass
         table.addColumn(new Column(table, "key", "java.lang.String", false));
         addTable(table);
      }

      configApi();

      client.close();

   }

   private void configApi()
   {
      for (Table t : getTables())
      {
         List<Column> cols = t.getColumns();
         Collection collection = new Collection();

         collection.setName(lowercaseAndPluralizeString(t.getName()));

         Entity entity = new Entity();
         entity.setTbl(t);
         entity.setHint(t.getName());
         entity.setCollection(collection);

         collection.setEntity(entity);

         for (Column col : cols)
         {
            Attribute attr = new Attribute();
            attr.setEntity(entity);
            attr.setName(col.getName());
            attr.setColumn(col);
            attr.setHint(col.getTable().getName() + "." + col.getName());
            attr.setType(col.getType());

            entity.addAttribute(attr);
         }

         api.addCollection(collection);
         collection.setApi(api);
      }
   }

   private S3Client getS3Client()
   {
      if (client != null)
         return client;

      S3ClientBuilder builder;
      if (accessKey != null)
      {
         AwsBasicCredentials creds = AwsBasicCredentials.create(accessKey, secretKey);
         builder = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(creds));
      }
      else
      {
         builder = S3Client.builder();
      }

      if (awsRegion != null)
      {
         builder.region(Region.of(awsRegion));
      }
      return builder.build();
   }

   public ResponseInputStream<GetObjectResponse> getDownload(S3Request req)
   {
      client = getS3Client();
      GetObjectRequest gob = GetObjectRequest.builder()
              .bucket(req.getBucket())
              .key(req.getKey())
              .ifNoneMatch(req.getEtag()) //TODO CONNOR: test
              .build();
      return client.getObject(gob);
   }

   public HeadObjectResponse headObject(S3Request req)
   {
      client = getS3Client();
      HeadObjectRequest hob = HeadObjectRequest.builder()
              .bucket(req.getBucket())
              .key(req.getKey())
              .ifNoneMatch(req.getEtag()) //TODO CONNOR: test
              .build();
      return client.headObject(hob); //TODO CONNOR: ensure calling methods are adding prefix to key
   }

   public PutObjectResponse saveFile(InputStream inputStream, String bucketName, String key, String contentType, Long contentLength, Map<String, String> userMetadata) throws IOException {
      client = getS3Client();
      return client.putObject(PutObjectRequest.builder()
              .bucket(bucketName)
              .key(key)
              .contentType(contentType)
              .contentLength(contentLength)
              .metadata(userMetadata)
              .build(), RequestBody.fromBytes(IoUtils.toByteArray(inputStream)));
   }

   /**
    *
    * @param s3Req - the s3 request
    * @return
    */
   public ListObjectsResponse getCoreMetaData(S3Request s3Req)
   {
      client = getS3Client();

      ListObjectsRequest.Builder reqBuilder = ListObjectsRequest.builder()
              .bucket(s3Req.getBucket())
              .delimiter("/")
              .marker(s3Req.getMarker())
              .prefix(s3Req.getKey());

      if (s3Req.getSize() >= 0) {
         reqBuilder.maxKeys(s3Req.getSize()); // TODO fix pagesize...currently always set to 1000 ... tied to 'size' but not 'pagesize'?
      }
      return client.listObjects(reqBuilder.build());
   }

   public CopyObjectResult updateObject(String bucket, String key, String newBucket, String newKey, Map<String, String> meta)
   {
      client = getS3Client();

      CopyObjectRequest copyReq;

      if (meta != null)
      {
         copyReq = CopyObjectRequest.builder()
                 .sourceBucket(bucket)
                 .sourceBucket(key)
                 .destinationBucket(newBucket)
                 .destinationKey(newKey)
                 .metadata(meta) //TODO CONNOR: check this meta data mapping is right
                 .build();
      }
      else
      {
         // rename or move request
         copyReq = CopyObjectRequest.builder()
                 .sourceBucket(bucket)
                 .sourceBucket(key)
                 .destinationBucket(newBucket)
                 .destinationKey(newKey)
                 .build();
      }

      // TODO if the key and newKey are not equal, (or the bucket and newBucket) delete the old key file
      return client.copyObject(copyReq).copyObjectResult();
   }

   public String getBuckets()
   {
      return buckets;
   }

   public void setBuckets(String buckets)
   {
      this.buckets = buckets;
   }

   public boolean isDefaultDownload()
   {
      return defaultDownload;
   }

   public void setDefaultDownload(boolean defaultDownload)
   {
      this.defaultDownload = defaultDownload;
   }

}
