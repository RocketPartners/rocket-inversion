package io.rocketpartners.cloud.action.s3;

import java.util.List;
import java.util.Map;

import io.rocketpartners.cloud.model.Db;
import io.rocketpartners.cloud.model.Results;
import io.rocketpartners.cloud.model.Table;
import io.rocketpartners.cloud.rql.Term;
import io.rocketpartners.cloud.utils.Rows.Row;
import io.rocketpartners.cloud.utils.Utils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;

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
public class S3Db extends Db<S3Db>
{
   protected String awsAccessKey = null;
   protected String awsSecretKey = null;
   protected String awsRegion    = null;

   protected String bucket       = null;
   protected String basePath     = null;
   protected String includePaths = null;

   private S3Client client       = null;

   /**
    * @see io.rcktapp.api.Db#bootstrapApi()
    */
   @Override
   protected void startup0()
   {
      S3Client client = getS3Client();

      // get all the buckets this account has access to.
      List<Bucket> bucketList = client.listBuckets().buckets();

      for (Bucket bucket : bucketList)
      {
         Table table = new Table(this, bucket.name());
         // Hardcoding 'key' as the only column as there is no useful way to use the other metadata
         // for querying 
         // Other core metadata includes: eTag, size, lastModified, storageClass
         table.makeColumn("key", String.class.getName());
         withTable(table);

         api.makeCollection(table, beautifyCollectionName(table.getName()));
      }
   }

   @Override
   protected void shutdown0() {
      if (client != null) {
         client.close();
      }
   }

   @Override
   public Results<Row> select(Table table, List<Term> columnMappedTerms) throws Exception
   {
      S3DbQuery query = new S3DbQuery(table, columnMappedTerms);
      return query.doSelect();
   }

   @Override
   public void delete(Table table, String entityKey) throws Exception
   {
      // TODO Auto-generated method stub

   }

   @Override
   public String upsert(Table table, Map<String, Object> rows) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   public S3Client getS3Client()
   {
      return getS3Client(awsRegion, awsAccessKey, awsSecretKey);
   }

   public S3Client getS3Client(String awsRegion, String awsAccessKey, String awsSecretKey)
   {
      if (this.client == null)
      {
         synchronized (this)
         {
            if (this.client == null)
            {
               awsRegion = Utils.findSysEnvPropStr(getName() + ".awsRegion", awsRegion);
               awsAccessKey = Utils.findSysEnvPropStr(getName() + ".awsAccessKey", awsAccessKey);
               awsSecretKey = Utils.findSysEnvPropStr(getName() + ".awsSecretKey", awsSecretKey);

               S3ClientBuilder builder = S3Client.builder();

               if (!Utils.empty(awsRegion))
                  builder.region(Region.of(awsRegion));

               if (!Utils.empty(awsAccessKey) && !Utils.empty(awsSecretKey))
               {
                  AwsBasicCredentials creds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);
                  builder.credentialsProvider(StaticCredentialsProvider.create(creds));
               }

               client = builder.build();
            }
         }
      }

      return this.client;
   }

   public S3Db withAwsRegion(String awsRegion)
   {
      this.awsRegion = awsRegion;
      return this;
   }

   public S3Db withAwsAccessKey(String awsAccessKey)
   {
      this.awsAccessKey = awsAccessKey;
      return this;
   }

   public S3Db withAwsSecretKey(String awsSecretKey)
   {
      this.awsSecretKey = awsSecretKey;
      return this;
   }

   public S3Db withBucket(String bucket)
   {
      this.bucket = bucket;
      return this;
   }

}
