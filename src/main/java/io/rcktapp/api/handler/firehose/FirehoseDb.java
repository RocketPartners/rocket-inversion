package io.rcktapp.api.handler.firehose;

import org.atteo.evo.inflector.English;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClientBuilder;

import io.forty11.j.J;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;

public class FirehoseDb extends Db
{
   protected String awsAccessKey = null;
   protected String awsSecretKey = null;
   protected String awsRegion    = null;

   /**
    * A CSV of pipe delimited collection name to table name pairs.
    *
    * Example: firehosedb.includeStreams=impression|liftck-player9-impression
    *
    * Or if the collection name is the name as the table name you can just send a the name
    *
    * Example: firehosedb.includeStreams=liftck-player9-impression
    */
   protected String includeStreams;

   FirehoseAsyncClient firehoseClient = null;

   @Override
   public void bootstrapApi() throws Exception
   {
      FirehoseAsyncClient firehoseClient = getFirehoseClient();

      this.setType("firehose");

      if (!J.empty(includeStreams))
      {
         String[] parts = includeStreams.split(",");
         for (String part : parts)
         {
            String[] arr = part.split("\\|");
            String collectionName = arr[0];
            String streamName = collectionName;
            if (arr.length > 1)
            {
               streamName = arr[1];
            }

            Table table = new Table(this, streamName);
            addTable(table);

            Collection collection = new Collection();
            if (!collectionName.endsWith("s"))
               collectionName = English.plural(collectionName);

            collection.setName(collectionName);

            Entity entity = new Entity();
            entity.setTbl(table);
            entity.setHint(table.getName());
            entity.setCollection(collection);

            collection.setEntity(entity);

            api.addCollection(collection);
         }
      }
      else
      {
         log.warn("FirehoseDb must have 'includeStreams' configured to be used");
      }
   }

   public FirehoseAsyncClient getFirehoseClient()
   {
      if (this.firehoseClient == null)
      {
         synchronized (this)
         {
            if (this.firehoseClient == null)
            {
               FirehoseAsyncClientBuilder builder = FirehoseAsyncClient.builder();
               if (!J.empty(awsRegion))
                  builder.region(Region.of(awsRegion));

               if (!J.empty(awsAccessKey) && !J.empty(awsSecretKey))
               {
                  AwsBasicCredentials creds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);
                  builder.credentialsProvider(StaticCredentialsProvider.create(creds));
               }

               firehoseClient = builder.build();
            }
         }
      }

      return firehoseClient;
   }

}
