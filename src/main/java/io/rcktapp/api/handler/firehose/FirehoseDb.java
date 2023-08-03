package io.rcktapp.api.handler.firehose;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import io.forty11.j.J;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;
import org.atteo.evo.inflector.English;

public class FirehoseDb extends Db
{
   protected String      awsAccessKey   = null;
   protected String      awsSecretKey   = null;
   protected String      awsRegion      = null;

   /**
    * A CSV of pipe delimited collection name to table name pairs.
    *
    * Example: firehosedb.includeStreams=impression|liftck-player9-impression
    *
    * Or if the collection name is the name as the table name you can just send a the name
    *
    * Example: firehosedb.includeStreams=liftck-player9-impression
    */
   protected String      includeStreams;

   AmazonKinesisFirehoseAsync firehoseClient = null;

   @Override
   public void bootstrapApi() throws Exception
   {
      AmazonKinesisFirehose firehoseClient = getFirehoseClient();

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

   public AmazonKinesisFirehoseAsync getFirehoseClient()
   {
      if (this.firehoseClient == null)
      {
         synchronized (this)
         {
            if (this.firehoseClient == null)
            {
               AmazonKinesisFirehoseAsyncClientBuilder builder = AmazonKinesisFirehoseAsyncClientBuilder.standard();
               if (!J.empty(awsRegion))
                  builder.withRegion(awsRegion);

               if (!J.empty(awsAccessKey) && !J.empty(awsSecretKey))
               {
                  BasicAWSCredentials creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
                  builder.withCredentials(new AWSStaticCredentialsProvider(creds));
               }

               firehoseClient = builder.build();
            }
         }
      }

      return firehoseClient;
   }

}
