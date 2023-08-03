package io.rcktapp.api.handler.firehose;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;
import org.atteo.evo.inflector.English;

/**
 * A db class that allows access to datastream streams.
 */
public class DataStreamDb extends Db {
    protected String awsAccessKey = null;
    protected String awsSecretKey = null;
    protected String awsRegion = null;
    AmazonKinesisAsync datastreamClient = null;

    private AWSCredentialsProvider getCreds() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
    }

    AmazonKinesisAsync getClient() {
        return datastreamClient;
    }

    @Override
    public void bootstrapApi() {
        datastreamClient = AmazonKinesisAsyncClientBuilder.standard().withRegion(awsRegion).withCredentials(getCreds()).build();

        this.setType("datastream");

        datastreamClient.listStreams().getStreamNames().forEach(streamName -> {
            Table table = new Table(this, streamName);
            addTable(table);

            Collection collection = new Collection();
            String collectionName = streamName;
            if (!collectionName.endsWith("s"))
                collectionName = English.plural(collectionName);

            collection.setName(collectionName);

            Entity entity = new Entity();
            entity.setTbl(table);
            entity.setHint(table.getName());
            entity.setCollection(collection);

            collection.setEntity(entity);

            api.addCollection(collection);
        });
    }
}
