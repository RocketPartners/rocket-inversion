package io.rcktapp.api.handler.firehose;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import io.forty11.j.J;
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
    KinesisAsyncClient datastreamClient = null;

    KinesisAsyncClient getClient() {
        return datastreamClient;
    }

    @Override
    public void bootstrapApi() {
        KinesisAsyncClientBuilder builder = KinesisAsyncClient.builder();
        if (!J.empty(awsRegion))
            builder.region(Region.of(awsRegion));
        if (!J.empty(awsAccessKey) && !J.empty(awsSecretKey)) {
            AwsBasicCredentials creds = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);
            builder.credentialsProvider(StaticCredentialsProvider.create(creds));
        }
        datastreamClient = builder.build();

        this.setType("datastream");

        datastreamClient.listStreams().join().streamNames().forEach(streamName -> {
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
