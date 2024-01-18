package io.rcktapp.api.handler.firehose;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import io.forty11.j.J;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.atteo.evo.inflector.English;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EqualsAndHashCode(callSuper = true)
@Slf4j
public class FirehoseDb extends Db
{
    protected String awsAccessKey = null;
    protected String awsSecretKey = null;
    protected String awsRegion = null;
    /**
     * If you set an allow regex pattern, all of the stream names that we will use MUST match it - even the ones in includeStreams.
     */
    protected String allowPattern = null;
    /**
     * If you set a deny regex pattern, all of the stream names that we will use must NOT match it - even the ones in includeStreams.
     */
    protected String denyPattern = null;

    /**
     * A CSV of pipe delimited collection name to table name pairs.
     * <p>
     * Example: firehosedb.includeStreams=impression|liftck-player9-impression
     * <p>
     * Or if the collection name is the name as the table name you can just send a the name
     * <p>
     * Example: firehosedb.includeStreams=liftck-player9-impression
     */
    protected String includeStreams;

    protected AmazonKinesisFirehoseAsync firehoseClient = null;

    public FirehoseDb() {
        super();
        setType("firehose");
    }

    @Override
    public void bootstrapApi() {
        // our local streams
        Stream<Pair<String, String>> inKinesis = firehoseNameStream().map(name -> Pair.of(name.toLowerCase(), name));
        // our defined aliases
        Stream<Pair<String, String>> inConfiguration = Stream.of(Optional.ofNullable(includeStreams).orElse("").split(","))
                .map(part -> part.split("\\|"))
                .map(arr -> Pair.of(arr[0], arr.length > 1 ? arr[1] : arr[0]));

        this.setType("firehose");
        Stream.concat(inConfiguration, inKinesis).collect(Collectors.toList()).forEach(stream -> {
            log.info("bootstrap {} stream {}", getType(), stream);
            String collectionName = stream.getKey();
            String streamName = stream.getValue();

            if (!J.empty(allowPattern) && !collectionName.matches(allowPattern)) {
                log.info("skipping {} stream {} because it doesn't match allow pattern {}", getType(), stream, allowPattern);
                return;
            }

            if (!J.empty(denyPattern) && collectionName.matches(denyPattern)) {
                log.info("skipping {} stream {} because it matches deny pattern {}", getType(), stream, denyPattern);
                return;
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
        });
    }

    public synchronized AmazonKinesisFirehoseAsync getFirehoseClient() {
        if (this.firehoseClient != null)
            return this.firehoseClient;
        AmazonKinesisFirehoseAsyncClientBuilder builder = AmazonKinesisFirehoseAsyncClientBuilder.standard();
        if (!J.empty(awsRegion))
            builder.withRegion(awsRegion);

        if (!J.empty(awsAccessKey) && !J.empty(awsSecretKey)) {
            BasicAWSCredentials creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            builder.withCredentials(new AWSStaticCredentialsProvider(creds));
        }

        firehoseClient = builder.build();
        return firehoseClient;
    }

    public Stream<String> firehoseNameStream() {
        return new DeliveryStreamNameSpliterator(getFirehoseClient()).stream();
    }
}
