package io.rcktapp.api.handler.firehose;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsResult;
import io.forty11.j.J;
import io.rcktapp.api.Collection;
import io.rcktapp.api.Db;
import io.rcktapp.api.Entity;
import io.rcktapp.api.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.atteo.evo.inflector.English;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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

    AmazonKinesisFirehoseAsync firehoseClient = null;

    public FirehoseDb() {
        super();
        setType("firehose");
    }

    @Override
    public void bootstrapApi() throws Exception {
        List<Pair<String, String>> nameActuals = new ArrayList<>();
        // our local streams
        // getFirehoseClient().listDeliveryStreams(new ListDeliveryStreamsRequest().withDeliveryStreamType("DirectPut")).getDeliveryStreamNames().forEach(name -> nameActuals.add(Pair.of(name.toLowerCase(), name)));

        listAllDeliveryStreamNames().forEach(name -> nameActuals.add(Pair.of(name.toLowerCase(), name)));

        // our defined aliases
        Stream.of(Optional.ofNullable(includeStreams).orElse("").split(",")).map(part -> part.split("\\|")).forEach(arr -> nameActuals.add(Pair.of(arr[0], arr.length > 1 ? arr[1] : arr[0])));

        this.setType("firehose");

        for (Pair<String, String> stream : nameActuals) {
            log.info("bootstrap {} stream {}", getType(), stream);
            String collectionName = stream.getKey();
            String streamName = stream.getValue();

            if (!J.empty(allowPattern) && !collectionName.matches(allowPattern)) {
                log.info("skipping {} stream {} because it doesn't match allow pattern {}", getType(), stream, allowPattern);
                continue;
            }

            if (!J.empty(denyPattern) && collectionName.matches(denyPattern)) {
                log.info("skipping {} stream {} because it matches deny pattern {}", getType(), stream, denyPattern);
                continue;
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

    private List<String> listAllDeliveryStreamNames() {
        ListDeliveryStreamsRequest listDeliveryStreamsRequest = new ListDeliveryStreamsRequest().withDeliveryStreamType("DirectPut");
        ListDeliveryStreamsResult listDeliveryStreamsResult;
        List<String> deliveryStreamNames = new ArrayList<>();

        do {
            if (!deliveryStreamNames.isEmpty()) {
                listDeliveryStreamsRequest.setExclusiveStartDeliveryStreamName(deliveryStreamNames.get(deliveryStreamNames.size() - 1));
            }

            listDeliveryStreamsResult = getFirehoseClient().listDeliveryStreams(listDeliveryStreamsRequest);

            deliveryStreamNames.addAll(listDeliveryStreamsResult.getDeliveryStreamNames());

        } while (listDeliveryStreamsResult.getHasMoreDeliveryStreams());

        return deliveryStreamNames;
    }

    public AmazonKinesisFirehoseAsync getFirehoseClient() {
        if (this.firehoseClient == null) {
            synchronized (this) {
                if (this.firehoseClient == null) {
                    AmazonKinesisFirehoseAsyncClientBuilder builder = AmazonKinesisFirehoseAsyncClientBuilder.standard();
                    if (!J.empty(awsRegion))
                        builder.withRegion(awsRegion);

                    if (!J.empty(awsAccessKey) && !J.empty(awsSecretKey)) {
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
