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
package io.rocketpartners.cloud.action.firehose;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import io.rocketpartners.cloud.model.ApiException;
import io.rocketpartners.cloud.model.Collection;
import io.rocketpartners.cloud.model.Db;
import io.rocketpartners.cloud.model.Entity;
import io.rocketpartners.cloud.model.JSNode;
import io.rocketpartners.cloud.model.Results;
import io.rocketpartners.cloud.model.SC;
import io.rocketpartners.cloud.model.Table;
import io.rocketpartners.cloud.rql.Term;
import io.rocketpartners.cloud.utils.English;
import io.rocketpartners.cloud.utils.Rows.Row;
import io.rocketpartners.cloud.utils.Utils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.rocketpartners.cloud.utils.Utils.empty;

/**
 * Posts records to a mapped AWS Kinesis Firehose stream.
 * <p>
 * When you PUT/POST a:
 * <ul>
 * <li>a JSON object - it is submitted as a single record
 * <li>a JSON array - each element in the array is submitted as a single record.
 * </ul>
 * <p>
 * Unless <code>jsonPrettyPrint</code> is set to <code>true</code> all JSON
 * records are stringified without return characters.
 * <p>
 * All records are always submitted in batches of up to <code>batchMax</code>.
 * You can submit more than <code>batchMax</code> to the handler, and it will try to
 * send as many batches as required.
 * <p>
 * If <code>jsonSeparator</code> is not null (it is '\n' by default) and the
 * stringified record does not end in <code>separator</code>,
 * <code>separator</code> will be appended to the record.
 * <p>
 * If your firehose is Redshift, you probably want to leave <code>jsonLowercaseNames</code>
 * at its default which is true.  Redshift only matches to lowercase names on COPY.
 * <p>
 * The underlying Firehose stream is naturally available, but you can also create an alias with
 * the FirehoseDb.includeStreams property
 * <p>
 * You may also define FirehoseDb.allowPattern as a regex to only make available specific matching streams
 * <p>
 * You may also define FirehoseDb.denyPattern as a regex to exclude sensitive streams from snooze's availability
 *
 * @author wells
 */
public class FirehoseDb extends Db<FirehoseDb> {
    /**
     * A CSV of pipe delimited collection name to table name pairs.
     * <p>
     * Example: firehosedb.includeStreams=impression|liftck-player9-impression
     * <p>
     * Or if the collection name is the name as the table name you can just send the name
     * <p>
     * Example: firehosedb.includeStreams=liftck-player9-impression
     */
    protected String includeStreams = null;
    /**
     * If you set an allow regex pattern, all the stream names that we will use MUST match it - even the ones
     * in includeStreams.
     */
    protected String allowPattern = null;
    /**
     * If you set a deny regex pattern, all the stream names that we will use must NOT match it - even the ones
     * in includeStreams.
     */
    protected String denyPattern = null;

    protected String awsAccessKey = null;
    protected String awsSecretKey = null;
    protected String awsRegion = null;

    protected AmazonKinesisFirehose firehoseClient = null;

    protected int batchMax = 500;
    protected String jsonSeparator = "\n";
    protected boolean jsonPrettyPrint = false;
    protected boolean jsonLowercaseNames = true;

    public FirehoseDb() {
        this.withType("firehose");
    }

    @Override
    protected void startup0() {
        List<Pair<String, String>> nameActualPairList = new ArrayList<>();
        // our local streams
        getFirehoseClient().listDeliveryStreams(new ListDeliveryStreamsRequest().withDeliveryStreamType("DirectPut")).getDeliveryStreamNames().forEach(name -> nameActualPairList.add(Pair.of(name.toLowerCase(), name.toLowerCase())));
        // our defined aliases
        Stream.of(includeStreams.split(",")).map(part -> part.split("\\|")).forEach(arr -> nameActualPairList.add(Pair.of(arr[0], arr.length > 1 ? arr[1] : arr[0])));

        for (Pair<String, String> stream : nameActualPairList) {
            log.info("bootstrap {} stream {}", getType(), stream);
            String collectionName = stream.getKey();
            String streamName = stream.getValue();

            if (!empty(allowPattern) && !collectionName.matches(allowPattern)) {
                log.info("skipping {} stream {} because it doesn't match allow pattern {}", getType(), stream, allowPattern);
                continue;
            }

            if (!empty(denyPattern) && collectionName.matches(denyPattern)) {
                log.info("skipping {} stream {} because it matches deny pattern {}", getType(), stream, denyPattern);
                continue;
            }

            Table table = new Table(this, streamName);
            withTable(table);

            Collection collection = new Collection();
            if (!collectionName.endsWith("s"))
                collectionName = English.plural(collectionName);

            collection.withName(collectionName);

            Entity entity = new Entity();
            entity.withTable(table);
            entity.withHint(table.getName());
            entity.withCollection(collection);

            collection.withEntity(entity);

            api.withCollection(collection);
        }
    }

    @Override
    public Results<Row> select(Table table, List<Term> columnMappedTerms) throws Exception {
        throw new ApiException(SC.SC_400_BAD_REQUEST, "The Firehose handler only supports PUT/POST operations...GET and DELETE don't make sense.");
    }

    @Override
    public void delete(Table table, String entityKey) throws Exception {
        throw new ApiException(SC.SC_400_BAD_REQUEST, "The Firehose handler only supports PUT/POST operations...GET and DELETE don't make sense.");
    }

    @Override
    public String upsert(Table table, Map<String, Object> row) throws Exception {
        List<String> keys = upsert(table, Arrays.asList(row));
        if (keys != null && !keys.isEmpty())
            return keys.get(0);

        return null;
    }

    @Override
    public List upsert(Table table, List<Map<String, Object>> rows) throws Exception {
        List<Record> batch = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            String string = new JSNode(rows.get(i)).toString(jsonPrettyPrint, jsonLowercaseNames);

            if (jsonSeparator != null && !string.endsWith(jsonSeparator))
                string += jsonSeparator;

            batch.add(new Record().withData(ByteBuffer.wrap(string.getBytes())));

            if (i > 0 && i % batchMax == 0) {
                getFirehoseClient().putRecordBatch(new PutRecordBatchRequest().withDeliveryStreamName(table.getName()).withRecords(batch));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            getFirehoseClient().putRecordBatch(new PutRecordBatchRequest().withDeliveryStreamName(table.getName()).withRecords(batch));
        }

        return Collections.emptyList();
    }

    public AmazonKinesisFirehose getFirehoseClient() {
        return getFirehoseClient(awsRegion, awsAccessKey, awsSecretKey);
    }

    public AmazonKinesisFirehose getFirehoseClient(String awsRegion, String awsAccessKey, String awsSecretKey) {
        synchronized (this) {
            if (this.firehoseClient == null) {
                awsRegion = Utils.findSysEnvPropStr(getName() + ".awsRegion", awsRegion);
                awsAccessKey = Utils.findSysEnvPropStr(getName() + ".awsAccessKey", awsAccessKey);
                awsSecretKey = Utils.findSysEnvPropStr(getName() + ".awsSecretKey", awsSecretKey);

                AmazonKinesisFirehoseClientBuilder builder = AmazonKinesisFirehoseClientBuilder.standard();

                if (!empty(awsRegion))
                    builder.withRegion(awsRegion);

                if (!empty(awsAccessKey) && !empty(awsSecretKey)) {
                    BasicAWSCredentials creds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
                    builder.withCredentials(new AWSStaticCredentialsProvider(creds));
                }

                firehoseClient = builder.build();
            }
        }

        return firehoseClient;
    }

    public FirehoseDb withAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    public FirehoseDb withAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public FirehoseDb withAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public FirehoseDb withIncludeStreams(String includeStreams) {
        this.includeStreams = includeStreams;
        return this;
    }

    public FirehoseDb withBatchMax(int batchMax) {
        this.batchMax = batchMax;
        return this;
    }

    public FirehoseDb withJsonSeparator(String jsonSeparator) {
        this.jsonSeparator = jsonSeparator;
        return this;
    }

    public FirehoseDb withJsonPrettyPrint(boolean jsonPrettyPrint) {
        this.jsonPrettyPrint = jsonPrettyPrint;
        return this;
    }

    public FirehoseDb withJsonLowercaseNames(boolean jsonLowercaseNames) {
        this.jsonLowercaseNames = jsonLowercaseNames;
        return this;
    }
}
