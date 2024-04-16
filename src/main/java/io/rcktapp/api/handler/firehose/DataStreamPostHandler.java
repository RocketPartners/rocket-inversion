package io.rcktapp.api.handler.firehose;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.StreamDescription;
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
import io.rcktapp.api.Response;
import io.rcktapp.api.SC;
import io.rcktapp.api.Table;
import io.rcktapp.api.service.Service;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import static org.apache.commons.codec.binary.Base64.decodeBase64;

/**
 * Basically the firehose class - only generalized for data streams.
 *
 * The format you POST or PUT to APIURI/datastream/STREAMNAME is:
 * [
 *     { "partitionKey": "distinct data for grouping delivery", "blob": "base64 encoded string" },
 *     { "partitionKey": "distinct data for grouping delivery", "blob": "base64 encoded string" },
 *     ...
 * ]
 *
 * Streamname has to be a valid stream name when the inversion started.
 */
@Slf4j
public class DataStreamPostHandler implements Handler {
    protected int batchMax = 500;
    protected String separator = "\n";

    @Override
    public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception {
        if (!req.isMethod("PUT", "POST"))
            throw new ApiException(SC.SC_400_BAD_REQUEST, "The Firehose handler only supports PUT/POST operations...GET and DELETE don't make sense.");

        String collectionKey = req.getCollectionKey();
        Collection col = api.getCollection(collectionKey, DataStreamDb.class);
        Table table = col.getEntity().getTable();
        String streamName = table.getName();

        AmazonKinesisAsync datastream = ((DataStreamDb) table.getDb()).getClient();

        validateStream(datastream, streamName);

        JSObject body = req.getJson();

        if (body == null)
            throw new ApiException(SC.SC_400_BAD_REQUEST, "Attempting to post an empty body to a Firehose stream");

        if (!(body instanceof JSArray))
            body = new JSArray(body);

        JSArray array = (JSArray) body;

        LinkedList<PutRecordsRequestEntry> batch = new LinkedList<>();

        try {
            for (int i = 0; i < array.length(); i++) {
                JSObject data = (JSObject) array.get(i);

                if (data == null)
                    continue;

                String blob = data.getString("base64");
                String partitionKey = data.getString("partitionkey");

                batch.add(new PutRecordsRequestEntry()
                        .withData(ByteBuffer.wrap(decodeBase64(blob)))
                        .withPartitionKey(partitionKey));

                if ((i + 1) % batchMax == 0) {
                    datastream.putRecordsAsync(new PutRecordsRequest().withStreamName(streamName).withRecords(batch));
                    batch.clear();
                }
            }

            if (!batch.isEmpty())
                datastream.putRecordsAsync(new PutRecordsRequest().withStreamName(streamName).withRecords(batch));
        } catch (Exception e) {
            e.printStackTrace();
            throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Error putting records to data stream '" + streamName + "' - " + e.getMessage());
        }

        res.setStatus(SC.SC_201_CREATED);
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private static void validateStream(AmazonKinesisAsync kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest =  new DescribeStreamRequest().withStreamName(streamName);
            StreamDescription describeStreamDescription = kinesisClient.describeStream(describeStreamRequest).getStreamDescription();
            if(!describeStreamDescription.getStreamStatus().equals("ACTIVE")) {
                log.error("Stream {} is not active. Please wait a few moments and try again.",  streamName );
            }
        } catch (Exception e) {
            log.error("Error found while describing the stream " + streamName, e);
        }
    }
}
