package io.rcktapp.api.handler.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
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

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Posts records to a mapped AWS Kinesis Firehose stream.
 * <p>
 * When you PUT/POST a:
 * <ul>
 * <li>a JSON object - it is submitted as a single record
 * <li>a JSON array - each element in the array is submitted as a single record.
 * </ul>
 * <p>
 * Unless <code>prettyPrint</code> is set to <code>true</code> all JSON
 * records are stringified without return characters.
 * <p>
 * All records are always submitted in batches of up to <code>batchMax</code>.
 * You can submit more than <code>batchMax</code> to the handler and it will try to
 * send as many batches as required.
 * <p>
 * If <code>separator</code> is not null (it is '\n' by default) and the
 * stringified record does not end in <code>separator</code>,
 * <code>separator</code> will be appended to the record.
 * <p>
 * The underlying Firehose stream is mapped to the collection name through
 * the FireshoseDb.includeStreams property.
 *
 * @author wells
 */
public class FirehosePostHandler implements Handler
{
    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public static final String lower = upper.toLowerCase();
    public static final String digits = "0123456789";
    public static final char[] symbols = (upper + lower + digits).toCharArray();
    static SecureRandom random = new SecureRandom();
    protected int     batchMax     = 500;
    protected int     fieldCharMax = 10000;
    protected String  separator    = "\n";
    protected boolean prettyPrint  = false;

    public static String randomString(int size)
    {

        char[] buf = new char[size];
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println(randomString(100));

        AmazonKinesisFirehose firehose = AmazonKinesisFirehoseClientBuilder.defaultClient();

        ListDeliveryStreamsRequest listDeliveryStreamsRequest = new ListDeliveryStreamsRequest();
        ListDeliveryStreamsResult listDeliveryStreamsResult = firehose.listDeliveryStreams(listDeliveryStreamsRequest);
        List<String> deliveryStreamNames = listDeliveryStreamsResult.getDeliveryStreamNames();
        while (listDeliveryStreamsResult.isHasMoreDeliveryStreams())
        {
            if (deliveryStreamNames.size() > 0)
            {
                listDeliveryStreamsRequest.setExclusiveStartDeliveryStreamName(deliveryStreamNames.get(deliveryStreamNames.size() - 1));
            }
            listDeliveryStreamsResult = firehose.listDeliveryStreams(listDeliveryStreamsRequest);
            deliveryStreamNames.addAll(listDeliveryStreamsResult.getDeliveryStreamNames());
        }

        System.out.println(deliveryStreamNames);

        for (int i = 0; i < 2000; i++)
        {
            List<Record> records = new ArrayList();
            for (int j = 0; j < 500; j++)
            {
                JSObject json = new JSObject("tenantCode", "us", "yearid", 2019, "monthid", 201901, "dayid", 20190110, "locationCode", "asdfasdf", "playerCode", "us-12345667-1", "adId", (i * j), "somecrapproperrty", randomString(500));
                records.add(new Record().withData(ByteBuffer.wrap((json.toString(false).trim() + "\n").getBytes())));
            }

            PutRecordBatchRequest put = new PutRecordBatchRequest();
            put.setDeliveryStreamName("liftck-player9-impression");
            put.setRecords(records);
            firehose.putRecordBatch(put);

            System.out.println(i);
        }

    }

    @Override
    public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
    {
        if (!req.isMethod("PUT", "POST"))
            throw new ApiException(SC.SC_400_BAD_REQUEST, "The Firehose handler only supports PUT/POST operations...GET and DELETE don't make sense.");

        String collectionKey = req.getCollectionKey();
        Collection col = api.getCollection(collectionKey, FirehoseDb.class);
        Table table = col.getEntity().getTable();
        String streamName = table.getName();

        AmazonKinesisFirehoseAsync firehose = ((FirehoseDb) table.getDb()).getFirehoseClient();

        JSObject body = req.getJson();

        if (body == null)
            throw new ApiException(SC.SC_400_BAD_REQUEST, "Attempting to post an empty body to a Firehose stream");

        if (!(body instanceof JSArray))
            body = new JSArray(body);

        JSArray array = (JSArray) body;

        List<Record> batch = new ArrayList();

        try
        {
            for (int i = 0; i < array.length(); i++)
            {
                Object data = array.get(i);

                if (data == null)
                    continue;

                String string;
                if (data instanceof JSObject)
                {
                    JSObject obj = (JSObject) data;
                    for (JSObject.Property property : obj.getProperties())
                    {
                        //firehose has a max field length.. limit here
                        if (property.getValue() instanceof String && ((String) property.getValue()).length() > fieldCharMax)
                        {
                            property.setValue(((String) property.getValue()).substring(0, fieldCharMax));
                        }
                    }
                    string = this.convertJsonFieldNamesToLowercase(obj).toString(prettyPrint);
                }
                else
                {
                    string = data.toString();
                }

                if (separator != null && !string.endsWith(separator))
                    string += separator;

                batch.add(new Record().withData(ByteBuffer.wrap(string.getBytes())));

                if ((i + 1) % batchMax == 0)
                {
                    firehose.putRecordBatchAsync(new PutRecordBatchRequest().withDeliveryStreamName(streamName).withRecords(batch));
                    batch = new ArrayList<>();
                }
            }

            if (!batch.isEmpty())
                firehose.putRecordBatchAsync(new PutRecordBatchRequest().withDeliveryStreamName(streamName).withRecords(batch));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new ApiException(SC.SC_500_INTERNAL_SERVER_ERROR, "Error putting records to firehose stream '" + streamName + "' - " + e.getMessage());
        }

        res.setStatus(SC.SC_201_CREATED);
    }

    // TODO Remove this with upgrade to Snooze 4. Method provided by Tim Collins as a temporary workaround. - Lukas Bradley 6 June 2019
    private JSObject convertJsonFieldNamesToLowercase(JSObject json)
    {
        if (json instanceof JSArray)
        {
            for (Object o : ((JSArray) json).getObjects())
            {
                if (o instanceof JSObject)
                {
                    convertJsonFieldNamesToLowercase((JSObject) o);
                }
            }
        }
        else
        {
            Object obj = null;
            for (String key : json.keySet())
            {
                obj = json.get(key);
                if (obj instanceof JSObject)
                {
                    obj = convertJsonFieldNamesToLowercase((JSObject) obj);
                }

                json.remove(key);
                json.put(key.toLowerCase(), obj);
            }
        }

        return json;
    }

}
