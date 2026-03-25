package io.rcktapp.api.handler.firehose;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.Record;
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

import java.util.ArrayList;
import java.util.List;

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
 * the FirehoseDb.includeStreams property.
 *
 * @author wells
 */
public class FirehosePostHandler implements Handler
{
    protected int     batchMax     = 500;
    protected int     fieldCharMax = 10000;
    protected String  separator    = "\n";
    protected boolean prettyPrint  = false;

    @Override
    public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
    {
        if (!req.isMethod("PUT", "POST"))
            throw new ApiException(SC.SC_400_BAD_REQUEST, "The Firehose handler only supports PUT/POST operations...GET and DELETE don't make sense.");

        String collectionKey = req.getCollectionKey();
        Collection col = api.getCollection(collectionKey, FirehoseDb.class);
        Table table = col.getEntity().getTable();
        String streamName = table.getName();

        FirehoseClient firehose = ((FirehoseDb) table.getDb()).getFirehoseClient();

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

                batch.add(Record.builder().data(SdkBytes.fromByteArray(string.getBytes())).build());

                if ((i + 1) % batchMax == 0)
                {
                    firehose.putRecordBatch(PutRecordBatchRequest.builder().deliveryStreamName(streamName).records(batch).build());
                    batch = new ArrayList<>();
                }
            }

            if (!batch.isEmpty())
                firehose.putRecordBatch(PutRecordBatchRequest.builder().deliveryStreamName(streamName).records(batch).build());
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
