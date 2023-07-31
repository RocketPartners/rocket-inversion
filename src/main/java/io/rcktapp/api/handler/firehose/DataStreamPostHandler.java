package io.rcktapp.api.handler.firehose;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import io.rcktapp.api.Action;
import io.rcktapp.api.Api;
import io.rcktapp.api.Chain;
import io.rcktapp.api.Endpoint;
import io.rcktapp.api.Handler;
import io.rcktapp.api.Request;
import io.rcktapp.api.Response;
import io.rcktapp.api.service.Service;

public class DataStreamPostHandler implements Handler {
    @Override
    public void service(Service service, Api api, Endpoint endpoint, Action action, Chain chain, Request req, Response res) throws Exception
    {
//
//        String streamName = "stethoscopes";
//        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(Region.of(Regions.US_EAST_1.getName())));
//        validateStream(kinesisClient, streamName);
//        PutRecordRequest rootBuilder = PutRecordRequest.builder().partitionKey("partitionKey").streamName(streamName).build();
//        kinesisClient.putRecord(rootBuilder.toBuilder().data(SdkBytes.fromUtf8String(om.writeValueAsString(responseJson))).build());

//        new AsyncHandler<PutRecordRequest, PutRecordResponse>() {
//
//            @Override
//            public void onError(Exception e) {
//                log.error("Error sending data to Kinesis", e);
//            }
//
//            @Override
//            public void onSuccess(PutRecordRequest request, PutRecordResponse putRecordResult) {
//                log.info("Successfully sent data to Kinesis");
//            }
//        });

        AmazonKinesisClientBuilder.defaultClient();


    }
//    /**
//     * Checks if the stream exists and is active
//     *
//     * @param kinesisClient Amazon Kinesis client instance
//     * @param streamName Name of stream
//     */
//    private static void validateStream(KinesisAsyncClient kinesisClient, String streamName) {
//        try {
//            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder().streamName(streamName).build();
//            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
//            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
//                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
//                System.exit(1);
//            }
//        } catch (Exception e) {
//            System.err.println("Error found while describing the stream " + streamName);
//            System.err.println(e);
//            System.exit(1);
//        }
//    }
}
