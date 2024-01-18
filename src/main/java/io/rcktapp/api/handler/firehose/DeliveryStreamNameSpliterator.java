package io.rcktapp.api.handler.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;

import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DeliveryStreamNameSpliterator implements Spliterator<String> {
    private final AmazonKinesisFirehose firehoseClient;
    private String last = null;

    private DeliveryStreamNameSpliterator(AmazonKinesisFirehose firehoseClient) {
        this.firehoseClient = firehoseClient;
    }

    static Stream<String> stream(AmazonKinesisFirehose firehoseClient) {
        return StreamSupport.stream(new DeliveryStreamNameSpliterator(firehoseClient), false);
    }


    @Override
    public boolean tryAdvance(Consumer<? super String> action) {
        List<String> page = firehoseClient.listDeliveryStreams(getRequest(last)).getDeliveryStreamNames();
        last = page.stream().reduce((first, second) -> second).orElse(null);
        page.forEach(action);
        return !page.isEmpty();
    }

    ListDeliveryStreamsRequest getRequest(String lastEntry) {
        return new ListDeliveryStreamsRequest()
                .withDeliveryStreamType("DirectPut")
                .withExclusiveStartDeliveryStreamName(lastEntry);
    }

    @Override
    public Spliterator<String> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    @Override
    public int characteristics() {
        return 0;
    }
}
