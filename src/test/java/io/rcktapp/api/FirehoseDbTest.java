package io.rcktapp.api;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.rcktapp.api.handler.firehose.FirehoseDb;
import org.junit.jupiter.api.Test;




public class FirehoseDbTest {
    @Test
    void TestFirehoseDbBootstrapApi() {
        FirehoseDb underTest = new FirehoseDb();

        assertNotNull(underTest);
    }
}
