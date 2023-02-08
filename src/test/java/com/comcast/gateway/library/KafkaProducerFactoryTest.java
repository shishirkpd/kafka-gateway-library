package com.comcast.gateway.library;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.comcast.gateway.library.tool.KafkaTestUtil.embeddedKafkaWithTopic;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaProducerFactoryTest {

    public static final String TOPIC = "topic";
    public static final String K = "key";
    public static final String V = "value";

    private static final int PARTITIONS = 1;


    @Test
    public void testIt() {

        CountDownLatch partitionAssignmentLatch = new CountDownLatch(PARTITIONS);
        CountDownLatch messageLatch = new CountDownLatch(1);

        embeddedKafkaWithTopic(TOPIC)
                .withPartitions(PARTITIONS)
                .withGroupId("grpId")
                .runTest(config -> {
                    try {
                        // partitionAssignmentLatch.await();
                        KafkaProducerFactory<String, String> sink = KafkaProducerFactory.newStringBuilder()
                                .withProperties(config)
                                .withTopic(TOPIC)
                                .build();

                        RecordMetadata m = sink.publish(K, V).get(3000, TimeUnit.MICROSECONDS);
                        assertNotNull(m);
                        messageLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }
                    return null;
                });
    }

}