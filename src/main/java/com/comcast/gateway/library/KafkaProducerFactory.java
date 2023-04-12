package com.comcast.gateway.library;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class KafkaProducerFactory<K, V> {
    private final KafkaProducer<K, V> producer;
    private final String topic;

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerFactory.class);

    public static <K,V> Builder<K, V> newBuilder() {
        return new Builder();
    }

    public static <V> Builder<String, V> newStringKeyBuilder() {
        return KafkaProducerFactory.<String, V>newBuilder().withKeySerializer(new StringSerializer());
    }

    public static <K> Builder<K, String> newStringValueBuilder() {
        return KafkaProducerFactory.<K, String>newBuilder().withValueSerializer(new StringSerializer());
    }

    public static Builder<String, String> newStringBuilder() {
        return KafkaProducerFactory.<String>newStringKeyBuilder().withValueSerializer(new StringSerializer());
    }

    public KafkaProducerFactory(Map<String, Object> props, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
        this.topic = topic;
    }


    public CompletableFuture<RecordMetadata> publish(K k, V v) {
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(topic, k, v), (m, e) -> {
            logger.info("Sending data with key: {} and value: {}", k, v);
            if(e != null) {
                f.completeExceptionally(e);
            } else  {
                f.complete(m);
            }
        });
        return f;
    }

    public static class Builder<K, V> {
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;

        private Map<String, Object> props;
        private String topic;

        public KafkaProducerFactory<K, V> build() {
            return new KafkaProducerFactory<>(props, topic, keySerializer, valueSerializer);
        }

        public Builder<K, V> withKeySerializer(Serializer<K> serializer) {
            this.keySerializer = serializer;
            return this;
        }

        public Builder<K, V> withValueSerializer(Serializer<V> serializer) {
            this.valueSerializer = serializer;
            return this;
        }

        @SafeVarargs
        public final Builder<K, V> withProperties(Map<String, Object>... props) {
            this.props = Arrays.stream(props).reduce(new HashMap<>(), (m1, m2) -> {
                m1.putAll(m2);
                return m1;
            });
            return this;
        }

        public Builder<K, V> withTopic(String topic) {
            this.topic = topic;
            return this;
        }
    }
}
