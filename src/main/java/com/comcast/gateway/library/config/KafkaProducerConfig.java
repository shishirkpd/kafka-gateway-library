package com.comcast.gateway.library.config;

import com.comcast.gateway.library.PayloadWithMetaInfo;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.comcast.gateway.library.config.Props.emptyProps;

@Configuration
public class KafkaProducerConfig {
    @Value(value = "${spring.kafka.bootstrapServers}")
    private String bootstrapAddress;
    @Value(value = "${spring.kafka.schemaRegistryUrl}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> producerFactoryForAvro() {
        Map<String, Object> configProps = emptyProps().toStringObjectMap();
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        configProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapAddress);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean KafkaTemplate<String, Object> kafkaTemplateAvro() {
        return new KafkaTemplate<>(producerFactoryForAvro());
    }
}