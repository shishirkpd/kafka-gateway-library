package com.comcast.gateway.library.config;

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
    @Value(value = "${kafka.bootstrapServers}")
    private String bootstrapAddress;
    @Value(value = "${kafka.schemaRegistryUrl}")
    private String schemaRegistryUrl;

    @Value(value = "${ssl.enable:false}")
    private boolean isSslEnable;

    @Value(value = "${compression.type:none}")
    private String compressionType;

    @Value(value = "${ssl.key.password:}")
    private String sslKeyPassword;

    @Value(value = "${ssl.keystore.key:}")
    private String sslkeystoreKey;

    @Value(value = "${ssl.keystore.location:}")
    private String sslkeystoreLocation;

    @Value(value = "${ssl.keystore.password:}")
    private String sslKeystorePassword;

    @Value(value = "${ssl.truststore.certificates:}")
    private String sslTruststoreCertificates;

    @Value(value = "${ssl.truststore.location:}")
    private String sslTruststoreLocation;

    @Value(value = "${ssl.truststore.password:}")
    private String sslTruststorePassword;

    @Value(value = "${batch.size:0}")
    private String batchSize;

    @Value(value = "${client.id:}")
    private String clientId;

    @Value(value = "${sasl.jaas.config:}")
    private String saslJaasConfig;

    @Value(value = "${sasl.mechanism:}")
    private String saslMechanism;

    @Value(value = "${security.protocol:}")
    private String securityProtocol;

    @Value(value = "${ssl.enabled.protocols:TLSv1.2}")
    private String sslEnabledProtocols;

    @Value(value = "${ssl.provider:}")
    private String sslProvider;

    @Value(value = "${acks:all}")
    private String acks;

    @Value(value = "${kafka.topic}")
    private String topic;


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
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.compressionType);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batchSize);
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientId);
        configProps.put(ProducerConfig.ACKS_CONFIG, this.acks);
        if(isSslEnable) {
            configProps.put("ssl.key.password", this.sslKeyPassword);
            configProps.put("ssl.keystore.key", this.sslkeystoreKey);
            configProps.put("ssl.keystore.location", this.sslkeystoreLocation);
            configProps.put("ssl.keystore.password", this.sslKeystorePassword);
            configProps.put("ssl.truststore.certificates", this.sslTruststoreCertificates);
            configProps.put("ssl.truststore.location", this.sslTruststoreLocation);
            configProps.put("ssl.truststore.password", this.sslTruststorePassword);
            configProps.put("sasl.jaas.config",  this.saslJaasConfig);
            configProps.put("sasl.mechanism",  this.saslMechanism);
            configProps.put("security.protocol",  this.securityProtocol);
            configProps.put("ssl.enabled.protocols",  this.sslEnabledProtocols);
            configProps.put("ssl.provider",  this.sslProvider);
        }
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    public KafkaProducerConfig() {
    }

    public String getTopic() {
        return this.topic;
    }
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplateAvro() {
        return new KafkaTemplate<>(producerFactoryForAvro());
    }
}