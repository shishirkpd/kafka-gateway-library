package com.comcast.gateway.library.service.impl;

import com.comcast.gateway.library.KafkaProducerFactory;
import com.comcast.gateway.library.PayloadWithMetaInfo;
import com.comcast.gateway.library.config.KafkaProducerConfig;
import com.comcast.gateway.library.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaProducerServiceImpl implements KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final KafkaTemplate<String, Object> avroKafkaTemplate;


    private final KafkaProducerConfig kafkaProducerConfig;

    public KafkaProducerServiceImpl(KafkaTemplate<String, String> kafkaTemplate,
                                    KafkaTemplate<String, Object> avroKafkaTemplate,
                                    KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public void sendMessage(String message) {
        kafkaTemplate.send(kafkaProducerConfig.getTopic(), message);
    }

    @Override
    public void sendMessage(PayloadWithMetaInfo message) {
        logger.info("sending message: {}", message.toString());
        try {
            avroKafkaTemplate.send(kafkaProducerConfig.getTopic(), message.getLastUpdateEventId(), message);
        }catch (Exception ex) {
            logger.error("Failed to send message: {} to topic: {} ", message, kafkaProducerConfig.getTopic());
        }
    }
}
