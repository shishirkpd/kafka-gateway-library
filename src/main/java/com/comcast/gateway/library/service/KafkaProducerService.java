package com.comcast.gateway.library.service;

import com.comcast.gateway.library.PayloadWithMetaInfo;

public interface KafkaProducerService {
    void sendMessage(String message);

    void sendMessage(PayloadWithMetaInfo message);
}
