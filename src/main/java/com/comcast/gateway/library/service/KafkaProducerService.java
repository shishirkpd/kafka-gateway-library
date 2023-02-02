package com.comcast.gateway.library.service;

public interface KafkaProducerService {
    void sendMessage(String message);
}
