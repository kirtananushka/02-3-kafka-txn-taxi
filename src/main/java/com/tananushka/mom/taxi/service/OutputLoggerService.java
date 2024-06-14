package com.tananushka.mom.taxi.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;

@Service
@Slf4j
public class OutputLoggerService {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.###");

    @KafkaListener(topics = "output", containerFactory = "doubleKafkaListenerContainerFactory")
    public void logOutput(ConsumerRecord<String, Double> record) {
        String vehicleId = record.key();
        Double totalDistance = record.value();
        try {
            MDC.put("vehicleId", vehicleId);
            log.info("Vehicle ID: {}, Total Distance: {}", vehicleId, DECIMAL_FORMAT.format(totalDistance));
        } finally {
            MDC.remove("vehicleId");
        }
    }
}
