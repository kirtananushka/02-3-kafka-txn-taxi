package com.tananushka.mom.taxi.controller;

import com.tananushka.mom.taxi.dto.VehicleSignal;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class SignalController {
    private final KafkaTemplate<String, VehicleSignal> kafkaVehicleSignalTemplate;
    private final KafkaTransactionManager<String, VehicleSignal> kafkaTransactionManager;

    @PostMapping("/signals")
    public ResponseEntity<Void> receiveSignal(@Valid @RequestBody VehicleSignal signal) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(kafkaTransactionManager);
        transactionTemplate.execute(status -> {
            kafkaVehicleSignalTemplate.executeInTransaction(kt -> {
                kt.send("input", signal.getVehicleId(), signal);
                return null;
            });
            return null;
        });
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
