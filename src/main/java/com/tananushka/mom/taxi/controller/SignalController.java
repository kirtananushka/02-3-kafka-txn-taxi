package com.tananushka.mom.taxi.controller;

import com.tananushka.mom.taxi.dto.VehicleSignal;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class SignalController {
    private final KafkaTemplate<String, VehicleSignal> kafkaVehicleSignalTemplate;

    @PostMapping("/signals")
    public String receiveSignal(@Valid @RequestBody VehicleSignal signal) {
        kafkaVehicleSignalTemplate.send("input", signal.getVehicleId(), signal);
        return "Signal received";
    }
}
