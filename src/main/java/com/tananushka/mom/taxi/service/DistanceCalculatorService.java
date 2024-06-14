package com.tananushka.mom.taxi.service;

import com.tananushka.mom.taxi.dto.VehicleSignal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class DistanceCalculatorService {
    private final Map<String, Double> distanceMap = new ConcurrentHashMap<>();
    private final Map<String, VehicleSignal> lastLocationMap = new ConcurrentHashMap<>();
    private final KafkaTemplate<String, Double> kafkaDoubleTemplate;

    @KafkaListener(topics = "input", containerFactory = "kafkaListenerContainerFactory", errorHandler = "kafkaListenerErrorHandler")
    public void processSignal(VehicleSignal signal) {
        VehicleSignal lastLocation = lastLocationMap.get(signal.getVehicleId());
        double distance = lastLocation != null ? calculateDistance(lastLocation, signal) : 0.0;
        distanceMap.compute(signal.getVehicleId(), (key, oldDistance) -> oldDistance == null ? distance : oldDistance + distance);
        lastLocationMap.put(signal.getVehicleId(), signal);
        kafkaDoubleTemplate.send("output", signal.getVehicleId(), distanceMap.get(signal.getVehicleId()));
    }

    private double calculateDistance(VehicleSignal lastSignal, VehicleSignal currentSignal) {
        double lat1 = Math.toRadians(lastSignal.getLatitude());
        double lon1 = Math.toRadians(lastSignal.getLongitude());
        double lat2 = Math.toRadians(currentSignal.getLatitude());
        double lon2 = Math.toRadians(currentSignal.getLongitude());
        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;
        double a = Math.pow(Math.sin(dLat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(dLon / 2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        final double R = 6371.0; // Radius of the Earth in kilometers
        return R * c; // Distance in kilometers
    }
}
