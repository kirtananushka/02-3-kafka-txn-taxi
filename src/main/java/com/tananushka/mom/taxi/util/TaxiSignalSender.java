package com.tananushka.mom.taxi.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tananushka.mom.taxi.dto.VehicleSignal;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaxiSignalSender {

    private static final String SIGNALS_URL = "http://localhost:8080/signals";
    private static final Random RANDOM = new Random();

    private static final List<String> TAXI_IDS = Arrays.asList(
            "taxi1", "taxi2", "taxi3", "taxi4"
    );

    private static final Map<String, VehicleSignal> taxiLocations = new ConcurrentHashMap<>();

    private static final double TAXI_1_SPEED = 0.0001; // Very slow (south to north)
    private static final double TAXI_2_SPEED = 0.0005; // Faster (north to south)
    private static final double TAXI_3_SPEED = 0.001; // Faster (west to east)
    private static final double TAXI_4_SPEED = 0.005; // Very fast (east to west)

    public static void main(String[] args) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(TAXI_IDS.size());

        taxiLocations.put("taxi1", new VehicleSignal("taxi1", -89.0, RANDOM.nextDouble() * 360 - 180));
        taxiLocations.put("taxi2", new VehicleSignal("taxi2", 89.0, RANDOM.nextDouble() * 360 - 180));
        taxiLocations.put("taxi3", new VehicleSignal("taxi3", RANDOM.nextDouble() * 180 - 90, -179.0));
        taxiLocations.put("taxi4", new VehicleSignal("taxi4", RANDOM.nextDouble() * 180 - 90, 179.0));

        for (String taxiId : TAXI_IDS) {
            executorService.scheduleAtFixedRate(() -> sendSignal(taxiId), 0, 5, TimeUnit.SECONDS);
        }
    }

    private static void sendSignal(String taxiId) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        VehicleSignal currentSignal = taxiLocations.get(taxiId);
        double newLatitude = currentSignal.getLatitude();
        double newLongitude = currentSignal.getLongitude();

        switch (taxiId) {
            case "taxi1":
                newLatitude += TAXI_1_SPEED;
                if (newLatitude > 90) newLatitude = -90;
                break;
            case "taxi2":
                newLatitude -= TAXI_2_SPEED;
                if (newLatitude < -90) newLatitude = 90;
                break;
            case "taxi3":
                newLongitude += TAXI_3_SPEED;
                if (newLongitude > 180) newLongitude = -180;
                break;
            case "taxi4":
                newLongitude -= TAXI_4_SPEED;
                if (newLongitude < -180) newLongitude = 180;
                break;
            default:
                // Random
                newLatitude = getRandomLatitude();
                newLongitude = getRandomLongitude();
                break;
        }

        VehicleSignal newSignal = new VehicleSignal(taxiId, newLatitude, newLongitude);
        taxiLocations.put(taxiId, newSignal);

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String signalJson = objectMapper.writeValueAsString(newSignal);

            HttpEntity<String> entity = new HttpEntity<>(signalJson, headers);
            restTemplate.exchange(SIGNALS_URL, HttpMethod.POST, entity, String.class);

            System.out.println("Sent signal: " + newSignal);
        } catch (Exception e) {
            // do nothing
        }
    }

    private static double getRandomLatitude() {
        return -90 + (90 - (-90)) * RANDOM.nextDouble();
    }

    private static double getRandomLongitude() {
        return -180 + (180 - (-180)) * RANDOM.nextDouble();
    }
}
