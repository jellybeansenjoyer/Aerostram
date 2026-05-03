package com.aerostream;

import com.aerostream.simulation.SimulatorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SimulatorProperties.class)
public class AerostreamProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AerostreamProducerApplication.class, args);
    }
}
