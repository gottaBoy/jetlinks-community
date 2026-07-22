package org.jetlinks.community.parallel.driving.configuration;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@ComponentScan(basePackages = "org.jetlinks.community.parallel.driving")
@EnableConfigurationProperties(ParallelDrivingVehicleToCockpitProperties.class)
@EnableScheduling
public class ParallelDrivingConfiguration {

}

