package org.jetlinks.community.firmware.configuration;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration
@ComponentScan(basePackages = "org.jetlinks.community.firmware")
@EnableScheduling
public class FirmwareAutoConfiguration {
}
