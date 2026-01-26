package org.jetlinks.community.parallel.driving.configuration;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * 并行驾驶管理配置类
 *
 * @author JetLinks
 */
@AutoConfiguration
@ComponentScan(basePackages = "org.jetlinks.community.parallel.driving")
public class ParallelDrivingConfiguration {

    // RelatedEntity 的 ReactiveRepository 会通过主应用的 @EnableEasyormRepository 自动创建
    // 无需手动创建 Bean
}

