package org.jetlinks.community.io.file.configuration;

import org.jetlinks.community.io.file.service.FileServiceProvider;
import org.jetlinks.community.io.file.service.S3FileProperties;
import org.jetlinks.community.io.file.service.S3FileServiceProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Registers S3FileServiceProvider when S3 object storage is enabled.
 * <p>
 * Activate with:
 * <pre>
 * s3.endpoint=http://minio:9000
 * s3.access-key=minioadmin
 * s3.secret-key=minioadmin
 * s3.bucket=ziot-files
 * file.manager.default-service=s3
 * </pre>
 */
@AutoConfiguration(after = DefaultFileManagerConfiguration.class)
@EnableConfigurationProperties(S3FileProperties.class)
@ConditionalOnProperty(prefix = "s3", name = "endpoint")
public class S3FileManagerConfiguration {

    @Bean
    public S3FileServiceProvider s3FileServiceProvider(S3FileProperties properties) {
        S3FileServiceProvider provider = new S3FileServiceProvider(properties);
        FileServiceProvider.providers.register(provider.getType(), provider);
        return provider;
    }
}
