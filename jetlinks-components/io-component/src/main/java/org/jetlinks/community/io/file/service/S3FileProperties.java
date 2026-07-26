package org.jetlinks.community.io.file.service;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * S3-compatible object storage configuration.
 * Works with AWS S3, MinIO, Tencent COS, Alibaba OSS (S3-compatible mode), etc.
 */
@Getter
@Setter
@ConfigurationProperties("s3")
public class S3FileProperties {

    /** S3 endpoint URL, e.g. http://minio:9000 or https://s3.amazonaws.com */
    private String endpoint = "http://localhost:9000";

    /** Access key */
    private String accessKey = "minioadmin";

    /** Secret key */
    private String secretKey = "minioadmin";

    /** Bucket name */
    private String bucket = "ziot-files";

    /** Use path-style access (required for MinIO, optional for AWS) */
    private boolean pathStyleAccess = true;

    /** Region (optional, auto-detected for AWS) */
    private String region;
}
