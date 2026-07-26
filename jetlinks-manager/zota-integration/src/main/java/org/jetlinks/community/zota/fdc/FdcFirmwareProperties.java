package org.jetlinks.community.zota.fdc;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * FDC firmware storage configuration.
 *
 * <pre>
 * fdc.firmware.storage-mode: local|s3
 * fdc.firmware.storage-path:  ./data/fdc-firmware         (local mode)
 * fdc.firmware.s3-endpoint:   http://minio:9000           (s3 mode)
 * fdc.firmware.s3-access-key: minioadmin
 * fdc.firmware.s3-secret-key: minioadmin
 * fdc.firmware.s3-bucket:     zota
 * </pre>
 */
@Getter
@Setter
@ConfigurationProperties("ziot.firmware")
public class FdcFirmwareProperties {

    /** Storage mode: "local" or "s3" */
    private String storageMode = "local";

    /** Local disk path (local mode only) */
    private String storagePath = "./data/fdc-firmware";

    /** Download base URL for constructing download links */
    private String downloadBaseUrl = "/api/firmware/download";

    /** S3 endpoint (s3 mode only) */
    private String s3Endpoint;

    /** S3 access key */
    private String s3AccessKey;

    /** S3 secret key */
    private String s3SecretKey;

    /** S3 bucket name */
    private String s3Bucket = "zota";

    /** S3 key prefix, placed after productId. Final key: {productId}/{s3Prefix}/{filename} */
    private String s3Prefix = "firmware";

    /** Routing key for OTA MQTT messages */
    private String routingKey = "zota.dmf.#";
}
