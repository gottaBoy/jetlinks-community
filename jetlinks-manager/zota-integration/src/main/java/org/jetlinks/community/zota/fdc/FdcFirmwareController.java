package org.jetlinks.community.zota.fdc;

import io.minio.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

/**
 * FDC firmware management REST API.
 * <p>
 * Supports two storage modes:
 * <ul>
 *   <li><b>local</b> (default): local disk at {@code fdc.firmware.storage-path}</li>
 *   <li><b>s3</b>: S3-compatible object storage (TOS/MinIO/AWS) at {@code firmware/{productId}/{filename}}</li>
 * </ul>
 *
 * <pre>
 * Endpoints:
 *   POST   /api/firmware/upload     — upload firmware
 *   GET    /api/firmware/list       — list firmware
 *   GET    /api/firmware/download/{productId}/{filename} — download firmware
 *   POST   /api/firmware/ota/upgrade — retired; use the generic firmware task API
 * </pre>
 */
@Slf4j
@RestController
@ConditionalOnProperty(prefix = "zota", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequestMapping("/api/firmware")
public class FdcFirmwareController {

    private final FdcFirmwareProperties props;
    private final MinioClient s3Client;
    private final boolean s3Mode;

    public FdcFirmwareController(FdcFirmwareProperties props) {
        this.props = props;
        this.s3Mode = "s3".equalsIgnoreCase(props.getStorageMode());
        if (s3Mode) {
            this.s3Client = MinioClient.builder()
                .endpoint(props.getS3Endpoint())
                .credentials(props.getS3AccessKey(), props.getS3SecretKey())
                .build();
            log.info("[FDC] Firmware storage: S3 mode, bucket={}, prefix={}",
                props.getS3Bucket(), props.getS3Prefix());
        } else {
            this.s3Client = null;
            log.info("[FDC] Firmware storage: local mode, path={}", props.getStoragePath());
        }
    }

    // ── 固件上传 ─────────────────────────────────────────────

    @PostMapping("/upload")
    public Mono<Map<String, Object>> uploadFirmware(
            @RequestParam("file") MultipartFile file,
            @RequestParam("version") String version,
            @RequestParam(value = "productId", required = false, defaultValue = "default") String productId,
            @RequestParam(value = "description", required = false, defaultValue = "") String description) {

        return Mono.fromCallable(() -> {
            String filename = "fdc_" + version + "_" + System.currentTimeMillis() + ".bin";
            byte[] data = file.getBytes();
            long fileSize = data.length;
            String sha256 = computeSha256(data);

            if (s3Mode) {
                String key = productId + "/" + props.getS3Prefix() + "/" + filename;
                s3Client.putObject(PutObjectArgs.builder()
                    .bucket(props.getS3Bucket())
                    .object(key)
                    .stream(new java.io.ByteArrayInputStream(data), fileSize, -1)
                    .contentType("application/octet-stream")
                    .build());
                log.info("[FDC] Firmware uploaded to S3: key={}, size={}", key, fileSize);
            } else {
                Path dir = Paths.get(props.getStoragePath(), productId);
                Files.createDirectories(dir);
                Path filePath = dir.resolve(filename);
                file.transferTo(filePath.toFile());
                log.info("[FDC] Firmware uploaded to local: path={}, size={}", filePath, fileSize);
            }

            String fileUrl = props.getDownloadBaseUrl() + "/" + productId + "/" + filename;

            return Map.<String, Object>of(
                "filename", filename,
                "version", version,
                "fileSize", fileSize,
                "sha256", sha256,
                "fileUrl", fileUrl,
                "productId", productId,
                "description", description,
                "status", "ok"
            );
        });
    }

    // ── 固件列表 ──────────────────────────────────────────────

    @GetMapping("/list")
    public Mono<List<Map<String, Object>>> listFirmware(
            @RequestParam(value = "productId", required = false) String productId) {

        return Mono.fromCallable(() -> {
            if (s3Mode) {
                return listFromS3(productId);
            } else {
                return listFromLocal(productId);
            }
        });
    }

    private List<Map<String, Object>> listFromLocal(String productId) {
        File dir = new File(props.getStoragePath());
        if (!dir.exists()) return Collections.emptyList();

        List<Map<String, Object>> result = new ArrayList<>();
        File[] productDirs = productId != null && !productId.isEmpty()
            ? new File[]{new File(dir, productId)}
            : dir.listFiles(File::isDirectory);
        if (productDirs == null) return result;

        for (File pd : productDirs) {
            if (!pd.isDirectory()) continue;
            String pid = pd.getName();
            File[] files = pd.listFiles((d, name) -> name.startsWith("fdc_") && name.endsWith(".bin"));
            if (files == null) continue;
            for (File f : files) {
                result.add(Map.<String, Object>of(
                    "filename", f.getName(),
                    "version", extractVersion(f.getName()),
                    "fileSize", f.length(),
                    "fileUrl", props.getDownloadBaseUrl() + "/" + pid + "/" + f.getName(),
                    "productId", pid,
                    "lastModified", f.lastModified()
                ));
            }
        }
        return result;
    }

    private List<Map<String, Object>> listFromS3(String productId) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            String prefix = "";
            if (productId != null && !productId.isEmpty()) {
                prefix = productId + "/" + props.getS3Prefix() + "/";
            }
            var objects = s3Client.listObjects(ListObjectsArgs.builder()
                .bucket(props.getS3Bucket())
                .prefix(prefix)
                .recursive(true)
                .build());
            for (var item : objects) {
                var obj = item.get();
                String name = obj.objectName().substring(obj.objectName().lastIndexOf('/') + 1);
                String pid = extractProductId(obj.objectName());
                result.add(Map.<String, Object>of(
                    "filename", name,
                    "version", extractVersion(name),
                    "fileSize", obj.size(),
                    "fileUrl", props.getDownloadBaseUrl() + "/" + pid + "/" + name,
                    "productId", pid,
                    "lastModified", obj.lastModified().toEpochSecond() * 1000
                ));
            }
        } catch (Exception e) {
            log.error("[FDC] Failed to list S3 firmware", e);
        }
        return result;
    }

    // ── 固件下载 ─────────────────────────────────────────────

    @GetMapping("/download/{productId}/{filename}")
    public ResponseEntity<Resource> downloadFirmware(
            @PathVariable String productId,
            @PathVariable String filename) {

        if (s3Mode) {
            return downloadFromS3(productId, filename);
        } else {
            return downloadFromLocal(productId, filename);
        }
    }

    private ResponseEntity<Resource> downloadFromLocal(String productId, String filename) {
        Path filePath = Paths.get(props.getStoragePath(), productId, filename).normalize();
        Resource resource = new FileSystemResource(filePath);
        if (!resource.exists()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .body(resource);
    }

    private ResponseEntity<Resource> downloadFromS3(String productId, String filename) {
        try {
            String key = productId + "/" + props.getS3Prefix() + "/" + filename;
            GetObjectResponse resp = s3Client.getObject(GetObjectArgs.builder()
                .bucket(props.getS3Bucket())
                .object(key)
                .build());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = resp.read(buf)) != -1) bos.write(buf, 0, n);
            resp.close();
            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .body(new ByteArrayResource(bos.toByteArray()));
        } catch (Exception e) {
            log.error("[FDC] Failed to download from S3: {}/{}", productId, filename, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // ── Retired OTA endpoint ─────────────────────────────────

    @PostMapping("/ota/upgrade")
    public ResponseEntity<Map<String, Object>> rejectLegacyUpgrade() {
        return ResponseEntity
            .status(410)
            .body(Map.of(
                "status", "gone",
                "code", "LEGACY_OTA_ENDPOINT_RETIRED",
                "message", "Use the generic firmware upgrade task API",
                "migrateTo", "/firmware/upgrade/task"
            ));
    }

    // ── 工具方法 ──────────────────────────────────────────────

    private String computeSha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            log.error("SHA256 computation failed", e);
            return "unknown";
        }
    }

    private String extractVersion(String filename) {
        String[] parts = filename.replace("fdc_", "").split("_");
        return parts.length > 1 ? String.join("_",
            java.util.Arrays.copyOf(parts, parts.length - 1)) : "unknown";
    }

    private String extractProductId(String s3Key) {
        // {productId}/firmware/{filename} → productId
        String[] parts = s3Key.split("/");
        return parts.length >= 2 ? parts[0] : "default";
    }
}
