package org.jetlinks.community.zota.fdc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

/**
 * FDC 固件管理 REST API。
 * 
 * 端点:
 *   POST   /api/fdc/firmware/upload     — 上传固件
 *   GET    /api/fdc/firmware/list       — 固件列表
 *   GET    /api/fdc/firmware/{id}       — 固件详情
 *   DELETE /api/fdc/firmware/{id}       — 删除固件
 *   GET    /api/fdc/firmware/download/{filename} — 固件下载（供 FDC 设备 HTTP 下载）
 *   POST   /api/fdc/ota/upgrade         — 创建升级任务
 *   GET    /api/fdc/ota/status/{taskId} — 查询升级任务状态
 */
@Slf4j
@RestController
@RequestMapping("/api/fdc")
@RequiredArgsConstructor
public class FdcFirmwareController {

    private final FdcOtaService otaService;
    private final String storagePath = "./data/fdc-firmware";

    // ── 固件上传 ─────────────────────────────────────────────

    @PostMapping("/firmware/upload")
    public Mono<Map<String, Object>> uploadFirmware(
            @RequestParam("file") MultipartFile file,
            @RequestParam("version") String version,
            @RequestParam(value = "description", required = false, defaultValue = "") String description) {

        return Mono.fromCallable(() -> {
            // 确保存储目录存在
            Path dir = Paths.get(storagePath);
            Files.createDirectories(dir);

            // 保存文件
            String filename = "fdc_" + version + "_" + System.currentTimeMillis() + ".bin";
            Path filePath = dir.resolve(filename);
            file.transferTo(filePath.toFile());

            // 计算 SHA256
            String sha256 = computeSha256(filePath);

            long fileSize = file.getSize();

            log.info("FDC firmware uploaded: version={}, filename={}, size={}, sha256={}",
                version, filename, fileSize, sha256);

            return Map.<String, Object>of(
                "filename", filename,
                "version", version,
                "fileSize", fileSize,
                "sha256", sha256,
                "fileUrl", "/api/fdc/firmware/download/" + filename,
                "description", description,
                "status", "ok"
            );
        });
    }

    // ── 固件列表（简化版，从文件系统读取） ──────────────────────

    @GetMapping("/firmware/list")
    public Mono<List<Map<String, Object>>> listFirmware() {
        return Mono.fromCallable(() -> {
            File dir = new File(storagePath);
            if (!dir.exists()) return Collections.emptyList();

            File[] files = dir.listFiles((d, name) -> name.startsWith("fdc_") && name.endsWith(".bin"));
            if (files == null) return Collections.emptyList();

            List<Map<String, Object>> result = new ArrayList<>();
            for (File f : files) {
                String name = f.getName();
                String version = extractVersion(name);
                result.add(Map.<String, Object>of(
                    "filename", name,
                    "version", version,
                    "fileSize", f.length(),
                    "fileUrl", "/api/fdc/firmware/download/" + name,
                    "lastModified", f.lastModified()
                ));
            }
            return result;
        });
    }

    // ── 固件下载（供 FDC 设备 HTTP 下载） ─────────────────────

    @GetMapping("/firmware/download/{filename}")
    public ResponseEntity<Resource> downloadFirmware(@PathVariable String filename) {
        Path filePath = Paths.get(storagePath).resolve(filename).normalize();
        Resource resource = new FileSystemResource(filePath);

        if (!resource.exists()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
            .body(resource);
    }

    // ── OTA 升级 ─────────────────────────────────────────────

    @PostMapping("/ota/upgrade")
    public Mono<Map<String, Object>> startUpgrade(@RequestBody Map<String, Object> request) {
        @SuppressWarnings("unchecked")
        List<String> deviceIds = (List<String>) request.getOrDefault("deviceIds", Collections.emptyList());
        String version = (String) request.getOrDefault("version", "");
        String fileUrl = (String) request.getOrDefault("fileUrl", "");
        String sha256 = (String) request.getOrDefault("sha256", "");
        Long fileSize = request.containsKey("fileSize") ? 
            ((Number) request.get("fileSize")).longValue() : 0L;

        if (deviceIds.isEmpty()) {
            return Mono.just(Map.of("status", "error", "message", "deviceIds is required"));
        }

        FdcFirmware fw = new FdcFirmware();
        fw.setVersion(version);
        fw.setFileUrl(fileUrl);
        fw.setFileSize(fileSize);
        fw.setSha256(sha256);

        return otaService.dispatchBatch(deviceIds, fw);
    }

    // ── 工具方法 ──────────────────────────────────────────────

    private String computeSha256(Path filePath) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(Files.readAllBytes(filePath));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            log.error("SHA256 computation failed", e);
            return "unknown";
        }
    }

    private String extractVersion(String filename) {
        // fdc_1.3.0_1753094400000.bin → 1.3.0
        String[] parts = filename.replace("fdc_", "").split("_");
        return parts.length > 1 ? String.join("_", 
            java.util.Arrays.copyOf(parts, parts.length - 1)) : "unknown";
    }
}
