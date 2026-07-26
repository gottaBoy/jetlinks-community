package org.jetlinks.community.io.file.service;

import io.minio.*;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.community.io.file.FileInfo;
import org.jetlinks.community.io.file.FileManager;
import org.jetlinks.community.io.file.FileOption;
import org.jetlinks.community.io.file.info.ShardingUploadResult;
import org.jetlinks.community.io.file.info.UploadResponse;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * S3-compatible file service provider.
 * Stores files in MinIO / AWS S3 / TOS / OSS etc.
 *
 * <p>Activate by setting {@code file.manager.default-service=s3}.
 * Falls back to {@link LocalFileServiceProvider} if not configured.
 */
@Slf4j
public class S3FileServiceProvider implements FileServiceProvider {

    public static final String TYPE = "s3";

    private final S3FileProperties props;
    private final MinioClient client;

    public S3FileServiceProvider(S3FileProperties props) {
        this.props = props;
        var builder = MinioClient.builder()
            .endpoint(props.getEndpoint())
            .credentials(props.getAccessKey(), props.getSecretKey());
        if (props.getRegion() != null && !props.getRegion().isEmpty()) {
            builder.region(props.getRegion());
        }
        this.client = builder.build();
        // Ensure bucket exists
        try {
            boolean exists = client.bucketExists(BucketExistsArgs.builder().bucket(props.getBucket()).build());
            if (!exists) {
                client.makeBucket(MakeBucketArgs.builder().bucket(props.getBucket()).build());
                log.info("[S3] Created bucket: {}", props.getBucket());
            }
        } catch (Exception e) {
            log.warn("[S3] Failed to check/create bucket: {}", e.getMessage());
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public Mono<ShardingUploadResult> saveFile(String sessionId, String fileId, String storagePath,
                                                long length, long offset,
                                                Flux<DataBuffer> stream, FileOption... options) {
        // For simplicity, buffer the chunks and upload as a single object.
        // For production with large files, use S3 multipart upload.
        return DataBufferUtils.join(stream)
            .publishOn(Schedulers.boundedElastic())
            .map(buf -> {
                byte[] bytes = new byte[buf.readableByteCount()];
                buf.read(bytes);
                DataBufferUtils.release(buf);
                return bytes;
            })
            .flatMap(bytes -> Mono.fromCallable(() -> doUpload(storagePath, bytes, (int) length))
                .subscribeOn(Schedulers.boundedElastic()))
            .map(len -> {
                UploadResponse resp = new UploadResponse();
                resp.setPath(storagePath);
                resp.setLength(len);
                ShardingUploadResult result = new ShardingUploadResult();
                result.setComplete(true);
                result.setFileId(fileId);
                result.setResponse(resp);
                return result;
            });
    }

    @Override
    public Mono<UploadResponse> saveFile(String storagePath, Flux<DataBuffer> stream, FileOption... options) {
        return DataBufferUtils.join(stream)
            .publishOn(Schedulers.boundedElastic())
            .flatMap(buf -> {
                byte[] bytes = new byte[buf.readableByteCount()];
                buf.read(bytes);
                DataBufferUtils.release(buf);
                return Mono.fromCallable(() -> doUpload(storagePath, bytes, bytes.length))
                    .subscribeOn(Schedulers.boundedElastic());
            })
            .map(len -> {
                UploadResponse resp = new UploadResponse();
                resp.setPath(storagePath);
                resp.setLength(len);
                return resp;
            });
    }

    @Override
    public Flux<DataBuffer> read(FileInfo info, Function<FileManager.ReaderContext, Mono<Void>> callback) {
        String key = info.getPath();
        if (key == null || key.isEmpty()) {
            return Flux.error(new IllegalArgumentException("FileInfo.path is required for S3 read"));
        }
        return Mono.fromCallable(() -> doRead(key))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapMany(bytes -> {
                org.springframework.core.io.buffer.DataBufferFactory factory =
                    new org.springframework.core.io.buffer.DefaultDataBufferFactory();
                return Flux.just(factory.wrap(bytes));
            });
    }

    @Override
    public Mono<Void> delete(String storagePath) {
        return Mono.fromRunnable(() -> {
            try {
                client.removeObject(RemoveObjectArgs.builder()
                    .bucket(props.getBucket())
                    .object(storagePath)
                    .build());
            } catch (Exception e) {
                log.warn("[S3] Failed to delete {}: {}", storagePath, e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    // ── private helpers ──

    private long doUpload(String objectName, byte[] data, int length) throws Exception {
        client.putObject(PutObjectArgs.builder()
            .bucket(props.getBucket())
            .object(objectName)
            .stream(new ByteArrayInputStream(data), length, -1)
            .contentType("application/octet-stream")
            .build());
        log.debug("[S3] Uploaded: {} ({} bytes)", objectName, length);
        return length;
    }

    private byte[] doRead(String objectName) throws Exception {
        try (GetObjectResponse resp = client.getObject(GetObjectArgs.builder()
                .bucket(props.getBucket())
                .object(objectName)
                .build());
             ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = resp.read(buf)) != -1) {
                bos.write(buf, 0, n);
            }
            return bos.toByteArray();
        }
    }
}
