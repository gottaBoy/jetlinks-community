package org.jetlinks.community.io.file.service;

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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.function.Function;

/**
 * S3-compatible file service provider using AWS S3 SDK.
 * Compatible with TOS / MinIO / AWS S3 / OSS etc.
 */
@Slf4j
public class S3FileServiceProvider implements FileServiceProvider {

    public static final String TYPE = "s3";

    private final S3FileProperties props;
    private final S3Client client;

    public S3FileServiceProvider(S3FileProperties props) {
        this.props = props;

        var creds = AwsBasicCredentials.create(props.getAccessKey(), props.getSecretKey());
        this.client = S3Client.builder()
            .endpointOverride(URI.create(props.getEndpoint()))
            .region(Region.of(props.getRegion() != null ? props.getRegion() : "cn-shanghai"))
            .credentialsProvider(StaticCredentialsProvider.create(creds))
            .build();

        // Ensure bucket exists
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(props.getBucket()).build());
        } catch (NoSuchBucketException e) {
            client.createBucket(CreateBucketRequest.builder().bucket(props.getBucket()).build());
            log.info("[S3] Created bucket: {}", props.getBucket());
        } catch (Exception e) {
            log.warn("[S3] Failed to check bucket: {}", e.getMessage());
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
        return DataBufferUtils.join(stream)
            .publishOn(Schedulers.boundedElastic())
            .map(buf -> {
                byte[] bytes = new byte[buf.readableByteCount()];
                buf.read(bytes);
                DataBufferUtils.release(buf);
                return bytes;
            })
            .flatMap(bytes -> Mono.fromCallable(() -> doUpload(storagePath, bytes))
                .subscribeOn(Schedulers.boundedElastic())
                .map(len -> {
                    UploadResponse resp = new UploadResponse();
                    resp.setPath(storagePath);
                    resp.setLength(len);
                    resp.setMd5(md5(bytes));
                    resp.setSha256(sha256(bytes));
                    ShardingUploadResult result = new ShardingUploadResult();
                    result.setComplete(true);
                    result.setFileId(fileId);
                    result.setResponse(resp);
                    return result;
                }));
    }

    @Override
    public Mono<UploadResponse> saveFile(String storagePath, Flux<DataBuffer> stream, FileOption... options) {
        return DataBufferUtils.join(stream)
            .publishOn(Schedulers.boundedElastic())
            .flatMap(buf -> {
                byte[] bytes = new byte[buf.readableByteCount()];
                buf.read(bytes);
                DataBufferUtils.release(buf);
                return Mono.fromCallable(() -> doUpload(storagePath, bytes))
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(len -> {
                        UploadResponse resp = new UploadResponse();
                        resp.setPath(storagePath);
                        resp.setLength(len);
                        resp.setMd5(md5(bytes));
                        resp.setSha256(sha256(bytes));
                        return resp;
                    });
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
                var factory = new org.springframework.core.io.buffer.DefaultDataBufferFactory();
                return Flux.just(factory.wrap(bytes));
            });
    }

    @Override
    public Mono<Void> delete(String storagePath) {
        return Mono.fromRunnable(() -> {
            try {
                client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(props.getBucket())
                    .key(storagePath)
                    .build());
            } catch (Exception e) {
                log.warn("[S3] Failed to delete {}: {}", storagePath, e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private long doUpload(String key, byte[] data) throws Exception {
        var resp = client.putObject(PutObjectRequest.builder()
            .bucket(props.getBucket())
            .key(key)
            .build(), RequestBody.fromBytes(data));
        log.debug("[S3] Uploaded: {} ({} bytes, etag={})", key, data.length, resp.eTag());
        return data.length;
    }

    private String md5(byte[] data) {
        return hex(data, "MD5");
    }
    private String sha256(byte[] data) {
        return hex(data, "SHA-256");
    }
    private String hex(byte[] data, String algo) {
        try {
            var md = MessageDigest.getInstance(algo);
            byte[] d = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private byte[] doRead(String key) throws Exception {
        try (ResponseInputStream<GetObjectResponse> resp = client.getObject(GetObjectRequest.builder()
                .bucket(props.getBucket())
                .key(key)
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
