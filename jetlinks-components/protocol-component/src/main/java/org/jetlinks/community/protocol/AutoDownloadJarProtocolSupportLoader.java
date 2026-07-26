/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.community.protocol;

import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.community.io.utils.FileUtils;
import org.jetlinks.community.protocol.monitor.ProtocolMonitorHelper;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.core.utils.ClassUtils;
import org.jetlinks.community.io.file.FileManager;
import org.jetlinks.community.utils.TimeUtils;
import org.jetlinks.supports.protocol.management.ProtocolSupportDefinition;
import org.jetlinks.supports.protocol.management.jar.JarProtocolSupportLoader;
import org.jetlinks.supports.protocol.management.jar.ProtocolClassLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PreDestroy;
import java.io.*;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static java.nio.file.StandardOpenOption.*;

/**
 * 协议包加载器，通过 configuration.storage 和 configuration.loadType 控制加载策略。
 * <pre>
 *     storage（存储方式）：
 *       local — 本地文件系统路径，支持目录（classes）和 JAR
 *       http  — 远程 HTTP(S) URL，下载到 ./data/protocols/ 缓存后加载
 *       s3    — FileManager(S3/TOS)，检查本地缓存 → 无缓存则下载
 *
 *     loadType（加载方式）：
 *       jar     — JAR 包模式，使用 classpath:{@literal **}/*.class 扫描
 *       classes — class 目录模式，使用 {@literal **}/*.class 扫描
 *       不设置   — 自动检测（目录 → classes，文件 → jar）
 *
 *     本地路径规范（storage=local）：
 *       dev/{protocol}/target/classes/   — class 目录
 *       dev/{protocol}/target/xxx.jar    — JAR 包
 *
 *     S3 缓存路径（storage=s3 / http）：
 *       ./data/protocols/{id}_{hash}.jar  — 自动管理，不手动指定
 *
 *     缓存目录: ./data/protocols（-Djetlinks.protocol.temp.path 可配）
 *     缓存失效: 协议保存时自动清除缓存并重新加载
 * </pre>
 *
 * @author zhouhao
 * @since 1.3
 */
@Slf4j
public class AutoDownloadJarProtocolSupportLoader extends JarProtocolSupportLoader {

    final WebClient webClient;

    final File tempPath;

    private final Duration loadTimeout = TimeUtils.parse(System.getProperty("jetlinks.protocol.load.timeout", "30s"));

    private final FileManager fileManager;
    private final ProtocolMonitorHelper helper;

    public AutoDownloadJarProtocolSupportLoader(WebClient.Builder builder,
                                                FileManager fileManager,
                                                ProtocolMonitorHelper helper) {
        this.webClient = builder.build();
        this.fileManager = fileManager;
        this.helper = helper;
        tempPath = new File(System.getProperty("jetlinks.protocol.temp.path", "./data/protocols"));
        tempPath.mkdirs();
    }

    @Override
    @Autowired
    @Generated
    public void setServiceContext(ServiceContext serviceContext) {
        super.setServiceContext(serviceContext);
    }

    @Override
    @Generated
    protected ServiceContext createServiceContext(ProtocolSupportDefinition definition) {
        String id = definition.getId();
        return CompositeServiceContext.of(
            helper.createMonitor(id),
            deviceId -> helper.createMonitor(id, deviceId),
            CommandSupportServiceContext.INSTANCE,
            super.createServiceContext(definition)
        );
    }

    @Override
    @PreDestroy
    @Generated
    protected void closeAll() {
        super.closeAll();
    }

    @Override
    protected void closeLoader(ProtocolClassLoader loader) {
        super.closeLoader(loader);
    }

    @Override
    public Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition) {

        ProtocolSupportDefinition newDef = FastBeanCopier.copy(definition, new ProtocolSupportDefinition());
        Map<String, Object> config = newDef.getConfiguration();

        String storage = Optional.ofNullable(config.get("storage"))
            .map(String::valueOf).orElse("local");
        String location = Optional.ofNullable(config.get("location"))
            .map(String::valueOf).orElse(null);

        switch (storage) {
            case "http":
                return loadFromHttp(newDef, location);
            case "s3":
                return loadFromS3(newDef);
            default:
                return loadFromLocal(newDef, location);
        }
    }

    // ═══ storage=local — 本地文件系统 ═══
    private Mono<? extends ProtocolSupport> loadFromLocal(ProtocolSupportDefinition newDef, String location) {
        Map<String, Object> config = newDef.getConfiguration();

        if (!StringUtils.hasText(location)) {
            // 未指定 location → 尝试 fileId 兜底
            String fileId = (String) config.get("fileId");
            if (StringUtils.hasText(fileId)) {
                return loadFromS3(newDef);
            }
            return Mono.error(new IllegalArgumentException(
                "storage=local requires location or fileId"));
        }

        File localFile = new File(location);
        if (localFile.exists()) {
            log.info("Loading protocol from local {}: {}",
                localFile.isDirectory() ? "directory" : "file", location);
            return super.load(newDef).subscribeOn(Schedulers.boundedElastic());
        }

        // 本地文件不存在 → 尝试 fileId 兜底（兼容旧配置）
        String fileId = (String) config.get("fileId");
        if (StringUtils.hasText(fileId)) {
            log.info("Local file not found, falling back to fileId: {}", fileId);
            return loadFromFileManager(newDef.getId(), fileId)
                .flatMap(file -> {
                    config.put("location", file.getAbsolutePath());
                    return super.load(newDef)
                        .subscribeOn(Schedulers.boundedElastic())
                        .doOnError(err -> file.delete());
                });
        }

        return Mono.error(new FileNotFoundException(
            "Local protocol file not found: " + location));
    }

    // ═══ storage=http — 远程 HTTP 下载 → 缓存 → 加载 ═══
    private Mono<? extends ProtocolSupport> loadFromHttp(ProtocolSupportDefinition newDef, String location) {
        Map<String, Object> config = newDef.getConfiguration();

        if (!StringUtils.hasText(location) || !location.startsWith("http")) {
            return Mono.error(new IllegalArgumentException(
                "storage=http requires a valid HTTP(S) location"));
        }

        String urlMd5 = DigestUtils.md5Hex(location);
        File file = new File(tempPath, (newDef.getId() + "_" + urlMd5) + ".jar");

        if (file.exists()) {
            config.put("location", file.getAbsolutePath());
            return super.load(newDef)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(err -> file.delete());
        }

        return FileUtils.readDataBuffer(webClient, location)
            .as(dataStream -> {
                log.debug("Download protocol from {} to {}", location, file.getAbsolutePath());
                return DataBufferUtils
                    .write(dataStream, file.toPath(), CREATE, WRITE)
                    .thenReturn(file.getAbsolutePath());
            })
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(path -> config.put("location", path))
            .then(super.load(newDef))
            .timeout(loadTimeout, Mono.error(() ->
                new TimeoutException("Download protocol timeout: " + location)))
            .doOnError(err -> file.delete());
    }

    // ═══ storage=s3 — FileManager(S3/TOS) → 缓存 → 加载 ═══
    private Mono<? extends ProtocolSupport> loadFromS3(ProtocolSupportDefinition newDef) {
        Map<String, Object> config = newDef.getConfiguration();
        String fileId = (String) config.get("fileId");

        if (!StringUtils.hasText(fileId)) {
            return Mono.error(new IllegalArgumentException(
                "storage=s3 requires fileId in configuration"));
        }

        return loadFromFileManager(newDef.getId(), fileId)
            .flatMap(file -> {
                config.put("location", file.getAbsolutePath());
                return super.load(newDef)
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnError(err -> file.delete());
            });
    }

    /**
     * 重写父类扫描逻辑：根据 configuration.loadType 选择 JAR 或目录扫描模式。
     * <p>
     * loadType=jar → classpath 扫描（JAR 内）
     * loadType=classes → 目录扫描
     * 不设置 → 自动检测（目录=classes，文件=jar）
     */
    @Override
    protected ProtocolSupportProvider lookupProvider(String provider,
                                                     ProtocolClassLoader classLoader) {
        // 指定了 provider 全限定类名 → 直接加载
        if (provider != null) {
            try {
                @SuppressWarnings("unchecked")
                Class<ProtocolSupportProvider> providerType =
                    (Class<ProtocolSupportProvider>) classLoader.loadSelfClass(provider);
                return providerType.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("Failed to load provider class: {}", provider, e);
                return null;
            }
        }

        // 自动扫描：根据 loadType 或自动检测决定扫描模式
        boolean isJar = resolveIsJar(classLoader);

        log.debug("Scanning for ProtocolSupportProvider, isJar={}, location={}",
            isJar, classLoader.getUrls().length > 0 ? classLoader.getUrls()[0] : "unknown");

        return ClassUtils
            .findImplClass(
                ProtocolSupportProvider.class,
                isJar ? "classpath:**/*.class" : "**/*.class",
                isJar,
                classLoader,
                ProtocolClassLoader::loadSelfClass)
            .orElse(null);
    }

    /**
     * 解析扫描模式：loadType 配置优先 → 自动检测。
     */
    private boolean resolveIsJar(ProtocolClassLoader classLoader) {
        // TODO: 从 ProtocolSupportDefinition 传递 loadType，当前通过 URL 检测
        URL[] urls = classLoader.getUrls();
        if (urls == null || urls.length == 0) {
            return true; // 默认 JAR 模式
        }
        String path = urls[0].getPath();
        if (path == null) {
            return true;
        }
        return !new File(path).isDirectory();
    }

    /**
     * Clear cached protocol JAR files for the given protocol ID.
     * Called before re-loading to ensure S3/TOS updates are picked up.
     */
    public void clearCache(String protocolId) {
        File[] files = tempPath.listFiles((dir, name) -> name.startsWith(protocolId + "_"));
        if (files != null) {
            for (File f : files) {
                if (f.delete()) {
                    log.info("[ProtocolCache] Deleted cached JAR: {}", f.getName());
                }
            }
        }
    }

    private Mono<File> loadFromFileManager(String protocolId, String fileId) {
        Path path = Paths.get(tempPath.getPath(), (protocolId + "_" + fileId) + ".jar");

        File file = path.toFile();
        if (file.exists()) {
            return Mono.just(file);
        }

        return DataBufferUtils
            .write(fileManager.read(fileId),
                   path, CREATE_NEW, TRUNCATE_EXISTING, WRITE)
            .thenReturn(file);
    }

}
