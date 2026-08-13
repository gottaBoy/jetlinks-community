package org.jetlinks.community.firmware.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FirmwareUrlResolverTest {

    @Test
    void shouldStoreManagedFileUrlWithoutDomain() {
        FirmwareUrlResolver resolver = new FirmwareUrlResolver("https://ziot.zerontruck.com/");

        assertEquals(
            "/file/file-id.bin?accessKey=key",
            resolver.normalizeForStorage(
                "http://ziot-web.zota.svc.cluster.local:8080/api/file/file-id.bin?accessKey=key"));
        assertEquals(
            "/file/file-id.bin?accessKey=key",
            resolver.normalizeForStorage("/file/file-id.bin?accessKey=key"));
    }

    @Test
    void shouldResolveRelativeUrlOnlyWhenBaseUrlIsConfigured() {
        FirmwareUrlResolver configured = new FirmwareUrlResolver("https://ziot.zerontruck.com/");
        FirmwareUrlResolver unconfigured = new FirmwareUrlResolver("");

        assertEquals(
            "https://ziot.zerontruck.com/file/file-id.bin?accessKey=key",
            configured.resolveDownloadUrl("/file/file-id.bin?accessKey=key"));
        assertEquals(
            "/file/file-id.bin?accessKey=key",
            unconfigured.resolveDownloadUrl("/file/file-id.bin?accessKey=key"));
    }

    @Test
    void shouldKeepExternalAbsoluteUrlUnchangedWhenDispatching() {
        FirmwareUrlResolver resolver = new FirmwareUrlResolver("https://ziot.zerontruck.com");

        assertEquals(
            "https://cdn.example.com/firmware/update.bin",
            resolver.normalizeForStorage("https://cdn.example.com/firmware/update.bin"));
        assertEquals(
            "https://cdn.example.com/firmware/update.bin",
            resolver.resolveDownloadUrl("https://cdn.example.com/firmware/update.bin"));
    }
}
