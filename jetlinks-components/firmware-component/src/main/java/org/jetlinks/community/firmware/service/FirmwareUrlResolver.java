package org.jetlinks.community.firmware.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.URI;

@Component
public class FirmwareUrlResolver {

    private final String accessBaseUrl;

    public FirmwareUrlResolver(@Value("${file.manager.access-base-url:}") String accessBaseUrl) {
        this.accessBaseUrl = trimTrailingSlash(accessBaseUrl);
    }

    public String normalizeForStorage(String url) {
        if (!StringUtils.hasText(url)) {
            return url;
        }
        String value = url.trim();
        try {
            URI uri = URI.create(value);
            String path = uri.getRawPath();
            int filePathIndex = managedFilePathIndex(path);
            if ((uri.isAbsolute() || uri.getRawAuthority() != null) && filePathIndex >= 0) {
                return appendQueryAndFragment(path.substring(filePathIndex), uri);
            }
        } catch (IllegalArgumentException ignore) {
            // Keep manually entered non-standard URLs unchanged.
        }
        if (value.startsWith("file/")) {
            return "/" + value;
        }
        return value;
    }

    public String resolveDownloadUrl(String url) {
        if (!StringUtils.hasText(url)) {
            return url;
        }
        String value = url.trim();
        if (!StringUtils.hasText(accessBaseUrl) || isAbsolute(value)) {
            return value;
        }
        return accessBaseUrl + (value.startsWith("/") ? value : "/" + value);
    }

    private static int managedFilePathIndex(String path) {
        return StringUtils.hasText(path) ? path.indexOf("/file/") : -1;
    }

    private static boolean isAbsolute(String value) {
        try {
            return URI.create(value).isAbsolute() || value.startsWith("//");
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }

    private static String appendQueryAndFragment(String path, URI uri) {
        StringBuilder result = new StringBuilder(path);
        if (uri.getRawQuery() != null) {
            result.append('?').append(uri.getRawQuery());
        }
        if (uri.getRawFragment() != null) {
            result.append('#').append(uri.getRawFragment());
        }
        return result.toString();
    }

    private static String trimTrailingSlash(String value) {
        if (!StringUtils.hasText(value)) {
            return "";
        }
        String result = value.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}
