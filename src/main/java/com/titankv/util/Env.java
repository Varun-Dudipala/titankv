package com.titankv.util;

/**
 * Reads configuration from environment variables, falling back to system properties.
 * Values are read on every call so tests can change system properties between servers.
 */
public final class Env {

    private Env() {
    }

    /**
     * @return the env var value, else the system property value, else null (empty counts as unset)
     */
    public static String get(String envKey, String propKey) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        return value != null && !value.isEmpty() ? value : null;
    }

    public static boolean getBoolean(String envKey, String propKey, boolean fallback) {
        String value = get(envKey, propKey);
        if (value == null) {
            return fallback;
        }
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    public static boolean isDevMode() {
        return getBoolean("TITANKV_DEV_MODE", "titankv.dev.mode", false);
    }

    public static String clusterSecret() {
        return get("TITANKV_CLUSTER_SECRET", "titankv.cluster.secret");
    }

    public static String clientToken() {
        return get("TITANKV_CLIENT_TOKEN", "titankv.client.token");
    }

    /**
     * Token that node-to-node connections authenticate with. Defaults to the cluster secret.
     */
    public static String internalToken() {
        String token = get("TITANKV_INTERNAL_TOKEN", "titankv.internal.token");
        return token != null ? token : clusterSecret();
    }
}
