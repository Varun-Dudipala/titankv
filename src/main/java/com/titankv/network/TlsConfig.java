package com.titankv.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;

/**
 * TLS configuration for TitanKV.
 * Provides SSLContext creation for both server and client modes.
 * 
 * Configuration via environment variables or system properties:
 * - TITANKV_TLS_ENABLED / titankv.tls.enabled: Enable TLS (default: false)
 * - TITANKV_TLS_KEYSTORE / titankv.tls.keystore: Path to keystore file (PKCS12 or JKS)
 * - TITANKV_TLS_KEYSTORE_PASSWORD / titankv.tls.keystore.password: Keystore password
 * - TITANKV_TLS_TRUSTSTORE / titankv.tls.truststore: Path to truststore file
 * - TITANKV_TLS_TRUSTSTORE_PASSWORD / titankv.tls.truststore.password: Truststore password
 * - TITANKV_TLS_MUTUAL / titankv.tls.mutual: Require client certificate (default: false)
 */
public class TlsConfig {

    private static final Logger logger = LoggerFactory.getLogger(TlsConfig.class);

    private final boolean enabled;
    private final Path keystorePath;
    private final char[] keystorePassword;
    private final Path truststorePath;
    private final char[] truststorePassword;
    private final boolean mutualTls;
    private final String[] protocols;
    private final String[] cipherSuites;

    private SSLContext sslContext;

    /**
     * Create TLS configuration from environment/system properties.
     */
    public TlsConfig() {
        this.enabled = getBooleanProperty("TITANKV_TLS_ENABLED", "titankv.tls.enabled", false);
        this.keystorePath = getPathProperty("TITANKV_TLS_KEYSTORE", "titankv.tls.keystore");
        this.keystorePassword = getPasswordProperty("TITANKV_TLS_KEYSTORE_PASSWORD", "titankv.tls.keystore.password");
        this.truststorePath = getPathProperty("TITANKV_TLS_TRUSTSTORE", "titankv.tls.truststore");
        this.truststorePassword = getPasswordProperty("TITANKV_TLS_TRUSTSTORE_PASSWORD", "titankv.tls.truststore.password");
        this.mutualTls = getBooleanProperty("TITANKV_TLS_MUTUAL", "titankv.tls.mutual", false);
        
        // Use modern TLS versions only
        this.protocols = new String[]{"TLSv1.3", "TLSv1.2"};
        
        // Strong cipher suites only
        this.cipherSuites = new String[]{
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
        };

        if (enabled) {
            validateConfig();
            initializeSslContext();
        }
    }

    /**
     * Create TLS configuration with explicit settings.
     */
    public TlsConfig(boolean enabled, Path keystorePath, char[] keystorePassword,
                     Path truststorePath, char[] truststorePassword, boolean mutualTls) {
        this.enabled = enabled;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.mutualTls = mutualTls;
        this.protocols = new String[]{"TLSv1.3", "TLSv1.2"};
        this.cipherSuites = new String[]{
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        };

        if (enabled) {
            validateConfig();
            initializeSslContext();
        }
    }

    private void validateConfig() {
        if (keystorePath == null || !Files.exists(keystorePath)) {
            throw new IllegalStateException("TLS enabled but keystore not found: " + keystorePath);
        }
        if (keystorePassword == null || keystorePassword.length == 0) {
            throw new IllegalStateException("TLS enabled but keystore password not set");
        }
        if (mutualTls && (truststorePath == null || !Files.exists(truststorePath))) {
            throw new IllegalStateException("Mutual TLS enabled but truststore not found: " + truststorePath);
        }
    }

    private void initializeSslContext() {
        try {
            // Load keystore (server certificate and private key)
            KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keystorePassword);

            // Load truststore (trusted certificates for client auth)
            TrustManagerFactory tmf = null;
            if (truststorePath != null && Files.exists(truststorePath)) {
                KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword);
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
            }

            // Initialize SSL context
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(
                kmf.getKeyManagers(),
                tmf != null ? tmf.getTrustManagers() : null,
                new SecureRandom()
            );

            logger.info("TLS initialized: protocols={}, mutualTLS={}", 
                String.join(",", protocols), mutualTls);

        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize TLS: " + e.getMessage(), e);
        }
    }

    private KeyStore loadKeyStore(Path path, char[] password) throws Exception {
        String type = path.toString().toLowerCase().endsWith(".p12") ? "PKCS12" : "JKS";
        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream is = new FileInputStream(path.toFile())) {
            keyStore.load(is, password);
        }
        return keyStore;
    }

    /**
     * Check if TLS is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Get the SSLContext for creating SSL engines.
     */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * Create an SSLEngine for server mode.
     */
    public SSLEngine createServerEngine() {
        if (!enabled || sslContext == null) {
            throw new IllegalStateException("TLS not enabled");
        }
        SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setEnabledProtocols(protocols);
        engine.setEnabledCipherSuites(filterSupportedCipherSuites(engine));
        if (mutualTls) {
            engine.setNeedClientAuth(true);
        }
        return engine;
    }

    /**
     * Create an SSLEngine for client mode.
     */
    public SSLEngine createClientEngine(String peerHost, int peerPort) {
        if (!enabled || sslContext == null) {
            throw new IllegalStateException("TLS not enabled");
        }
        SSLEngine engine = sslContext.createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(protocols);
        engine.setEnabledCipherSuites(filterSupportedCipherSuites(engine));
        
        // Enable hostname verification
        SSLParameters params = engine.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(params);
        
        return engine;
    }

    private String[] filterSupportedCipherSuites(SSLEngine engine) {
        java.util.Set<String> supported = new java.util.HashSet<>(
            java.util.Arrays.asList(engine.getSupportedCipherSuites())
        );
        return java.util.Arrays.stream(cipherSuites)
            .filter(supported::contains)
            .toArray(String[]::new);
    }

    /**
     * Whether mutual TLS (client certificate) is required.
     */
    public boolean isMutualTls() {
        return mutualTls;
    }

    private static boolean getBooleanProperty(String envKey, String propKey, boolean defaultValue) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    private static Path getPathProperty(String envKey, String propKey) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        if (value == null || value.isEmpty()) {
            return null;
        }
        return Path.of(value);
    }

    private static char[] getPasswordProperty(String envKey, String propKey) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey);
        }
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value.toCharArray();
    }
}
