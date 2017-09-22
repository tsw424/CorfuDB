package org.corfudb.security.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;

/**
 * Utilities for common options parsing and session configuration for
 * encrypted & authenticated TLS sessions.
 */
@Slf4j
public class TlsUtils {
    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param isServer Server or client
     * @param keyStorePath Key store path string
     * @param ksPasswordFile Key store password file string
     * @param trustStorePath Trust store path string
     * @param tsPasswordFile Trust store password file path string
     * @return SslContext object or null on error
     */
    public static SslContext enableTls(boolean isServer,
                                       String keyStorePath,
                                       String ksPasswordFile,
                                       String trustStorePath,
                                       String tsPasswordFile) throws SSLException{
        // Get the key store password
        String ksp = "";
        if (ksPasswordFile != null) {
            try {
                ksp = (new String(Files.readAllBytes(Paths.get(ksPasswordFile)))).trim();
            } catch (IOException e) {
                String errorMessage = "Unable to read key store password file " + ksPasswordFile + ".";
                log.error(errorMessage, e);
                throw new SSLException(errorMessage, e);
            }
        }

        // Get the key store
        if (keyStorePath == null) {
            String errorMessage = "Certificate key store is not specified.";
            log.error(errorMessage);
            throw new SSLException(errorMessage);
        }

        KeyManagerFactory kmf;
        try {
            FileInputStream fis = new FileInputStream(keyStorePath);
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(fis, ksp.toCharArray());

            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, ksp.toCharArray());
        } catch (IOException e) {
            String errorMessage = "Unable to read key store file " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (CertificateException e) {
            String errorMessage = "Unable to read certificate in key store file " + trustStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "No support for algorithm indicated in the key store " + trustStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Error loading key store " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (UnrecoverableKeyException e) {
            String errorMessage = "Unrecoverable key in key store " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        }

        if (trustStorePath == null) {
            String errorMessage = "Trust store is not specified.";
            log.error(errorMessage);
            throw new SSLException(errorMessage);
        }

        ReloadableTrustManagerFactory tmf = new ReloadableTrustManagerFactory(trustStorePath, tsPasswordFile);

        if (isServer) {
            return SslContextBuilder.forServer(kmf).trustManager(tmf).build();
        } else {
            return SslContextBuilder.forClient().keyManager(kmf).trustManager(tmf).build();
        }
    }

}
