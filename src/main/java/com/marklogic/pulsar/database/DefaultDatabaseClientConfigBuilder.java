package com.marklogic.pulsar.database;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;

public class DefaultDatabaseClientConfigBuilder implements DatabaseClientConfigBuilder {

	@Override
	public DatabaseClientConfig buildDatabaseClientConfig(MarkLogicAbstractConfig mlConfig) {
		DatabaseClientConfig clientConfig = new DatabaseClientConfig();
		clientConfig.setCertFile(mlConfig.getMlPathToCertFile());
		clientConfig.setCertPassword(mlConfig.getMlPasswordForCertFile());
		clientConfig.setTrustManager(new SimpleX509TrustManager());
		clientConfig = configureHostNameVerifier(clientConfig,mlConfig);
		String securityContextType = (mlConfig.getMlSecurityContext()).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));
		String database = mlConfig.getMlDatabase();
		if (database != null && database.trim().length() > 0) {
			clientConfig.setDatabase(database);
		}
		String connType = mlConfig.getMlConnectionType();
		if (connType != null && connType.trim().length() > 0) {
			clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(connType.toUpperCase()));
		}
		clientConfig.setExternalName(mlConfig.getMlExternalName());
		clientConfig.setHost(mlConfig.getMlConnectionHost());
		clientConfig.setPassword(mlConfig.getMlPassword());
		clientConfig.setPort(mlConfig.getMlConnectionPort());
		Boolean customSsl = mlConfig.getMlSSL();
		if (customSsl != null && customSsl) {
			clientConfig = configureCustomSslConnection(clientConfig, mlConfig);
		}
		Boolean simpleSsl = mlConfig.getMlSimpleSSL();
		if (simpleSsl != null && simpleSsl) {
			clientConfig = configureSimpleSsl(clientConfig);
		}
		clientConfig.setUsername(mlConfig.getMlUserName());
		return clientConfig;
	}

	/**
	 * This provides a "simple" SSL configuration in that it uses the JVM's default SSLContext and
	 * a "trust everything" hostname verifier. No default TrustManager is configured because in the absence of one,
	 * the JVM's cacerts file will be used.
	 *
	 * @param clientConfig
	 */
	protected DatabaseClientConfig configureSimpleSsl(DatabaseClientConfig clientConfig) {
		clientConfig.setSslContext(SimpleX509TrustManager.newSSLContext());
		clientConfig.setTrustManager(new SimpleX509TrustManager());
		clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		return clientConfig;
	}
	/**
	 * This function configures the Host Name verifier based on the configuration.
	 * ANY, STRICT and COMMON are the possible values, ANY being default.
	 *
	 * @param clientConfig
	 */
	protected DatabaseClientConfig configureHostNameVerifier(DatabaseClientConfig clientConfig, MarkLogicAbstractConfig mlConfig) {
		String sslHostNameVerifier = mlConfig.getMlHostNameVerifier();
		if ("ANY".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		else if ("COMMON".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
		else if ("STRICT".equals(sslHostNameVerifier))
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
		else
			clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
		return clientConfig;
	}

	protected DatabaseClientConfig configureCustomSslConnection(DatabaseClientConfig clientConfig, MarkLogicAbstractConfig mlConfig) {
		Boolean ssl = mlConfig.getMlSSL();
		String tlsVersion = mlConfig.getMlTlsVersion();
		Boolean sslMutualAuth = mlConfig.getMlSSLMutualAuth();
		SSLContext sslContext = null;
		String securityContextType = (mlConfig.getMlSecurityContext()).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));

		if ("BASIC".equals(securityContextType) ||
				"DIGEST".equals(securityContextType)
				) {
					if (ssl != null && ssl.booleanValue()) {
						if (sslMutualAuth != null && sslMutualAuth.booleanValue()) {
							/*2 way ssl changes*/
							KeyStore clientKeyStore = null;
							try {
								clientKeyStore = KeyStore.getInstance("PKCS12");
							} catch (KeyStoreException e) {

								throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
							}
					        TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager()};
					        
					        try (InputStream keystoreInputStream = new FileInputStream(clientConfig.getCertFile())) {
					            clientKeyStore.load(keystoreInputStream, clientConfig.getCertPassword().toCharArray());
					        } catch (Exception e) {
								throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
							}
					        KeyManagerFactory keyManagerFactory = null;
							try {
								keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
							} catch (Exception e) {

								throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
							}
					        try {
								keyManagerFactory.init(clientKeyStore, clientConfig.getCertPassword().toCharArray());
							} catch (Exception e) {

								throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
							}
					        KeyManager[] key = keyManagerFactory.getKeyManagers();
							try {
								if (tlsVersion != null && tlsVersion.trim().length() > 0 ) {
									sslContext = SSLContext.getInstance(tlsVersion);
								}
								else {
									sslContext = SSLContext.getInstance("TLSv1.2");
								}
							} catch (NoSuchAlgorithmException e) {

								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
					        try {
								sslContext.init(key, trust, null);
							} catch (KeyManagementException e) {
								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
							clientConfig.setSslContext(sslContext);
						}
						else {/*1wayssl*/
							TrustManager[] trust = new TrustManager[] { new SimpleX509TrustManager()};
							try {
									if (tlsVersion != null && tlsVersion.trim().length() > 0 ) {
										sslContext = SSLContext.getInstance(tlsVersion);
									}
									else {
										sslContext = SSLContext.getInstance("TLSv1.2");
									}
								} catch (NoSuchAlgorithmException e) {
								throw new RuntimeException("Unable to configure custom SSL connection: " + e.getMessage(), e);
							}
							try {
								sslContext.init(null, trust, null);
							}catch (KeyManagementException e) {
								throw new RuntimeException("Unable to configure custom SSL connection:" + e.getMessage(), e);
							}
							clientConfig.setSslContext(sslContext);
						}
					} /* End of if ssl */
			}
		return clientConfig;
	}
}
