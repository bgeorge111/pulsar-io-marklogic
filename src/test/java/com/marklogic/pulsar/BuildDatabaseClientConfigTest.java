package com.marklogic.pulsar;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;
import com.marklogic.pulsar.database.DefaultDatabaseClientConfigBuilder;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.marklogic.client.DatabaseClientFactory;

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

public class BuildDatabaseClientConfigTest {

	DefaultDatabaseClientConfigBuilder builder = new DefaultDatabaseClientConfigBuilder();

	MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();

	@BeforeClass
	public void setup() throws IOException {

		config.setMlConnectionHost("some-host");
		config.setMlConnectionPort(8123);
		config.setMlDatabase("some-database");
	}

	@Test
	public void basicAuthentication() {

		config.setMlSecurityContext("basic");
		config.setMlUserName("some-user");
		config.setMlPassword("some-password");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals("some-host", clientConfig.getHost());
		assertEquals(8123, clientConfig.getPort());
		assertEquals("some-database", clientConfig.getDatabase());
		assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
		assertEquals("some-user", clientConfig.getUsername());
		assertEquals("some-password", clientConfig.getPassword());
	}

	@Test
	public void certificateAuthentication() {
		config.setMlSecurityContext("certificate");
		config.setMlPathToCertFile("/path/to/file");
		config.setMlPasswordForCertFile("cert-password");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.CERTIFICATE, clientConfig.getSecurityContextType());
		assertEquals("/path/to/file", clientConfig.getCertFile());
		assertEquals("cert-password", clientConfig.getCertPassword());
	}

	@Test
	public void kerberosAuthentication() {
		config.setMlSecurityContext("kerberos");
		config.setMlExternalName("some-name");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.KERBEROS, clientConfig.getSecurityContextType());
		assertEquals("some-name", clientConfig.getExternalName());
	}

	@Test
	public void digestAuthenticationAndSimpleSsl() {
		config.setMlSecurityContext("digest");
		config.setMlSimpleSSL(true);

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertNotNull(clientConfig.getSslHostnameVerifier());
		assertNotNull(clientConfig.getTrustManager());
	}

	@Test
	public void basictAuthenticationAndSimpleSsl() {
		config.setMlSecurityContext("basic");
		config.setMlSimpleSSL(true);

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertNotNull(clientConfig.getSslHostnameVerifier());
		assertNotNull(clientConfig.getTrustManager());
	}

	@Test
	public void basicAuthenticationAndMutualSSL() {
		File file = new File("src/test/resources/srportal.p12");
		String absolutePath = file.getAbsolutePath();

		config.setMlSecurityContext("basic");
		config.setMlSimpleSSL(false);
		config.setMlSSL(true);
		config.setMlTlsVersion("TLSv1.2");
		config.setMlHostNameVerifier("STRICT");
		config.setMlSSLMutualAuth(true);
		config.setMlPathToCertFile(absolutePath);
		config.setMlPasswordForCertFile("abc");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertEquals(DatabaseClientFactory.SSLHostnameVerifier.STRICT, clientConfig.getSslHostnameVerifier());
		assertNotNull(clientConfig.getTrustManager());
	}

	@Test
	public void basicAuthenticationAndMutualSSLWithInvalidHost() {
		File file = new File("src/test/resources/srportal.p12");
		String absolutePath = file.getAbsolutePath();
		config.setMlSecurityContext("basic");
		config.setMlSimpleSSL(false);
		config.setMlSSL(true);
		config.setMlTlsVersion("TLSv1.2");
		config.setMlHostNameVerifier("SOMETHING");
		config.setMlSSLMutualAuth(true);
		config.setMlPathToCertFile(absolutePath);
		config.setMlPasswordForCertFile("abc");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.BASIC, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertEquals(DatabaseClientFactory.SSLHostnameVerifier.ANY, clientConfig.getSslHostnameVerifier());
		System.out.println(clientConfig.getSslHostnameVerifier());
		assertNotNull(clientConfig.getTrustManager());
	}

	@Test
	public void digestAuthenticationAnd1WaySSL() {

		config.setMlSecurityContext("digest");
		config.setMlSimpleSSL(false);
		config.setMlSSL(true);
		config.setMlTlsVersion("TLSv1.2");
		config.setMlHostNameVerifier("STRICT");
		config.setMlSSLMutualAuth(false);

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(SecurityContextType.DIGEST, clientConfig.getSecurityContextType());
		assertNotNull(clientConfig.getSslContext());
		assertNotNull(clientConfig.getSslHostnameVerifier());
		assertNotNull(clientConfig.getTrustManager());
	}

	@Test
	public void gatewayConnection() {
		config.setMlSecurityContext("digest");
		config.setMlConnectionType("gateway");

		DatabaseClientConfig clientConfig = builder.buildDatabaseClientConfig(config);
		assertEquals(DatabaseClient.ConnectionType.GATEWAY, clientConfig.getConnectionType());
	}
}
