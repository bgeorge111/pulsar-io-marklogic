package com.marklogic.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration class for the MarkLogic Connectors.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class MarkLogicConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	@FieldDoc(required = true, defaultValue = "localhost", help = "The MarkLogic host that the connector connects to")
	private String mlConnectionHost;

	@FieldDoc(required = true, defaultValue = "8000", help = "The MarkLogic app server port that the connector connects to")
	private int mlConnectionPort;

	@FieldDoc(required = true, defaultValue = "Documents", help = "The MarkLogic database that the connector connects to")
	private String mlDatabase;

	@FieldDoc(required = true, defaultValue = "", help = "The MarkLogic Security Context to create - digest, basic, certificate")
	private String mlSecurityContext;

	@FieldDoc(required = true, defaultValue = "", help = "Name of MarkLogic user to authenticate as")
	private String mlUserName;

	@FieldDoc(required = true, defaultValue = "", help = "Password for the MarkLogic user")
	private String mlPassword;

	@FieldDoc(required = false, defaultValue = "DIRECT", help = "Connection Type; DIRECT or GATEWAY")
	private String mlConnectionType;

	@FieldDoc(required = false, defaultValue = "", help = "Set to true to use a trust-everything SSL connection")
	private Boolean mlSimpleSSL;

	@FieldDoc(required = false, defaultValue = "", help = "Path to the certificate file for certificate authentication")
	private String mlPathToCertFile;

	@FieldDoc(required = false, defaultValue = "", help = "Password for the certificate file")
	private String mlPasswordForCertFile;

	@FieldDoc(required = false, defaultValue = "", help = "External name for Kerberos authentication")
	private String mlExternalName;

	@FieldDoc(required = false, defaultValue = "1", help = "Number of documents to write in each batch of DMSDK")
	private Integer dmsdkBatchSize;

	@FieldDoc(required = false, defaultValue = "1", help = "Number of threads for DMSDK to use")
	private Integer dmsdkThreadCount;

	@FieldDoc(required = false, defaultValue = "", help = "Name of a REST transform to use when writing documents")
	private String dmsdkTransform;

	@FieldDoc(required = false, defaultValue = "", help = "Delimited set of transform parameter names and values")
	private String dmsdkTransformParams;

	@FieldDoc(required = false, defaultValue = ",", help = "Delimiter for transform parameter names and values; defaults to a comma")
	private String dmsdkTransformParamDelimiter;

	@FieldDoc(required = false, defaultValue = "false", help = "Indicates if the topic name should be added to the set of collections for a document")
	private Boolean mlAddTopicAsCollections;

	@FieldDoc(required = false, defaultValue = "", help = "Comma delimited collections to add each document to")
	private String mlDocumentCollections;

	@FieldDoc(required = false, defaultValue = "", help = "Defines format of each document; can be one of json, xml, text, binary, or unknown")
	private String mlDocumentFormat;

	@FieldDoc(required = false, defaultValue = "", help = "Defines the mime type of each document; typically format is set instead of mime type")
	private String mlDocumentMimeType;

	@FieldDoc(required = false, defaultValue = "", help = "Comma delimited permissions to add to each document; role1,capability1,role1,capability2,role2,capability1")
	private String mlDocumentPermissions;

	@FieldDoc(required = false, defaultValue = "", help = "Prefix to prepend to each generated URI")
	private String mlDocumentURIPrefix;

	@FieldDoc(required = false, defaultValue = "", help = "Suffix to append to each generated URI")
	private String mlDocumentURISuffix;

	@FieldDoc(required = false, defaultValue = "false", help = "Whether SSL connection to the App server")
	private Boolean mlSSL;

	@FieldDoc(required = false, defaultValue = "", help = "Version of TLS to connecto MarkLogic SSL enabled App server. Ex. TLSv1.2")
	private String mlTlsVersion;

	@FieldDoc(required = false, defaultValue = "", help = "The strictness of Host Verifier - ANY, COMMON, STRICT")
	private String mlHostNameVerifier;

	@FieldDoc(required = false, defaultValue = "", help = "Mutual Authentication for Basic or Digest: true or false")
	private Boolean mlSSLMutualAuth;

	@FieldDoc(required = false, defaultValue = "", help = "The ID Strategy for URI")
	private String mlIdStrategyForURI;

	@FieldDoc(required = false, defaultValue = "", help = "The JSON path for ID Strategy")
	private String mlIdStrategyPath;

	public static MarkLogicConfig load(String yamlFile) throws IOException {
		final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		final MarkLogicConfig cfg = mapper.readValue(new File(yamlFile), MarkLogicConfig.class);

		return cfg;
	}

	public static MarkLogicConfig load(Map<String, Object> map) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final MarkLogicConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map),
				MarkLogicConfig.class);
		return cfg;
	}

}