package com.marklogic.pulsar.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
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
 * Configuration class for the MarkLogic Sink Connectors.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
@Accessors(chain = true)
public class MarkLogicSinkConfig extends MarkLogicAbstractConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	@FieldDoc(required = false, defaultValue = "false", help = "Indicates if the topic name should be added to the set of collections for a document")
	private Boolean mlAddTopicAsCollections;

	@FieldDoc(required = false, defaultValue = "", help = "Comma delimited collections to add each document to")
	private String mlDocumentCollections;

	@FieldDoc(required = true, defaultValue = "", help = "Defines format of each document; can be one of json, xml, text, binary, or unknown")
	private String mlDocumentFormat;

	@FieldDoc(required = false, defaultValue = "", help = "Defines the mime type of each document; typically format is set instead of mime type")
	private String mlDocumentMimeType;

	@FieldDoc(required = false, defaultValue = "", help = "Comma delimited permissions to add to each document; role1,capability1,role1,capability2,role2,capability1")
	private String mlDocumentPermissions;

	@FieldDoc(required = false, defaultValue = "", help = "Prefix to prepend to each generated URI")
	private String mlDocumentURIPrefix;

	@FieldDoc(required = false, defaultValue = "", help = "Suffix to append to each generated URI")
	private String mlDocumentURISuffix;

	@FieldDoc(required = false, defaultValue = "", help = "The ID Strategy for URI")
	private String mlIdStrategyForURI;

	@FieldDoc(required = false, defaultValue = "", help = "The JSON path for ID Strategy")
	private String mlIdStrategyPath;
	
	public static MarkLogicSinkConfig load(String yamlFile) throws IOException {
		final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		final MarkLogicSinkConfig cfg = mapper.readValue(new File(yamlFile), MarkLogicSinkConfig.class);

		return cfg;
	}

	public static MarkLogicSinkConfig load(Map<String, Object> map) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));

		final MarkLogicSinkConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map),
				MarkLogicSinkConfig.class);
		return cfg;
	}
	
	public MarkLogicAbstractConfig getMarkLogicAbstractConfig()
	{
		return this;
	}

}