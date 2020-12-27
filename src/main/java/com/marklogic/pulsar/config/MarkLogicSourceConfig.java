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
public class MarkLogicSourceConfig extends MarkLogicAbstractConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	@FieldDoc(required = false, defaultValue = "", help = "The query to be used for Source Connector")
	private String dmsdkSourceQuery;
	
	@FieldDoc(required = false, defaultValue = "", help = "The query to be used for Source Connector is serialized or not")
	private String dmsdkIsSourceQuerySerialized; 

	
	public static MarkLogicSourceConfig load(String yamlFile) throws IOException {
		final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		final MarkLogicSourceConfig cfg = mapper.readValue(new File(yamlFile), MarkLogicSourceConfig.class);

		return cfg;
	}

	public static MarkLogicSourceConfig load(Map<String, Object> map) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));

		final MarkLogicSourceConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map),
				MarkLogicSourceConfig.class);
		return cfg;
	}
	
	public MarkLogicAbstractConfig getMarkLogicAbstractConfig()
	{
		return this;
	}

}