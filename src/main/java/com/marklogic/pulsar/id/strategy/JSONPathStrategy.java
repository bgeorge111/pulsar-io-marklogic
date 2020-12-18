package com.marklogic.pulsar.id.strategy;

import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.io.marker.AbstractWriteHandle;

@Slf4j
public class JSONPathStrategy implements IdStrategy{
	
	private String path;
	
	public JSONPathStrategy (String path) {
		this.path = path;
	}
	
	@Override
	public String generateId(AbstractWriteHandle content, String topic, String partition, Long offset) {
		ObjectMapper om = new ObjectMapper();
		try {
			JsonNode node = om.readTree(content.toString());
			String id = node.at(path).asText();
			return id;
		}
		catch (IOException e) {
			log.warn("IOException. Not creating JSONPATH URI, instead generating UUID");
			return UUID.randomUUID().toString();
		}
	}
	
} 
