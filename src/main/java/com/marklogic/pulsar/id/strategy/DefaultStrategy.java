package com.marklogic.pulsar.id.strategy;

import java.util.UUID;

import com.marklogic.client.io.marker.AbstractWriteHandle;


public class DefaultStrategy implements IdStrategy{
	
	@Override
	public String generateId(AbstractWriteHandle content, String topic, String partition, Long offset) {
		return UUID.randomUUID().toString();
	}
    
} 
