package com.marklogic.pulsar.id.strategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

public class PulsarMetaStrategy implements IdStrategy {

	@Override
	public String generateId(AbstractWriteHandle content, String topic, String partition, Long offset) {
		String id = "";
		id = topic + "/" + partition.toString() + "/" + String.valueOf(offset);
		return id;
	}

}
