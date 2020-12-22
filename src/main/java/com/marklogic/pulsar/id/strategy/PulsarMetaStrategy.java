package com.marklogic.pulsar.id.strategy;

import com.marklogic.client.io.marker.AbstractWriteHandle;

public class PulsarMetaStrategy implements IdStrategy {

	@Override
	public String generateId(AbstractWriteHandle content, String topic, String partition, Long offset) {
		String id = "";
		/*
		 * A pulsar topic is having structure {persistent|non-persistent}://tenant/namespace/topic.
		 * The URI will remove the :/ and will have the format {persistent|non-persistent}/tenant/namespace/topic
		 * So, a full URI will be like {persistent|non-persistent}/tenant/namespace/topic/partition/sequence
		 */
		id = topic.replace(":/", "") + "/" + partition.toString() + "/" + String.valueOf(offset);
		return id;
	}

}
