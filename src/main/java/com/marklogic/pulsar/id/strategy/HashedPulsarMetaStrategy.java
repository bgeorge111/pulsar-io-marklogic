package com.marklogic.pulsar.id.strategy;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import com.marklogic.client.io.marker.AbstractWriteHandle;

@Slf4j
public class HashedPulsarMetaStrategy implements IdStrategy {
	
	@Override
	public String generateId(AbstractWriteHandle content, String topic, String partition, Long offset) {
		String id = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			String tmp = topic + partition.toString() + String.valueOf(offset);
			id = bytesToHex(md.digest(tmp.getBytes()));
			return id;
		} catch (NoSuchAlgorithmException e) {
			log.warn("NoSuchAlgorithmException. Not creating MD5 URI, instead generating UUID");
			return UUID.randomUUID().toString();
		}
	}
	
	private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
        sb.append(String.format("%02x", b));
    }
    return sb.toString();
	}
}
