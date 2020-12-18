package com.marklogic.pulsar.database;

import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import lombok.Data;

@Data
public class RecordContent {

	AbstractWriteHandle content;
	DocumentMetadataHandle additionalMetadata;
	String id; 
}

