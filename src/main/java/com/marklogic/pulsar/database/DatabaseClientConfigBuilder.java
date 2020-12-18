package com.marklogic.pulsar.database;

import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.pulsar.MarkLogicConfig;

/**
 * Defines how a map of properties read in by Kafka are used to build an instance of DatabaseClientConfig.
 */
public interface DatabaseClientConfigBuilder {

	DatabaseClientConfig buildDatabaseClientConfig(MarkLogicConfig mlConfig);

}
