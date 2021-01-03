package com.marklogic.pulsar.database;

import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;

/**
 * Defines how a map of properties read in by Pulsar are used to build an instance of DatabaseClientConfig.
 */
public interface DatabaseClientConfigBuilder {

	DatabaseClientConfig buildDatabaseClientConfig(MarkLogicAbstractConfig mlConfig);

}
