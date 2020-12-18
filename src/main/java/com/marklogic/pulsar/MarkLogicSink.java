package com.marklogic.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.pulsar.database.DefaultDatabaseClientConfigBuilder;
import com.marklogic.pulsar.database.DocumentWriteOperationBuilder;
import com.marklogic.pulsar.database.RecordContent;
import com.marklogic.pulsar.id.strategy.IdStrategy;
import com.marklogic.pulsar.id.strategy.IdStrategyFactory;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * The base class for MarkLogic sink. Users need to implement extractKeyValue
 * function to use this sink. This class assumes that the input will be JSON
 * documents.
 */
@Connector(name = "marklogic", type = IOType.SINK, help = "A sink connector that sends pulsar messages to marklogic", configClass = MarkLogicConfig.class)
@Slf4j
@Data
public class MarkLogicSink implements Sink<byte[]> {

	private DatabaseClient databaseClient;
	private DataMovementManager dataMovementManager;
	private WriteBatcher writeBatcher;
	private DocumentWriteOperationBuilder documentWriteOperationBuilder;
	private MarkLogicConfig mlConfig;
	
	@Override
	public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
		log.info("Opening MarkLogic Connection");
		mlConfig = MarkLogicConfig.load(config);
		documentWriteOperationBuilder = new DocumentWriteOperationBuilder();
		DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder()
				.buildDatabaseClientConfig(mlConfig);
		databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

		dataMovementManager = databaseClient.newDataMovementManager();
		writeBatcher = dataMovementManager.newWriteBatcher().withJobName("MarkLogic Sink Connector Job")
				.withBatchSize(mlConfig.getDmsdkBatchSize()).withThreadCount(mlConfig.getDmsdkThreadCount());

		ServerTransform transform = buildServerTransform(mlConfig);
		if (transform != null) {
			writeBatcher.withTransform(transform);
		}

		writeBatcher.onBatchSuccess(batch -> {
			if (log.isDebugEnabled()) {
				log.info("Marklogic Connector::Batch {} wrote, {} at {}", batch.getJobBatchNumber(),
						batch.getJobWritesSoFar(), batch.getTimestamp().getTime());
			}
		});
		writeBatcher.onBatchFailure((batch, throwable) -> {
			log.warn("Marklogic Connector::Batch Failed. Retrying" + throwable.getMessage());
			try {
				batch.getBatcher().retry(batch);
			} catch (Exception e) {
				log.warn("Marklogic Connector::Batch Failed on Retrying also." + e.getMessage());
			}
		});

		dataMovementManager.startJob(writeBatcher);
		log.info("Opened MarkLogic Connection with a Write Batcher. Job ID = {}", writeBatcher.getJobId());
		return;
	}

	protected ServerTransform buildServerTransform(final MarkLogicConfig mlConfig) {
		String transform = mlConfig.getDmsdkTransform();
		if (transform != null && transform.trim().length() > 0) {
			ServerTransform t = new ServerTransform(transform);
			String params = mlConfig.getDmsdkTransformParams();
			if (params != null && params.trim().length() > 0) {
				String delimiter = mlConfig.getDmsdkTransformParamDelimiter();
				if (delimiter != null && delimiter.trim().length() > 0) {
					String[] tokens = params.split(delimiter);
					for (int i = 0; i < tokens.length; i += 2) {
						if (i + 1 >= tokens.length) {
							throw new IllegalArgumentException(String.format(
									"The value of the %s property does not have an even number of "
											+ "parameter names and values; property value: %s",
									mlConfig.getDmsdkTransformParams(), params));
						}
						t.addParameter(tokens[i], tokens[i + 1]);
					}
				} else {
					log.warn(String.format(
							"Unable to apply transform parameters to transform: %s; please set the "
									+ "delimiter via the %s property",
							transform, mlConfig.getDmsdkTransformParamDelimiter()));
				}
			}
			return t;
		}
		return null;
	}

	@Override
	public void write(Record<byte[]> record) {
		IdStrategy idStrategy = null;
		final String recordValue = new String(record.getValue(), StandardCharsets.UTF_8);
		RecordContent recordContent = new RecordContent();
		if (log.isDebugEnabled()) {
			log.debug("MarkLogic Connector received record: " + recordValue);
		}
		idStrategy = IdStrategyFactory.getIdStrategy(mlConfig);
		AbstractWriteHandle content = toContent(recordValue);
		DocumentMetadataHandle meta = new DocumentMetadataHandle();
		documentWriteOperationBuilder.withCollections(mlConfig.getMlDocumentCollections());
		documentWriteOperationBuilder.withUriPrefix(mlConfig.getMlDocumentURIPrefix());
		documentWriteOperationBuilder.withUriSuffix(mlConfig.getMlDocumentURISuffix());
		recordContent.setContent(content);
		recordContent.setId(idStrategy.generateId(content, record.getTopicName().get(), record.getPartitionId().get(),
				record.getRecordSequence().get()));
		if (mlConfig.getMlAddTopicAsCollections()) {
			recordContent.setAdditionalMetadata(meta.withCollections(record.getTopicName().get().replace("persistent://", "")));
		}
		try {
			writeBatcher.add(documentWriteOperationBuilder.build(recordContent));
			record.ack();
		} catch (Exception e) {
			log.error("Error in Writing Record to MarkLogic::" + e.getMessage());
			record.fail();
		}
		
	}

	protected AbstractWriteHandle toContent(String recordValue) {
		StringHandle content = new StringHandle(recordValue);
		return content;
	}

	@Override
	public void close() throws Exception {
		if (writeBatcher != null) {
			writeBatcher.flushAndWait();
			dataMovementManager.stopJob(writeBatcher);
		}
		if (databaseClient != null) {
			databaseClient.release();
		}
	}
}