package com.marklogic.pulsar;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

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
import com.marklogic.hub.DatabaseKind;
import com.marklogic.hub.impl.HubConfigImpl;
import com.marklogic.mgmt.util.SimplePropertySource;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;
import com.marklogic.pulsar.config.MarkLogicSinkConfig;
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
@Connector(name = "marklogic", type = IOType.SINK, help = "A sink connector that sends pulsar messages to marklogic", configClass = MarkLogicSinkConfig.class)
@Slf4j
@Data
public class MarkLogicSink implements Sink<byte[]> {

	private DatabaseClient databaseClient;
	private DataMovementManager dataMovementManager;
	private WriteBatcher writeBatcher;
	private DocumentWriteOperationBuilder documentWriteOperationBuilder;
	private MarkLogicSinkConfig mlConfig;
	
	@Override
	public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
		log.info("Opening MarkLogic Connection");
		mlConfig = MarkLogicSinkConfig.load(config);
		documentWriteOperationBuilder = new DocumentWriteOperationBuilder();
		DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder()
				.buildDatabaseClientConfig(mlConfig.getMarkLogicAbstractConfig());
		databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
		
		HubConfigImpl hubConfig = null;
		
		dataMovementManager = databaseClient.newDataMovementManager();
		writeBatcher = dataMovementManager.newWriteBatcher().withJobName("MarkLogic Sink Connector Job")
				.withBatchSize(mlConfig.getDmsdkBatchSize()).withThreadCount(mlConfig.getDmsdkThreadCount());

		ServerTransform transform = buildServerTransform(mlConfig.getMarkLogicAbstractConfig());
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
		/*
		 * Build a success listener only if flow name is set. All DHF configurations will be ignored 
		 * if a flowName is not configured.
		 */
		final String flowName = mlConfig.getDhfFlowName();
		if (flowName != null && flowName.trim().length() > 0) {
			writeBatcher.onBatchSuccess(buildSuccessListener(flowName, mlConfig, hubConfig));
		}

		dataMovementManager.startJob(writeBatcher);
		log.info("Opened MarkLogic Connection with a Write Batcher. Job ID = {}", writeBatcher.getJobId());
		return;
	}
	
	protected RunFlowWriteBatchListener buildSuccessListener(String flowName, MarkLogicSinkConfig mlConfig, HubConfigImpl hubConfig) {
		String logMessage = String.format("After ingesting a batch, will run flow '%s'", flowName);
		final String flowSteps = mlConfig.getDhfFlowSteps();
		List<String> steps = null;
		if (flowSteps != null && flowSteps.trim().length() > 0) {
			steps = Arrays.asList(flowSteps.split(","));
			logMessage += String.format(" with steps '%s' constrained to the URIs in that batch", steps.toString());
		}
		log.info(logMessage);
		hubConfig = buildHubConfig(mlConfig);
		RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener(flowName, steps, hubConfig);
		return listener;
	}

	protected HubConfigImpl buildHubConfig(MarkLogicSinkConfig mlConfig) {
		/*Start with default and then customize based on the configuration. Many times default will not be enough
		 * 
		 */
		HubConfigImpl hubConfig = HubConfigImpl.withDefaultProperties();
        hubConfig.setHost(mlConfig.getMlConnectionHost());
        hubConfig.setMlUsername(mlConfig.getMlUserName());
        hubConfig.setMlPassword(mlConfig.getMlPassword());
        
        /*
         * Customize the hubConfig if dhfType=dhs & dhfProperties = default. 
         */
        if ( "DHS".equalsIgnoreCase(mlConfig.getDhfType()) && 
        	 "DEFAULT".equalsIgnoreCase(mlConfig.getDhfProperties())) 
        {
        	log.info("Creating DHS Default hubConfig.");
        	hubConfig = buildDHSDefaultHubConfig(hubConfig);
        }
        
        /*
         * Customize the hubConfig by loading the properties from properties file.   
         */
        
        if ("CUSTOM".equalsIgnoreCase(mlConfig.getDhfProperties())) 
        {
        	log.info("Creating custom hub configuration based on configuration.");
        	hubConfig = buildCustomHubConfig(mlConfig,hubConfig);
        }
        
        return hubConfig;
	}

	protected HubConfigImpl buildCustomHubConfig(MarkLogicSinkConfig mlConfig, HubConfigImpl hubConfig) {
		Properties props = new Properties();
		InputStream input = null;
		
		try {
			input = new FileInputStream(mlConfig.getDhfPropertiesPath());
		} catch (FileNotFoundException e) {
			log.error("Unable  to create HubConfig. Pls check the configurations again" + e.getMessage());
		}

		try {
			props.load(input);
		} catch (IOException e) {
			log.error("Unable  to create HubConfig. Pls check the configurations again" + e.getMessage());
		}
			
		hubConfig.applyProperties(new SimplePropertySource(props));
		return hubConfig;
	}
	
	protected HubConfigImpl buildDHSDefaultHubConfig(HubConfigImpl hubConfig) {
		
		Stream.of(DatabaseKind.STAGING, DatabaseKind.FINAL, DatabaseKind.JOB).forEach(kind -> hubConfig.setAuthMethod(kind, "basic"));
		Stream.of(DatabaseKind.STAGING, DatabaseKind.FINAL, DatabaseKind.JOB).forEach(kind -> hubConfig.setSimpleSsl(kind, true));
		hubConfig.setPort(DatabaseKind.STAGING, 8010);
		hubConfig.setPort(DatabaseKind.FINAL, 8011);
		hubConfig.setPort(DatabaseKind.JOB, 8013);
		Properties props = new Properties();
		props.setProperty("mlIsHostLoadBalancer", "true");
		hubConfig.applyProperties(new SimplePropertySource(props));
		return hubConfig;
		
	}
	
	protected ServerTransform buildServerTransform(final MarkLogicAbstractConfig mlConfig) {
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
			recordContent.setAdditionalMetadata(meta.withCollections(record.getTopicName().get()));
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