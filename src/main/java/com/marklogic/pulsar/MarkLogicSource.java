package com.marklogic.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchPushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.JobReport;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.ProgressListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCtsQueryDefinition;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;
import com.marklogic.pulsar.config.MarkLogicSourceConfig;
import com.marklogic.pulsar.database.DefaultDatabaseClientConfigBuilder;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * The base class for MarkLogic source.
 */

@Connector(name = "marklogic", type = IOType.SOURCE, help = "A source connector that sends Marklogic documents to pulsar", configClass = MarkLogicSourceConfig.class)
@Slf4j
public class MarkLogicSource extends BatchPushSource<byte[]> implements Runnable {

	private DatabaseClient databaseClient;
	private DataMovementManager dataMovementManager;
	private MarkLogicSourceConfig mlConfig;
	private SourceContext sourceContext;
	private QueryManager queryMgr;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

	@Override
	public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
		log.info("Opening MarkLogic Connection");
		mlConfig = MarkLogicSourceConfig.load(config);
		DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder()
				.buildDatabaseClientConfig(mlConfig.getMarkLogicAbstractConfig());
		databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
		dataMovementManager = databaseClient.newDataMovementManager();
		queryMgr = databaseClient.newQueryManager();
		this.sourceContext = sourceContext;
		return;
	}

	@Override
	public void discover(Consumer taskEater) throws Exception {
	    taskEater.accept(String.format("MarkLogic Source Task -%d", System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public void prepare(byte[] instanceSplit) throws Exception {
		 log.info("Instance " + sourceContext.getInstanceId() + " got a new discovered source task {}",
		            new String(instanceSplit, StandardCharsets.UTF_8));
		    executor.submit(this);
	}
	
	@Override
	public void close() throws Exception {
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
	
	private QueryBatcher getQueryBatcher(MarkLogicSourceConfig mlConfig) {
		
		String serializedQuery = "";
		QueryBatcher queryBatcher;
		if ("FALSE".equalsIgnoreCase(mlConfig.getDmsdkIsSourceQuerySerialized())) {
			serializedQuery = databaseClient.newServerEval().javascript(mlConfig.getDmsdkSourceQuery()+ ".toObject()")
					.evalAs(String.class);
		} else {
			serializedQuery = "{'ctsquery':" + mlConfig.getDmsdkSourceQuery()+ "}";
		}
		ServerTransform transform = buildServerTransform(mlConfig.getMarkLogicAbstractConfig());
	
		StringHandle handle = new StringHandle("{'ctsquery':" + serializedQuery + "}").withFormat(Format.JSON);
		RawCtsQueryDefinition query = queryMgr.newRawCtsQueryDefinition(handle);
		ExportListener exportListener = new ExportListener().withConsistentSnapshot();
		if (transform != null) {
			exportListener.withTransform(transform);
		}
		queryBatcher = dataMovementManager.newQueryBatcher(query)
										.withJobName("Query Batcher Job")
										.withBatchSize(mlConfig.getDmsdkBatchSize())
										.withThreadCount(mlConfig.getDmsdkThreadCount())
										.onUrisReady(exportListener.onDocumentReady(doc -> {
																		try {
																				String jsonDoc = doc.getContentAs(String.class);
																				consume(new MarkLogicRecord(jsonDoc.getBytes()));
																			} catch (final Exception e) {
																		throw new RuntimeException(e);
																		}
																		}))
										.onUrisReady(new ProgressListener().onProgressUpdate(progressUpdate -> {
															log.info(progressUpdate.getProgressAsString());
													}))
										.withConsistentSnapshot();

		return  queryBatcher;
	}
	
	@Override
	  public void run() {
		QueryBatcher queryBatcher = getQueryBatcher(mlConfig);
		JobTicket ticket = dataMovementManager.startJob(queryBatcher);
		queryBatcher.awaitCompletion();
		dataMovementManager.stopJob(queryBatcher);
		JobReport report = dataMovementManager.getJobReport(ticket);
		log.info("The task {}  finished in {} seconds with {} successful events and {} failed events.",
				ticket.getJobId(),
				(report.getJobEndTime().getTimeInMillis() - report.getJobStartTime().getTimeInMillis())/1000, 
				report.getSuccessEventsCount(),
				report.getFailureEventsCount()
				);
		consume(null);
	  }

	@Data
	static private class MarkLogicRecord implements Record<byte[]> {
		private final byte[] value;
	}
}