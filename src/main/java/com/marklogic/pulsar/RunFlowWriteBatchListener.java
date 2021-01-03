package com.marklogic.pulsar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.marklogic.client.datamovement.WriteBatch;
import com.marklogic.client.datamovement.WriteBatchListener;
import com.marklogic.client.datamovement.WriteEvent;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.FlowRunner;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.flow.impl.FlowRunnerImpl;
import com.marklogic.hub.impl.HubConfigImpl;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
@Accessors(chain = true)
public class RunFlowWriteBatchListener extends LoggingObject implements WriteBatchListener {

	private String flowName;
	private List<String> steps;
	private boolean logResponse;
	private HubConfigImpl hubConfig;

	public RunFlowWriteBatchListener(String flowName, List<String> steps, HubConfigImpl hubConfig) {
		this.flowName = flowName;
		this.steps = steps;
		this.hubConfig = hubConfig;
	}

	@Override
	public void processEvent(WriteBatch batch) {
		FlowInputs inputs = buildFlowInputs(batch);

		FlowRunner flowRunner = new FlowRunnerImpl(hubConfig);
		RunFlowResponse response = flowRunner.runFlow(inputs);
		flowRunner.awaitCompletion();

		if (logResponse) {
			logger.info(format("Flow response for batch number %d:\n%s", batch.getJobBatchNumber(), response.toJson()));
		}
	}

	protected FlowInputs buildFlowInputs(WriteBatch batch) {
		FlowInputs inputs = new FlowInputs(flowName);
		if (steps != null) {
			inputs.setSteps(steps);
		}
		inputs.setJobId(batch.getBatcher().getJobId() + "-" + batch.getJobBatchNumber());

		Map<String, Object> options = new HashMap<>();
		options.put("sourceQuery", buildSourceQuery(batch));
		inputs.setOptions(options);

		return inputs;
	}

	protected String buildSourceQuery(WriteBatch batch) {
		StringBuilder sb = new StringBuilder("cts.documentQuery([");
		boolean firstOne = true;
		for (WriteEvent event : batch.getItems()) {
			if (!firstOne) {
				sb.append(",");
			}
			sb.append(String.format("'%s'", event.getTargetUri()));
			firstOne = false;
		}
		return sb.append("])").toString();
	}

	public boolean isLogResponse() {
		return logResponse;
	}
}
