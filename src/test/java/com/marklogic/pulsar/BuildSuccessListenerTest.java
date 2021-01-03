package com.marklogic.pulsar;

import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.Test;

import com.marklogic.hub.impl.HubConfigImpl;
import com.marklogic.pulsar.config.MarkLogicSinkConfig;

public class BuildSuccessListenerTest {

	private MarkLogicSink task = new MarkLogicSink();
	
	@Test
	void multipleSteps() {
		MarkLogicSinkConfig config = new MarkLogicSinkConfig();
		config.setDhfFlowSteps("1,2");
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new HubConfigImpl());

		assertEquals("myFlow", listener.getFlowName());
		List<String> steps = listener.getSteps();
		assertEquals(2, steps.size());
		assertEquals("1", steps.get(0));
		assertEquals("2", steps.get(1));
	}

	@Test
	void emptySteps() {
		MarkLogicSinkConfig config = new MarkLogicSinkConfig();
		config.setDhfFlowSteps(" ");
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new HubConfigImpl());
		assertNull(listener.getSteps());
	}

	@Test
	void noSteps() {
		MarkLogicSinkConfig config = new MarkLogicSinkConfig();
		RunFlowWriteBatchListener listener = task.buildSuccessListener("myFlow", config, new HubConfigImpl());
		assertNull(listener.getSteps());
	}
}
