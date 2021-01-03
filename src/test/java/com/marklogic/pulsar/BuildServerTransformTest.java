package com.marklogic.pulsar;

import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.marklogic.client.document.ServerTransform;
import com.marklogic.pulsar.config.MarkLogicAbstractConfig;


public class BuildServerTransformTest {

	private MarkLogicSink task = new MarkLogicSink();
	

	@Test
	public void noTransform() {
		MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();
		assertNull(task.buildServerTransform(config));
	}

	@Test
	public void noParams() {
		MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();
		config.setDmsdkTransform("noParams");
		ServerTransform t = task.buildServerTransform(config);
		assertEquals("noParams", t.getName());
	}

	@Test
	public void oneParam() {
		MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();
		config.setDmsdkTransform("oneParam");
		config.setDmsdkTransformParams("param1,value1");
		config.setDmsdkTransformParamDelimiter(",");
		ServerTransform t = task.buildServerTransform(config);
		assertEquals(1, t.keySet().size());
		assertEquals("value1", t.get("param1").get(0));
	}

	@Test
	public void twoParamsWithCustomDelimiter() {
		MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();
		config.setDmsdkTransform("twoParams");
		config.setDmsdkTransformParams("param1;value1;param2;value2");
		config.setDmsdkTransformParamDelimiter(";");
		ServerTransform t = task.buildServerTransform(config);
		assertEquals(2, t.keySet().size());
		assertEquals("value1", t.get("param1").get(0));
		assertEquals("value2", t.get("param2").get(0));
	}

	@Test
	public void malformedParams() {
		MarkLogicAbstractConfig config = new MarkLogicAbstractConfig();
		config.setDmsdkTransform("malformedParams");
		config.setDmsdkTransformParams("param1,value1,param2");
		config.setDmsdkTransformParamDelimiter(",");
		try {
			task.buildServerTransform(config);
			fail("The call should have failed because the params property does not have an even number of parameter " +
				"names and values");
		} catch (IllegalArgumentException ex) {
			assertTrue(ex.getMessage().startsWith("The value of the param1,value1,param2 property"));
		}
	}
}
