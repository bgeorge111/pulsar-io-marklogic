package com.marklogic.pulsar;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class MarkLogicConfigTest {

	private static File getFile(String fileName) {
		return new File(MarkLogicConfigTest.class.getClassLoader().getResource(fileName).getFile());
	}

	@Test
	public void testMap() throws IOException {
		final Map<String, Object> map = TestHelper.createMap();
		final MarkLogicConfig cfg = MarkLogicConfig.load(map);
		assertEquals(cfg.getMlConnectionHost(), TestHelper.MLHOST);
		assertEquals(cfg.getMlDatabase(), TestHelper.MLDB);
		assertEquals(cfg.getMlConnectionPort(), TestHelper.MLPORT);
		assertEquals(cfg.getDmsdkBatchSize(), TestHelper.DMSDKBATCHSIZE);
		assertEquals(cfg.getDmsdkThreadCount(), TestHelper.DMSDKTHREADCOUNT);
		assertEquals(cfg.getMlUserName(), TestHelper.MLUSER);
		assertEquals(cfg.getMlPassword(), TestHelper.MLPWD);
	}

	@Test
	public void testYaml() throws IOException {
		final File yaml = getFile("marklogicSinkConfig.json");
		final MarkLogicConfig cfg = MarkLogicConfig.load(yaml.getAbsolutePath());

		assertEquals(cfg.getMlConnectionHost(), TestHelper.MLHOST);
		assertEquals(cfg.getMlDatabase(), TestHelper.MLDB);
		assertEquals(cfg.getMlConnectionPort(), TestHelper.MLPORT);
		assertEquals(cfg.getDmsdkBatchSize(), TestHelper.DMSDKBATCHSIZE);
		assertEquals(cfg.getDmsdkThreadCount(), TestHelper.DMSDKTHREADCOUNT);
		assertEquals(cfg.getMlUserName(), TestHelper.MLUSER);
		assertEquals(cfg.getMlPassword(), TestHelper.MLPWD);

	}
}