package com.marklogic.pulsar;

import java.util.HashMap;
import java.util.Map;

public final class TestHelper {

	public static final String MLHOST = "localhost";
	public static final int MLPORT = 8000;
	public static final String MLDB = "Documents";
	public static final String MLUSER = "admin";
	public static final String MLPWD = "admin";
	public static final String MLSECCONTEXT = "DIGEST";
	public static final Integer DMSDKBATCHSIZE = 1;
	public static final Integer DMSDKTHREADCOUNT = 1;

	public static Map<String, Object> createMap() {
		final Map<String, Object> map = new HashMap<>();
		map.put("mlConnectionHost", MLHOST);
		map.put("mlConnectionPort", MLPORT);
		map.put("mlDatabase", MLDB);
		map.put("mlUserName", MLUSER);
		map.put("mlPassword", MLPWD);
		map.put("mlSecurityContext", MLSECCONTEXT);
		map.put("dmsdkBatchSize", DMSDKBATCHSIZE);
		map.put("dmsdkThreadCount", DMSDKTHREADCOUNT);

		return map;
	}

	private TestHelper() {

	}
}
