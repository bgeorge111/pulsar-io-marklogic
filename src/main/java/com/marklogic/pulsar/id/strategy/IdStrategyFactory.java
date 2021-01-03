package com.marklogic.pulsar.id.strategy;

import java.util.Map;

import com.marklogic.pulsar.config.MarkLogicSinkConfig;

public  class IdStrategyFactory {
	
	public static IdStrategy getIdStrategy(MarkLogicSinkConfig config) {
		String strategyType = (String) config.getMlIdStrategyForURI();
		String strategyPaths= (String) config.getMlIdStrategyPath();
		
		switch((strategyType != null) ? strategyType : "UUID") {
			case "JSONPATH":
				return (new JSONPathStrategy(strategyPaths.trim().split(",")[0]));
			case "HASH":
				return (new HashedJSONPathsStrategy(strategyPaths.trim().split(",")));
			case "UUID":
				return (new DefaultStrategy());
			case "PULSAR_META_WITH_SLASH":
				return (new PulsarMetaStrategy());
			case "PULSAR_META_HASHED":
				return (new HashedPulsarMetaStrategy());
			default: 
				return (new DefaultStrategy());
		}
	}
	
} 
