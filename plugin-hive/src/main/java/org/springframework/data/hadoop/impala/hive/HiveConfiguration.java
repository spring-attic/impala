/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.impala.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jarred Li
 *
 */
public class HiveConfiguration {

	private static final String HIVE_SERVER_KEY = "hive.server";

	private static final String DEFAULT_HIVE_SERVER = "localhost:10000";

	private Map<String, String> props = new HashMap<String, String>();

	public void setHiveServer(String hiveServer) {
		props.put(HIVE_SERVER_KEY, hiveServer);
	}

	public String getHiveServer() {
		if (props.containsKey(HIVE_SERVER_KEY)) {
			return props.get(HIVE_SERVER_KEY);
		}
		return DEFAULT_HIVE_SERVER;

	}

}
