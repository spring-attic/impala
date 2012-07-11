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
package org.springframework.data.hadoop.impala.hdfs;

import java.util.HashMap;
import java.util.Map;


public class HdfsConfiguration {

	private static final String DFS_KEY = "fs.default.name";
	
	private static final String DEFAULT_DFS_NAME = "webhdfs://localhost:50070";
	
	private Map<String, String> props = new HashMap<String, String>();
	
	public void setDfsName(String dfsName) {
		props.put(DFS_KEY, dfsName);

	}

	public String getDfsName() {
		if (props.containsKey(DFS_KEY)){
			return props.get(DFS_KEY);
		}
		return DEFAULT_DFS_NAME;
		
	}
}
