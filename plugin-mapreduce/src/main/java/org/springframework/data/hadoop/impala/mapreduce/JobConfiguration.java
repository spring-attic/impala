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
package org.springframework.data.hadoop.impala.mapreduce;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jarred Li
 *
 */
public class JobConfiguration {

	private static final String JOB_TRACKER_KEY = "mapred.job.tracker";

	private static final String DEFAULT_JOB_TRACKER = "localhost:9001";

	private Map<String, String> props = new HashMap<String, String>();

	public void setJobTracker(String jobTracker) {
		props.put(JOB_TRACKER_KEY, jobTracker);

	}

	public String getJobTracker() {
		if (props.containsKey(JOB_TRACKER_KEY)) {
			return props.get(JOB_TRACKER_KEY);
		}
		return DEFAULT_JOB_TRACKER;

	}

}
