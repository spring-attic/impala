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
package org.springframework.data.hadoop.impala.mapreduce.commands;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.impala.mapreduce.JobConfiguration;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.stereotype.Component;

/**
 * @author Jarred Li
 *
 */
@Component
public class JobTargetCommand implements CommandMarker {

	@Autowired
	private JobConfiguration jobConfiguration;

	@CliCommand(value = "mr target", help = "set Job Tracker URL")
	public void dfsName(@CliOption(key = { "url" }, mandatory = true, help = "Job Tracker URL") final String url) {
		jobConfiguration.setJobTracker(url);
	}

	@CliCommand(value = "mr info", help = "show Job Tracker URL")
	public void showDfsName() {
		System.out.println("Job Tracker URL:" + jobConfiguration.getJobTracker());
	}
}
