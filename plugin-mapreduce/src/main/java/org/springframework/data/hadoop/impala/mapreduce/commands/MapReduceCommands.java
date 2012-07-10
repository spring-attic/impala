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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.RunJar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.impala.mapreduce.JobConfiguration;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.stereotype.Component;

/**
 * Commands to submit and interact with MapReduce jobs
 * 
 * @author Jarred Li
 *
 */
@Component
public class MapReduceCommands implements CommandMarker {

	private boolean initialized;

	@Autowired
	private JobConfiguration jobConfiguration;

	private JobClient jobClient;


	//Usage: hadoop job [GENERIC_OPTIONS] [-submit <job-file>] | [-status <job-id>] 
	//| [-counter <job-id> <group-name> <counter-name>] | [-kill <job-id>] 
	//| [-events <job-id> <from-event-#> <#-of-events>] | [-history [all] <jobOutputDir>] 
	//| [-list [all]] | [-kill-task <task-id>] | [-fail-task <task-id>] | [-set-priority <job-id> <priority>] 
	@CliCommand(value = "mr job list", help = "list MapReduce Jobs.")
	public void listJobs(@CliOption(key = { "all" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "list all jobs") final boolean all) {
		setupJobClient();
		JobStatus[] jobs = null;

		try {
			if (all) {
				jobs = jobClient.getAllJobs();
			}
			else {
				jobs = jobClient.jobsToComplete();
			}
		} catch (IOException e) {
			System.err.println("get job information failed." + e.getMessage());
		}
		if (jobs == null)
			jobs = new JobStatus[0];
		if (all) {
			System.out.printf("%d jobs submitted\n", jobs.length);
			System.out.printf("States are:\n\tRunning : 1\tSucceded : 2" + "\tFailed : 3\tPrep : 4\n");
		}
		else {
			System.out.printf("%d jobs currently running\n", jobs.length);
		}
		displayJobList(jobs);
	}

	@CliCommand(value = "mr jar", help = "run Map Reduce Job in the jar")
	public void jar(
			@CliOption(key = { "jarfile" }, mandatory = true, help = "jar file name") final String jarFile,
			@CliOption(key="mainclass",mandatory = false, help = "main class name") final String mainClass,
			@CliOption(key="args",mandatory = false, help = "input path") final String args
			) {
		List<String> argv = new ArrayList<String>();
		argv.add(jarFile);
		if(mainClass != null){
			argv.add(mainClass);
		}
		String[] params = args.split(" ");
		argv.addAll(Arrays.asList(params));
		try {
			RunJar.main(argv.toArray(new String[0]));
		} catch (Throwable e) {
			System.err.println("Run jar failed." + e.getMessage());
		}
	}
	
	@CliCommand(value = "mr job submit", help = "submit Map Reduce Jobs.")
	public void submit(@CliOption(key = { "all" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "list all jobs") final boolean all) {
		setupJobClient();
		
	}
	
	private void displayJobList(JobStatus[] jobs) {
		System.out.printf("JobId\tState\tStartTime\tUserName\tPriority\tSchedulingInfo\n");
		for (JobStatus job : jobs) {
			System.out.printf("%s\t%d\t%d\t%s\t%s\t%s\n", job.getJobID(), job.getRunState(), job.getStartTime(),
					job.getUsername(), job.getJobPriority().name(), job.getSchedulingInfo());
		}
	}

	private void setupJobClient() {
		if (!initialized) {
			Configuration config = new Configuration();
			config.setStrings("mapred.job.tracker", jobConfiguration.getJobTracker());
			try {
				jobClient = new JobClient(new JobConf(config));
			} catch (IOException e) {
				System.err.println("Init job client failed." + e.getMessage());
			}
			initialized = true;
		}
	}

}
