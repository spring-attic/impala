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

import org.springframework.roo.shell.CliCommand;
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

	/**
	 * Runs a jar file. Users can bundle their Map Reduce code in a jar file and execute it using this command. 
	 * 
	 */
	@CliCommand(value = "mr jar", help = "Runs a jar file. Users can bundle their Map Reduce code in a jar file and execute it using this command.")
	public void runMapReduceJar(
			) {

	}
	
	@CliCommand(value = "mr job", help = "Command to interact with Map Reduce Jobs.")
	public void interactWithJobs(
	    //Usage: hadoop job [GENERIC_OPTIONS] [-submit <job-file>] | [-status <job-id>] | [-counter <job-id> <group-name> <counter-name>] | [-kill <job-id>] | [-events <job-id> <from-event-#> <#-of-events>] | [-history [all] <jobOutputDir>] | [-list [all]] | [-kill-task <task-id>] | [-fail-task <task-id>] | [-set-priority <job-id> <priority>] 
            ) {

	}

}
