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
package org.springframework.data.hadoop.impala.hive.commands;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.impala.hive.HiveConfiguration;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.stereotype.Component;

/**
 * Commands to submit and interact with MapReduce jobs
 * 
 *
 */
@Component
public class HiveCommand implements CommandMarker {

	private boolean initialized;

	@Autowired
	private HiveConfiguration hiveConfiguration;



	@CliCommand(value = "hive script", help = "run hive script")
	public void submit(@CliOption(key = { "scriptfile" }, mandatory = true, help = "the hive script file") final String scriptFile) {
		setupHiveClient();

	}


	private void setupHiveClient() {
		if (!initialized) {

			initialized = true;
		}
	}


}
