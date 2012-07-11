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
 * @author Jarred Li
 *
 */
@Component
public class HiveTargetCommand implements CommandMarker {

	@Autowired
	private HiveConfiguration hiveConfiguration;

	@CliCommand(value = "hive target", help = "set Hive server URL")
	public void setHiveServer(@CliOption(key = { "url" }, mandatory = true, help = "Hive server URL") final String url) {
		hiveConfiguration.setHiveServer(url);
	}

	@CliCommand(value = "hive info", help = "show Hive server URL")
	public void showHiveServer() {
		System.out.println("Hive server URL:" + hiveConfiguration.getHiveServer());
	}
}
