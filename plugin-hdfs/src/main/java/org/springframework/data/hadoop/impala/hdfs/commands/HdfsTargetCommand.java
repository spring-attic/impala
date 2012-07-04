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
package org.springframework.data.hadoop.impala.hdfs.commands;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.impala.hdfs.HdfsConfiguration;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.stereotype.Component;

/**
 * Target command to set HDFS URL
 * 
 * @author Jarred Li
 *
 */
@Component
public class HdfsTargetCommand implements CommandMarker {

	@Autowired
	private HdfsConfiguration hdfsConfiguration;

	/**
	 * set HDFS URL
	 * 
	 * @param url HDFS url, for example, "hdfs://localhost:9000".
	 */
	@CliCommand(value = "hdfs target", help = "set HDFS URL")
	public void dfsName(@CliOption(key = { "url" }, mandatory = true, help = "HDFS URL") final String url) {
		hdfsConfiguration.setDfsName(url);
	}

	@CliCommand(value = "hdfs info", help = "show HDFS URL")
	public void showDfsName() {
		System.out.println("HDFS URL:" + hdfsConfiguration.getDfsName());
	}
}
