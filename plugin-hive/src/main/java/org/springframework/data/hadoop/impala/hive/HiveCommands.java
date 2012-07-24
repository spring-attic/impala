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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.core.io.FileSystemResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.hadoop.hive.HiveClientFactoryBean;
import org.springframework.data.hadoop.hive.HiveScriptRunner;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.util.StringUtils;
import org.springframework.stereotype.Component;

/**
 * Provider of Hive commands.
 * 
 * @author Costin Leau
 */
@Component
public class HiveCommands implements CommandMarker {

	private static final String PREFIX = "hive ";

	private String host = "localhost";
	private Integer port = 10000;
	private Long timeout = TimeUnit.MINUTES.toMillis(2);

	private HiveClientFactoryBean hiveClientFactory;

	private ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver(
			new FileSystemResourceLoader());

	@PostConstruct
	public void init() throws Exception {
		hiveClientFactory = new HiveClientFactoryBean();

		hiveClientFactory.setAutoStartup(false);
		hiveClientFactory.setHost(host);
		hiveClientFactory.setPort(port);
		hiveClientFactory.setTimeout(timeout.intValue());
	}

	private String info() {
		StringBuilder sb = new StringBuilder();
		sb.append("Hive [");
		String hiveVersion = HiveConf.class.getPackage().getImplementationVersion();
		sb.append((StringUtils.hasText(hiveVersion) ? hiveVersion : "unknown"));
		sb.append("][host=");
		sb.append(host);
		sb.append("][port=");
		sb.append(port);
		sb.append("]");
		// TODO: potentially add a check to see whether HDFS is running

		return sb.toString();
	}

	@CliCommand(value = { PREFIX + "cfg" }, help = "Configures Hive")
	public String config(@CliOption(key = { "host" }, mandatory = false, help = "Server host") String host,
			@CliOption(key = { "port" }, mandatory = false, help = "Server port") Integer port,
			@CliOption(key = { "timeout" }, mandatory = false, help = "Connection Timeout") Long timeout)
			throws Exception {
		
		if (StringUtils.hasText(host)) {
			this.host = host;
		}
		if (port != null) {
			this.port = port;
		}
		if (timeout != null) {
			this.timeout = timeout;
		}

		return info();
	}

	@CliCommand(value = { PREFIX + "script" }, help = "Executes a Hive script")
	public String script(@CliOption(key = { "", "location" }, mandatory = true, help = "Script location") String location) {

		Resource resource = resourceResolver.getResource(fixLocation(location));
		if (!resource.exists()) {
			return "No resource found at " + location;
		}

		String uri = location;

		try {
			uri = resource.getURI().toString();
		} catch (IOException ex) {
			// ignore - we'll use location
		}

		StringBuilder sb = new StringBuilder();
		try {
			// for each run, start a new Hive instance
			init();

			hiveClientFactory.afterPropertiesSet();
			hiveClientFactory.start();
			HiveClient client = hiveClientFactory.getObject();
			List<String> results = HiveScriptRunner.run(client, resource);
			sb.append(StringUtils.collectionToDelimitedString(results, StringUtils.LINE_SEPARATOR));
		} catch (Exception ex) {
			return "Script [" + uri + "] failed - " + ex;
		} finally {
			hiveClientFactory.destroy();
		}

		return sb.append(StringUtils.LINE_SEPARATOR).append("Script [" + uri + "] executed succesfully").toString();
	}

	private static String fixLocation(String location) {
		if (StringUtils.hasText(location) && !location.contains(":")) {
			return "file:" + location;
		}
		return location;
	}
}