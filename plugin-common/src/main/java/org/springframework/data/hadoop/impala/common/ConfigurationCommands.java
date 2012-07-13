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
package org.springframework.data.hadoop.impala.common;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.roo.shell.ParseResult;
import org.springframework.shell.ExecutionProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * Command for configuring the global Hadoop {@link Configuration} object used by other components.
 * Modeled after {@link GenericOptionsParser} (for usability reasons).
 * 
 * @author Costin Leau
 */
@Component
public class ConfigurationCommands implements ApplicationEventPublisherAware, CommandMarker, ExecutionProcessor {

	private static Logger log = Logger.getLogger(ConfigurationCommands.class.getName());
	private static final String PREFIX = "cfg ";

	@Autowired
	private Configuration hadoopConfiguration;
	private ApplicationEventPublisher applicationEventPublisher;

	@CliCommand(value = { PREFIX + "load" }, help = "Loads the Hadoop configuration from the given resource")
	public void loadConfiguration(@CliOption(key = { "", "location" }, mandatory = true, help = "Configuration location (can be a URL)") String location) {
		hadoopConfiguration.addResource(location);
		hadoopConfiguration.size();
	}

	@CliCommand(value = { PREFIX + "props set" }, help = "Sets the value for the given Hadoop property - <name=value>")
	public void setProperty(@CliOption(key = { "", "property" }, mandatory = true, help = "<name=value>") String property) {
		int i = property.indexOf("=");
		Assert.isTrue(i >= 0, "invalid format");
		String name = property.substring(0, i);
		Assert.hasText(name, "a valid name is required");
		String value = property.substring(i + 1);

		hadoopConfiguration.set(name, value);
	}

	@CliCommand(value = { PREFIX + "props get" }, help = "Returns the value of the given Hadoop property")
	public String getProperty(@CliOption(key = { "", "key" }, mandatory = true, help = "Property name") String name) {
		return hadoopConfiguration.get(name);
	}

	@CliCommand(value = { PREFIX + "props list" }, help = "Returns (all) the Hadoop properties")
	public String listProps() {
		return ConfigurationUtils.asProperties(hadoopConfiguration).toString();
	}

	@CliCommand(value = { PREFIX + "fs" }, help = "Sets the Hadoop namenode - can be 'local' or <namenode:port>")
	public void setFs(@CliOption(key = { "", "namenode" }, mandatory = true, help = "Namenode address - local|<namenode:port>") String namenode) {
		FileSystem.setDefaultUri(hadoopConfiguration, namenode);
	}

	@CliCommand(value = { PREFIX + "jt" }, help = "Sets the Hadoop job tracker - can be 'local' or <jobtracker:port>")
	public void setJt(@CliOption(key = { "", "jobtracker" }, mandatory = true, help = "Job tracker address - local|<jobtracker:port>") String jobtracker) {
		hadoopConfiguration.set("mapred.job.tracker", jobtracker);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public ParseResult beforeInvocation(ParseResult invocationContext) {
		return invocationContext;
	}

	@Override
	public void afterReturningInvocation(ParseResult invocationContext, Object result) {
		String name = invocationContext.getMethod().getName();
		if (name.startsWith("load") || name.startsWith("set")) {
			publishChange();
		}
	}

	@Override
	public void afterThrowingInvocation(ParseResult invocationContext, Throwable thrown) {
		// no-op
	}

	private void publishChange() {
		applicationEventPublisher.publishEvent(new ConfigurationModifiedEvent(hadoopConfiguration));
	}
}