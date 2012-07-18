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
package org.springframework.data.hadoop.impala.pig;

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.hadoop.pig.PigContextFactoryBean;
import org.springframework.data.hadoop.pig.PigServerFactoryBean;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Provider of pig commands.
 * 
 * @author Costin Leau
 */
@Component
public class PigCommands implements ApplicationContextAware, CommandMarker {

	private static final String PREFIX = "pig ";

	private final Logger LOG = Logger.getLogger(getClass().getName());

	@Autowired
	private Configuration hadoopConfiguration;

	private PigContextFactoryBean pigContextFactory;
	private PigServerFactoryBean pigFactory;

	private ExecType execType;
	private String jobTracker, jobName, jobPriority;
	private Boolean validateEachStatement;
	private String propertiesLocation;

	private ResourcePatternResolver resourceResolver;

	@PostConstruct
	public void init() throws Exception {
		pigContextFactory = new PigContextFactoryBean();
		pigContextFactory.setConfiguration(hadoopConfiguration);

		if (StringUtils.hasText(jobTracker)) {
			pigContextFactory.setJobTracker(jobTracker);
		}

		if (execType != null) {
			pigContextFactory.setExecType(execType);
		}
		Properties props = loadProperties();
		if (props != null) {
			pigContextFactory.setProperties(props);
		}
		pigContextFactory.afterPropertiesSet();

		pigFactory = new PigServerFactoryBean();
		pigFactory.setPigContext(pigContextFactory.getObject());
		if (validateEachStatement != null) {
			pigFactory.setValidateEachStatement(validateEachStatement);
		}
		if (StringUtils.hasText(jobName)) {
			pigFactory.setJobName(jobName);
		}
		if (StringUtils.hasText(jobPriority)) {
			pigFactory.setJobPriority(jobPriority);
		}
	}

	private Properties loadProperties() throws Exception {
		if (StringUtils.hasText(propertiesLocation)) {
			PropertiesFactoryBean propsFactory = new PropertiesFactoryBean();
			propsFactory.setLocations(resourceResolver.getResources(propertiesLocation));
			propsFactory.afterPropertiesSet();
			return propsFactory.getObject();
		}

		return null;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.resourceResolver = applicationContext;
	}

	@CliCommand(value = { PREFIX + "cfg" }, help = "Configures Pig")
	public void config(@CliOption(key = { "props" }, help = "Properties file location") String location,
			@CliOption(key = { "jobTracker" }, mandatory = false, help = "Job tracker") String jobTracker,
			@CliOption(key = { "execType" }, mandatory = false, help = "Execution type") ExecType execType,
			@CliOption(key = { "jobName" }, mandatory = false, help = "Job name") String jobName, 
			@CliOption(key = { "jobPriority" }, mandatory = false, help = "Job priority") String jobPriority, 
			@CliOption(key = { "validateEachStatement" }, mandatory = false, help = "Validation of each statement") Boolean validateEachStatement)
			throws Exception {

		this.jobTracker = jobTracker;
		this.jobName = jobName;
		this.jobPriority = jobPriority;
		this.validateEachStatement = validateEachStatement;
		this.execType = execType;
		this.propertiesLocation = location;
	}

	@CliCommand(value = { PREFIX + "info" }, help = "Returns basic info about the Pig configuration")
	public String info() {
		StringBuilder sb = new StringBuilder();
		sb.append("Pig [");
		String pigVersion = PigServer.class.getPackage().getImplementationVersion();
		sb.append((StringUtils.hasText(pigVersion) ? pigVersion : "unknown"));
		sb.append("][fs=");
		sb.append(FileSystem.getDefaultUri(hadoopConfiguration));
		sb.append("][jt=");
		sb.append((StringUtils.hasText(jobTracker) ? jobTracker : hadoopConfiguration.get("mapred.job.tracker")));
		sb.append("][execType=");
		sb.append((execType != null ? execType.name() : ExecType.MAPREDUCE.name()));
		sb.append("]");
		// TODO: potentially add a check to see whether HDFS is running

		return sb.toString();
	}

	@CliCommand(value = { PREFIX + "script" }, help = "Executes a Pig script")
	public String script(@CliOption(key = { "", "location" }, mandatory = true, help = "Script location") String location) {
		if(location.startsWith("/")){
			location = "file://"+ location;
		}
		Resource resource = resourceResolver.getResource(location);
		if(!resource.exists()){
			LOG.severe("No resource found at " + location);
		}
		PigServer pig = null;
		try {
			// for each run, start a new Pig instance
			init();

			pig = pigFactory.getObject();

			pig.setBatchOn();
			pig.getPigContext().connect();

			// register scripts
			InputStream in = null;
			try {
				in = resource.getInputStream();
				pig.registerScript(in);
			} finally {
				IOUtils.closeStream(in);
			}

			ExecJob result = pig.executeBatch().get(0);
			Exception exception = result.getException();
			StringBuilder sb = new StringBuilder(result.getStatus().name());
			if (exception != null) {
				sb.append(" ;Cause=");
				sb.append(exception.getMessage());
			}
			return sb.toString();
		} catch (Throwable t) {
			LOG.severe("Run pig script failed. Failed message:" + t.getMessage());
		} finally {
			if (pig != null)
				pig.shutdown();
		}
		return "";
	}
}