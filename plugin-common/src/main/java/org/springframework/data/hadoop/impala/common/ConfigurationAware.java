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

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.roo.shell.ParseResult;
import org.springframework.shell.ExecutionProcessor;

/**
 * Utility base class for components monitoring {@link Configuration} changes in order to update state. 
 * 
 * @author Costin Leau
 * @author Jarred Li
 */
public abstract class ConfigurationAware implements ApplicationListener<ConfigurationModifiedEvent>, ExecutionProcessor {

	@Autowired
	private Configuration hadoopConfiguration;

	private boolean needToReinitialize = false;

	@Override
	public void onApplicationEvent(ConfigurationModifiedEvent event) {
		needToReinitialize = true;
	}

	@Override
	public ParseResult beforeInvocation(ParseResult invocationContext) {
		// check whether the Hadoop configuration has changed
		if (needToReinitialize) {
			this.needToReinitialize = !configurationChanged();
		}
		return invocationContext;
	}

	/**
	 * Called before invoking a command in case the configuration changed.
	 * Should return true if the change has been acknowledged, false otherwise 
	 * 
	 * @return true if the change has been acknowledged, false otherwise.
	 */
	public abstract boolean configurationChanged();

	@Override
	public void afterReturningInvocation(ParseResult invocationContext, Object result) {
		// no-op
	}


	@Override
	public void afterThrowingInvocation(ParseResult invocationContext, Throwable thrown) {
		// no-op
	}

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the hadoopConfiguration
	 */
	public Configuration getHadoopConfiguration() {
		return hadoopConfiguration;
	}
}