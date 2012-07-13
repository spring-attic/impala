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
package org.springframework.data.hadoop.impala.mapreduce;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jarred Li
 *
 */
public class MapReduceCommandsTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.mapreduce.MapReduceCommands#writeHadoopConfiguration(java.io.File, org.apache.hadoop.conf.Configuration)}.
	 */
	@Test
	public void testWriteHadoopConfiguration() {
		MapReduceCommands cmd = new MapReduceCommands();
		cmd.writeHadoopConfiguration(new File("/tmp/impala"), new Configuration());
	}

}
