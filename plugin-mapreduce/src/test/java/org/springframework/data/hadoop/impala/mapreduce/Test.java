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

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Jarred Li
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.setStrings("mapred.job.tracker", "localhost:9001");
		System.out.println("configuration:" + config.toString());
		System.out.println("hdfs:" + config.get("fs.default.name"));
		PrintWriter pw = new PrintWriter(System.out);
		try {
			Configuration.dumpConfiguration(config, pw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
