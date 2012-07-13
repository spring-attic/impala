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
package org.springframework.data.hadoop.impala.mapreduce.config;

import java.io.FileOutputStream;
import java.util.List;

import javax.xml.transform.stream.StreamResult;

import org.springframework.oxm.Marshaller;
import org.springframework.oxm.castor.CastorMarshaller;

/**
 * @author Jarred Li
 *
 */
public class Configuration {

	private Marshaller marshaller;
	
	private List<Property> properties;

	public Configuration(org.apache.hadoop.conf.Configuration config){
		marshaller = new CastorMarshaller();
	}
	
	/**
	 * @return the properties
	 */
	public List<Property> getProperties() {
		return properties;
	}

	/**
	 * @param properties the properties to set
	 */
	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}
	
	public void save(String fileName){
		FileOutputStream fos = null;
		try{
			fos = new FileOutputStream(fileName);
			StreamResult xmlResult = new StreamResult(fos);
			marshaller.marshal(this, xmlResult);
		}
		catch(Exception e){
			System.err.println("Save Hadoop configuration failed. Message:" + e.getMessage());
		}
	}
	
}
