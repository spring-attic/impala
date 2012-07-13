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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.data.hadoop.impala.common.ConfigurationHolder;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.stereotype.Component;

/**
 * Commands to submit and interact with MapReduce jobs
 * 
 * @author Jarred Li
 * @author Author of <code>org.apache.hadoop.util.RunJar</code>
 */
@Component
public class MapReduceCommands extends ConfigurationHolder implements CommandMarker {
	
	private JobClient jobClient;

	/* (non-Javadoc)
	 * @see org.springframework.data.hadoop.impala.common.ConfigurationHolder#init()
	 */
	@Override
	public boolean init() {
		boolean result = true;
		if(jobClient != null){
			
		}
		try {
			jobClient = new JobClient(new JobConf(getHadoopConfiguration()));
		} catch (IOException e) {
			System.err.println("init job client failed. Message:" + e.getMessage());
			result = false;
		}
		return result;
	}
	
	@CliCommand(value = "mr job submit", help = "submit Map Reduce Jobs.")
	public void submit(@CliOption(key = { "jobfile" }, mandatory = true, help = "the configuration file for MR job") final String jobFile) {
		List<String> argv = new ArrayList<String>();
		argv.add("-submit");
		argv.add(jobFile);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job status", help = "query Map Reduce status.")
	public void status(@CliOption(key = { "jobid" }, mandatory = true, help = "the job Id") final String jobid) {
		List<String> argv = new ArrayList<String>();
		argv.add("-status");
		argv.add(jobid);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job counter", help = "query job counter.")
	public void counter(@CliOption(key = { "jobid" }, mandatory = true, help = "the job Id") final String jobid, @CliOption(key = { "groupname" }, mandatory = true, help = "the job Id") final String groupName, @CliOption(key = { "countername" }, mandatory = true, help = "the job Id") final String counterName) {
		List<String> argv = new ArrayList<String>();
		argv.add("-counter");
		argv.add(jobid);
		argv.add(groupName);
		argv.add(counterName);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job kill", help = "kill Map Reduce job.")
	public void kill(@CliOption(key = { "jobid" }, mandatory = true, help = "the job Id") final String jobid) {
		List<String> argv = new ArrayList<String>();
		argv.add("-kill");
		argv.add(jobid);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job events", help = "query Map Reduce events.")
	public void events(@CliOption(key = { "jobid" }, mandatory = true, help = "the job Id") final String jobid, @CliOption(key = { "from" }, mandatory = true, help = "from event number") final String from, @CliOption(key = { "number" }, mandatory = true, help = "total number of events") final String number) {
		List<String> argv = new ArrayList<String>();
		argv.add("-events");
		argv.add(jobid);
		argv.add(from);
		argv.add(number);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job history", help = "list MapReduce Job history.")
	public void history(@CliOption(key = { "all" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "list all jobs") final boolean all, @CliOption(key = { "" }, mandatory = true, help = "output directory") final String outputDir) {
		List<String> argv = new ArrayList<String>();
		argv.add("-history");
		if (all) {
			argv.add("all");
		}
		argv.add(outputDir);
		run(argv.toArray(new String[0]));
	}

	@CliCommand(value = "mr job list", help = "list MapReduce Jobs.")
	public void list(@CliOption(key = { "all" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "list all jobs") final boolean all) {
		List<String> argv = new ArrayList<String>();
		argv.add("-list");
		if (all) {
			argv.add("all");
		}
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = "mr task kill", help = "kill Map Reduce task.")
	public void killTask(@CliOption(key = { "taskid" }, mandatory = true, help = "the task Id") final String taskid) {
		List<String> argv = new ArrayList<String>();
		argv.add("-kill-task");
		argv.add(taskid);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "mr task fail", help = "fail Map Reduce task.")
	public void failTask(@CliOption(key = { "taskid" }, mandatory = true, help = "the task Id") final String taskid) {
		List<String> argv = new ArrayList<String>();
		argv.add("-fail-task");
		argv.add(taskid);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "mr job set priority", help = "Changes the priority of the job.")
	public void setPriority(@CliOption(key = { "jobid" }, mandatory = true, help = "the job Id") final String jobid,
			@CliOption(key = { "priority" }, mandatory = true, help = "the job priority") final JobPriority priority) {
		List<String> argv = new ArrayList<String>();
		argv.add("-set-priority");
		argv.add(jobid);
		argv.add(priority.getValue());
		run(argv.toArray(new String[0]));
	}
	
	public enum JobPriority{
		VERY_HIGH("VERY_HIGH"),
		HIGH("HIGH"),
		NORML("NORMAL"),
		LOW("LOW"),
		VERY_LOW("VERY_LOW");
		
		private String val;
		
		JobPriority(String v){
			this.val = v;
		}
		
		public String getValue(){
			return val;
		}
	}

	@CliCommand(value = "mr jar", help = "run Map Reduce Job in the jar")
	public void jar(@CliOption(key = { "jarfile" }, mandatory = true, help = "jar file name") final String jarFileName, 
			@CliOption(key = "mainclass", mandatory = true, help = "main class name") final String mainClassName,
			@CliOption(key = "args", mandatory = false, help = "input path") final String args) {
		File file = new File(jarFileName);
		File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
		tmpDir.mkdirs();
		if (!tmpDir.isDirectory()) {
			System.err.println("Mkdirs failed to create " + tmpDir);
		}

		try {
			final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
			workDir.delete();
			workDir.mkdirs();
			if (!workDir.isDirectory()) {
				System.err.println("Mkdirs failed to create " + workDir);
				System.exit(-1);
			}

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						FileUtil.fullyDelete(workDir);
					} catch (IOException e) {
					}
				}
			});

			unJar(file, workDir);

			ArrayList<URL> classPath = new ArrayList<URL>();
			
			//This is to add hadoop configuration dir to classpath so that 
			//user's configuration can be accessed when running the jar
			File hadoopConfigurationDir = new File(workDir + Path.SEPARATOR +"impala-hadoop-configuration");
			writeHadoopConfiguration(hadoopConfigurationDir,this.getHadoopConfiguration());
			classPath.add(hadoopConfigurationDir.toURL());
			//classPath.add(new File(System.getenv("HADOOP_CONF_DIR")).toURL());

			classPath.add(new File(workDir + Path.SEPARATOR).toURL());
			classPath.add(file.toURL());
			classPath.add(new File(workDir, "classes" + Path.SEPARATOR).toURL());
			File[] libs = new File(workDir, "lib").listFiles();
			if (libs != null) {
				for (int i = 0; i < libs.length; i++) {
					classPath.add(libs[i].toURL());
				}
			}
			ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]), this.getClass().getClassLoader());
			Thread.currentThread().setContextClassLoader(loader);
			Class<?> mainClass = Class.forName(mainClassName, true, loader);
			Method main = mainClass.getMethod("main", new Class[] { Array.newInstance(String.class, 0).getClass() });
			String[] newArgs = args.split(" ");
			main.invoke(null, new Object[] { newArgs });
		} catch (Exception e) {
			System.err.println("failed to run MR job. Failed Message:" + e.getMessage());
			//e.printStackTrace();
		}
	}

	/**
	 * wirte the Hadoop configuration to one directory, 
	 * file name is "core-site.xml", "hdfs-site.xml" and "mapred-site.xml".
	 * 
	 * @param configDir the directory that the file be written
	 * @param config Hadoop configuration
	 * 
	 */
	public void writeHadoopConfiguration(File configDir,Configuration config) {
		configDir.mkdirs();
		try {
			FileOutputStream fos = new FileOutputStream(new File(configDir + Path.SEPARATOR + "core-site.xml"));
			config.writeXml(fos);
			fos = new FileOutputStream(new File(configDir + Path.SEPARATOR + "hdfs-site.xml"));
			config.writeXml(fos);
			fos = new FileOutputStream(new File(configDir + Path.SEPARATOR + "mapred-site.xml"));
			config.writeXml(fos);
		} catch (Exception e) {
			System.err.println("Save user's configuration failed. Message:" + e.getMessage());
		}
		
	}

	private void unJar(File jarFile, File toDir) throws IOException {
		JarFile jar = new JarFile(jarFile);
		try {
			Enumeration entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = (JarEntry) entries.nextElement();
				if (!entry.isDirectory()) {
					InputStream in = jar.getInputStream(entry);
					try {
						File file = new File(toDir, entry.getName());
						if (!file.getParentFile().mkdirs()) {
							if (!file.getParentFile().isDirectory()) {
								throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
							}
						}
						OutputStream out = new FileOutputStream(file);
						try {
							byte[] buffer = new byte[8192];
							int i;
							while ((i = in.read(buffer)) != -1) {
								out.write(buffer, 0, i);
							}
						} finally {
							out.close();
						}
					} finally {
						in.close();
					}
				}
			}
		} finally {
			jar.close();
		}
	}

	private void run(String[] argv) {
		try {
			jobClient.run(argv);
		} catch (Exception e) {
			System.err.println("run MR job failed. Failed Message:" + e.getMessage());
		}
	}


}
