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
package org.springframework.data.hadoop.impala.hdfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.springframework.data.hadoop.impala.common.ConfigurationAware;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * HDFS shell commands
 * 
 * @author Jarred Li
 *
 */
@Component
public class FsShellCommands extends ConfigurationAware {

	private static final String PREFIX = "fs ";

	private FsShell shell;

	@PostConstruct
	public void init() {
		shell = new FsShell(getHadoopConfiguration());
	}

	@Override
	protected String failedComponentName() {
		return "shell";
	}

	@Override
	protected boolean configurationChanged() {
		if (shell != null) {
			LOG.info("Hadoop configuration changed, re-initializing shell...");
		}
		init();
		return true;
	}

	@CliCommand(value = PREFIX + "ls", help = "list files in HDFS")
	public void ls(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive) {
		if (recursive) {
			runCommand("-lsr", path);
		}
		else {
			runCommand("-ls", path);
		}
	}


	@CliCommand(value = PREFIX + "cat", help = "show file content")
	public void cat(@CliOption(key = { "" }, mandatory = true, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "file name to be showed") final String path) {
		runCommand("-cat", path);
	}

	@CliCommand(value = PREFIX + "chgrp", help = "change file group")
	public void chgrp(@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "group" }, mandatory = true, help = "group name") final String group,
			@CliOption(key = { "" }, mandatory = true, help = "file name to be changed group") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-chgrp");
		if (recursive) {
			argv.add("-R");
		}
		argv.add(group);
		String[] fileNames = path.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "chown", help = "change file ownership")
	public void chown(
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "owner" }, mandatory = true, help = "owner name") final String owner,
			@CliOption(key = { "" }, mandatory = true, help = "file name to be changed group") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-chown");
		if (recursive) {
			argv.add("-R");
		}
		argv.add(owner);
		String[] fileNames = path.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "chmod", help = "change file permissions")
	public void chmod(
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "mode" }, mandatory = true, help = "permission mode") final String mode,
			@CliOption(key = { "" }, mandatory = true, help = "file name to be changed permissions") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-chmod");
		if (recursive) {
			argv.add("-R");
		}
		argv.add(mode);
		String[] fileNames = path.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "copyFromLocal", help = "copy local files to HDFS")
	public void copyFromLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		List<String> argv = new ArrayList<String>();
		argv.add("-copyFromLocal");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "put", help = "copy local files to HDFS")
	public void put(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		List<String> argv = new ArrayList<String>();
		argv.add("-put");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "moveFromLocal", help = "move local files to HDFS")
	public void moveFromLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		List<String> argv = new ArrayList<String>();
		argv.add("-moveFromLocal");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = PREFIX + "copyToLocal", help = "copy HDFS files to local")
	public void copyToLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "ignoreCrc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether copy CRC") final boolean crc) {
		List<String> argv = new ArrayList<String>();
		argv.add("-copyToLocal");
		if(ignoreCrc){
			argv.add("-ignoreCrc");
		}
		if(crc){
			argv.add("-crc");
		}
		argv.add(source);
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "get", help = "copy HDFS files to local")
	public void get(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "ignoreCrc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether copy CRC") final boolean crc) {
		List<String> argv = new ArrayList<String>();
		argv.add("-get");
		if(ignoreCrc){
			argv.add("-ignoreCrc");
		}
		if(crc){
			argv.add("-crc");
		}
		argv.add(source);
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = PREFIX + "count", help = "Count the number of directories, files, bytes, quota, and remaining quota")
	public void count(
			@CliOption(key = { "quota" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean quota,
			@CliOption(key = { "path" }, mandatory = true, help = " path name") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-count");
		if(quota){
			argv.add("-q");
		}
		String[] fileNames = path.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "cp", help = "copy files in the HDFS")
	public void cp(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		List<String> argv = new ArrayList<String>();
		argv.add("-cp");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "mv", help = "move files in the HDFS")
	public void mv(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		List<String> argv = new ArrayList<String>();
		argv.add("-mv");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "du", help = "display sizes of file")
	public void du(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "summary" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with summary") final boolean summary) {
		List<String> argv = new ArrayList<String>();
		if(summary){
			argv.add("-dus");
		}
		else{
			argv.add("-du");
		}
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "expunge", help = "empty the trash")
	public void expunge() {
		List<String> argv = new ArrayList<String>();
		argv.add("-expunge");
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = PREFIX + "mkdir", help = "create new directory")
	public void mkdir(
			@CliOption(key = { "" }, mandatory = true, help = "directory name") final String dir) {
		List<String> argv = new ArrayList<String>();
		argv.add("-mkdir");
		argv.add(dir);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "rm", help = "remove files in HDFS")
	public void rm(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "skipTrash" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether skip trash") final boolean skipTrash,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive) {
		try {
			Path file = new Path(path);
			FileSystem fs = file.getFileSystem(getHadoopConfiguration());
			for (Path p : FileUtil.stat2Paths(fs.globStatus(file), file)) {
				FileStatus status = fs.getFileStatus(p);
				if (status.isDir() && !recursive) {
					LOG.severe("Cannot remove directory \"" + path +
                            "\", use fs rm --recursive instead");
				}
				if (!skipTrash) {
						Trash trash = new Trash(fs, getHadoopConfiguration());
						trash.moveToTrash(p);
				}
				fs.delete(p, recursive);
			}
		} catch (Throwable t) {
			LOG.severe("run HDFS shell failed. Message is: " + t.getMessage());
		}
		
	}
	
	
	
	@CliCommand(value = PREFIX + "setrep", help = "set replication number")
	public void setrep(
			@CliOption(key = { "replica" }, mandatory = true, help = "source file names") final int replica,
			@CliOption(key = { "path" }, mandatory = true, help = " path name") final String path,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "waiting" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether enable waiting list") final boolean waiting) {
		List<String> argv = new ArrayList<String>();
		argv.add("-setrep");
		if(recursive){
			argv.add("-R");
		}
		if(waiting){
			argv.add("-w");
		}
		argv.add(String.valueOf(replica));
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "tail", help = "tails file in HDFS")
	public void tail(
			@CliOption(key = { "" }, mandatory = true, help = "file to be tailed") final String path,
			@CliOption(key = { "file" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether show content while file grow") final boolean file) {
		List<String> argv = new ArrayList<String>();
		argv.add("-tail");
		if(file){
			argv.add("-f");
		}
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "text", help = "show the text content")
	public void text(
			@CliOption(key = { "" }, mandatory = true, help = "file to be showed") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-text");
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = PREFIX + "touchz", help = "touch the file")
	public void touchz(
			@CliOption(key = { "" }, mandatory = true, help = "file to be touched") final String path) {
		List<String> argv = new ArrayList<String>();
		argv.add("-touchz");
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliAvailabilityIndicator({PREFIX + "ls", PREFIX + "cat", PREFIX + "chgrp", 
		PREFIX + "chown", PREFIX + "chmod", PREFIX + "copyFromLocal", PREFIX + "put", PREFIX + "moveFromLocal",
		PREFIX + "copyToLocal", PREFIX + "get", PREFIX + "count", PREFIX + "cp", PREFIX + "mv", 
		PREFIX + "du", PREFIX + "expunge", PREFIX + "mkdir", PREFIX + "rm", 
		PREFIX + "setrep", PREFIX + "tail", PREFIX + "text", PREFIX + "touchz"})
	public boolean isCmdAvailable() {
		String fs = getHadoopConfiguration().get("fs.default.name");
		if(fs != null && fs.length() > 0){
			return true;
		}
		return false;
	}
	
	/**
	 * @param value
	 */
	private void runCommand(String command, String value) {
		List<String> argv = new ArrayList<String>();
		argv.add(command);
		String[] fileNames = value.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}

	private void run(String[] argv) {
		try {
			shell.run(argv);
		} catch (Throwable t) {
			LOG.severe("run HDFS shell failed. Message is: " + t.getMessage());
			if(t.getCause() != null){
				LOG.severe("root error message is:" + t.getCause().getMessage());
			}
			t.printStackTrace();
		}
	}

}