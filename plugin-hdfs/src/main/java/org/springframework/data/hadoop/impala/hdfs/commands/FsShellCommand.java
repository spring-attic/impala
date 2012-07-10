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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.impala.hdfs.HdfsConfiguration;
import org.springframework.roo.shell.CliAvailabilityIndicator;
import org.springframework.roo.shell.CliCommand;
import org.springframework.roo.shell.CliOption;
import org.springframework.roo.shell.CommandMarker;
import org.springframework.roo.support.logging.HandlerUtils;
import org.springframework.stereotype.Component;

/**
 * HDFS shell commands
 * 
 * @author Jarred Li
 *
 */
@Component
public class FsShellCommand implements CommandMarker {

	// Constants
	private static Logger LOGGER = HandlerUtils.getLogger(FsShellCommand.class);

	private FsShell shell;

	private boolean initialized;

	@Autowired
	private HdfsConfiguration hdfsConfiguration;

	@CliAvailabilityIndicator({ "dfs" })
	public boolean isCommandsAvailable() {
		return isHDFSUrlSet();
	}

	//TODO - add back in functionality to read a property file of a well known name to set the default value.
	//       This can be handled using @Value in HdfsConfiguration

	/**
	 * judge whether HDFS URL is set 
	 * 
	 * @return true - if HDFS URL is set
	 * 		   false - otherwise
	 */
	protected boolean isHDFSUrlSet() {
		boolean result = true;
		String dfsName = hdfsConfiguration.getDfsName();
		if (dfsName == null || dfsName.length() == 0) {
			result = false;
		}
		return result;
	}

	@CliCommand(value = "hdfs ls", help = "list files in HDFS")
	public void ls(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive) {
		setupShell();
		if (recursive) {
			runCommand("-lsr", path);
		}
		else {
			runCommand("-ls", path);
		}
	}

	/*
	@CliCommand(value = "hdfs lsr", help = "list files in HDFS with recursion")
	public void lsr(@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path) {
		setupShell();
		runCommand("-lsr", path);
	}
	*/

	@CliCommand(value = "hdfs cat", help = "show file content")
	public void cat(@CliOption(key = { "" }, mandatory = true, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "file name to be showed") final String path) {
		setupShell();
		runCommand("-cat", path);
	}

	@CliCommand(value = "hdfs chgrp", help = "change file group")
	public void chgrp(@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "group" }, mandatory = true, help = "group name") final String group,
			@CliOption(key = { "" }, mandatory = true, help = "file name to be changed group") final String path) {
		setupShell();

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
	
	@CliCommand(value = "hdfs chown", help = "change file ownership")
	public void chown(
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "owner" }, mandatory = true, help = "owner name") final String owner,
			@CliOption(key = { "" }, mandatory = true, help = "file name to be changed group") final String path) {
		setupShell();

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
	
	@CliCommand(value = "hdfs copyFromLocal", help = "copy local files to HDFS")
	public void copyFromLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-copyFromLocal");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs put", help = "copy local files to HDFS")
	public void put(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-put");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs moveFromLocal", help = "move local files to HDFS")
	public void moveFromLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-moveFromLocal");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = "hdfs copyToLocal", help = "copy HDFS files to local")
	public void copyToLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "ignoreCrc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether copy CRC") final boolean crc) {
		setupShell();

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
	
	@CliCommand(value = "hdfs get", help = "copy HDFS files to local")
	public void get(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "ignoreCrc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether copy CRC") final boolean crc) {
		setupShell();

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
	
	@CliCommand(value = "hdfs moveToLocal", help = "move HDFS files to local")
	public void moveToLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether copy CRC") final boolean crc) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-moveToLocal");
		if(crc){
			argv.add("-crc");
		}
		argv.add(source);
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = "hdfs count", help = "Count the number of directories, files, bytes, quota, and remaining quota")
	public void count(
			@CliOption(key = { "quota" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean quota,
			@CliOption(key = { "path" }, mandatory = true, help = " path name") final String path) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-count");
		if(quota){
			argv.add("-q");
		}
		String[] fileNames = path.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs cp", help = "copy files in the HDFS")
	public void cp(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-cp");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs mv", help = "move files in the HDFS")
	public void mv(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-mv");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs du", help = "display sizes of file")
	public void du(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "summary" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with summary") final boolean summary) {
		setupShell();

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

	/*
	@CliCommand(value = "hdfs dus", help = "display summary sizes of file")
	public void dus(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-dus");
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	*/
	
	@CliCommand(value = "hdfs expunge", help = "empty the trash")
	public void expunge() {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-expunge");
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs mergeget", help = "merge files")
	public void getmerge(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-getmerge");
		String[] fileNames = source.split(" ");
		argv.addAll(Arrays.asList(fileNames));
		argv.add(dest);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs mkdir", help = "create new directory")
	public void mkdir(
			@CliOption(key = { "" }, mandatory = true, help = "directory name") final String dir) {
		setupShell();

		List<String> argv = new ArrayList<String>();
		argv.add("-mkdir");
		argv.add(dir);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs rm", help = "remove files in HDFS")
	public void rm(
			@CliOption(key = { "" }, mandatory = false, specifiedDefaultValue = ".", unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { "skipTrash" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether skip trash") final boolean skipTrash,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive) {
		setupShell();
		List<String> argv = new ArrayList<String>();
		if(recursive){
			argv.add("-rmr");
		}
		else{
			argv.add("-rm");
		}
		if(skipTrash){
			argv.add("-skipTrash");
		}
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	
	@CliCommand(value = "hdfs setrep", help = "set replication number")
	public void setrep(
			@CliOption(key = { "replica" }, mandatory = true, help = "source file names") final int replica,
			@CliOption(key = { "path" }, mandatory = true, help = " path name") final String path,
			@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
			@CliOption(key = { "waiting" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether enable waiting list") final boolean waiting) {
		setupShell();

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
	
	@CliCommand(value = "hdfs tail", help = "tails file in HDFS")
	public void tail(
			@CliOption(key = { "" }, mandatory = true, help = "file to be tailed") final String path,
			@CliOption(key = { "file" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether show content while file grow") final boolean file) {
		setupShell();
		List<String> argv = new ArrayList<String>();
		argv.add("-tail");
		if(file){
			argv.add("-f");
		}
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs test", help = "test file in HDFS")
	public void test(
			@CliOption(key = { "" }, mandatory = true, help = "file to be tested") final String path,
			@CliOption(key = { "exist" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether the file exit") final boolean exist,
			@CliOption(key = { "zero" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether the file has zero length") final boolean zero,
			@CliOption(key = { "dir" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether the file is directory") final boolean directory
			) {
		setupShell();
		List<String> argv = new ArrayList<String>();
		argv.add("-test");
		if(exist){
			argv.add("-e");
		}
		if(zero){
			argv.add("-z");
		}
		if(directory){
			argv.add("-d");
		}
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs text", help = "show the text content")
	public void text(
			@CliOption(key = { "" }, mandatory = true, help = "file to be showed") final String path) {
		setupShell();
		List<String> argv = new ArrayList<String>();
		argv.add("-text");
		argv.add(path);
		run(argv.toArray(new String[0]));
	}
	
	@CliCommand(value = "hdfs touchz", help = "touch the file")
	public void touchz(
			@CliOption(key = { "" }, mandatory = true, help = "file to be touched") final String path) {
		setupShell();
		List<String> argv = new ArrayList<String>();
		argv.add("-touchz");
		argv.add(path);
		run(argv.toArray(new String[0]));
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
		} catch (Exception e) {
			LOGGER.warning("run HDFS shell failed. " + e.getMessage());
		}
	}

	private void setupShell() {
		if (!initialized) {
			Configuration config = new Configuration();
			config.setStrings("fs.default.name", hdfsConfiguration.getDfsName());
			shell = new FsShell(config);
			initialized = true;
		}
	}

}
