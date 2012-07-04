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
	public void chown(@CliOption(key = { "recursive" }, mandatory = false, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", help = "whether with recursion") final boolean recursive,
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
	
	
	@CliCommand(value = "hdfs copyToLocal", help = "copy HDFS files to local")
	public void copyToLocal(
			@CliOption(key = { "from" }, mandatory = true, help = "source file names") final String source,
			@CliOption(key = { "to" }, mandatory = true, help = "destination path name") final String dest,
			@CliOption(key = { "ignoreCrc" }, mandatory = false, specifiedDefaultValue = "true", help = "whether ignore CRC") final boolean ignoreCrc,
			@CliOption(key = { "crc" }, mandatory = false, specifiedDefaultValue = "true", help = "whether copy CRC") final boolean crc) {
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
	

	//TODO - these should be their own commands.
	@CliCommand(value = "dfs", help = "run dfs commands")
	public void runDfsCommands(@CliOption(key = { "ls" }, mandatory = false, specifiedDefaultValue = ".", help = "directory to be listed") final String ls,
			@CliOption(key = { "lsr" }, mandatory = false, specifiedDefaultValue = ".", help = "directory to be listed with recursion") final String lsr,
			@CliOption(key = { "cat" }, mandatory = false, help = "file to be showed") final String cat, @CliOption(key = { "chgrp" }, mandatory = false, help = "file to be changed group") final String chgrp,
			@CliOption(key = { "chmod" }, mandatory = false, help = "file to be changed right") final String chmod,
			@CliOption(key = { "chown" }, mandatory = false, help = "file to be changed owner") final String chown,
			@CliOption(key = { "copyFromLocal" }, mandatory = false, help = "copy from local to HDFS") final String copyFromLocal,
			@CliOption(key = { "copyToLocal" }, mandatory = false, help = "copy HDFS to local") final String copyToLocal,
			@CliOption(key = { "count" }, mandatory = false, help = "file to be count") final String count,
			@CliOption(key = { "cp" }, mandatory = false, help = "file to be copied") final String cp,
			@CliOption(key = { "du" }, mandatory = false, help = "display sizes of file") final String du,
			@CliOption(key = { "dus" }, mandatory = false, help = "display summary sizes of file") final String dus,
			@CliOption(key = { "expunge" }, mandatory = false, help = "empty the trash") final String expunge,
			@CliOption(key = { "get" }, mandatory = false, help = "copy to local") final String get,
			@CliOption(key = { "getmerge" }, mandatory = false, help = "merge file") final String getmerge,
			@CliOption(key = { "mkdir" }, mandatory = false, help = "create new directory") final String mkdir,
			@CliOption(key = { "moveFromLocal" }, mandatory = false, help = "move local to HDFS") final String moveFromLocal,
			@CliOption(key = { "moveToLocal" }, mandatory = false, help = "move local to HDFS") final String moveToLocal,
			@CliOption(key = { "mv" }, mandatory = false, help = "move file from source to destination") final String mv,
			@CliOption(key = { "put" }, mandatory = false, help = "copy from local to HDFS") final String put,
			@CliOption(key = { "rm" }, mandatory = false, help = "remove file") final String rm,
			@CliOption(key = { "rmr" }, mandatory = false, help = "remove file with recursion") final String rmr,
			@CliOption(key = { "setrep" }, mandatory = false, help = "set replication number") final String setrep,
			@CliOption(key = { "stat" }, mandatory = false, help = "return stat information") final String stat,
			@CliOption(key = { "tail" }, mandatory = false, help = "tail the file") final String tail,
			@CliOption(key = { "test" }, mandatory = false, help = "check a file") final String test,
			@CliOption(key = { "text" }, mandatory = false, help = "output the file in text format") final String text,
			@CliOption(key = { "touchz" }, mandatory = false, help = "create a file of zero lenth") final String touchz) {

		//TODO - should not recreate shell over and over again. 
		setupShell();

		if (ls != null) {
			runCommand("-ls", ls);
			return;
		}
		else if (lsr != null) {
			runCommand("-lsr", lsr);
			return;
		}
		else if (cat != null) {
			runCommand("-cat", cat);
			return;
		}
		else if (chgrp != null) {
			runCommand("-chgrp", chgrp);
			return;
		}
		else if (chmod != null) {
			runCommand("-chmod", chmod);
			return;
		}
		else if (chown != null) {
			runCommand("-chown", chown);
			return;
		}
		else if (copyFromLocal != null) {
			runCommand("-copyFromLocal", copyFromLocal);
			return;
		}
		else if (copyToLocal != null) {
			runCommand("-copyToLocal", copyToLocal);
			return;
		}
		else if (count != null) {
			runCommand("-count", count);
			return;
		}
		else if (cp != null) {
			runCommand("-cp", cp);
			return;
		}
		else if (du != null) {
			runCommand("-du", du);
			return;
		}
		else if (dus != null) {
			runCommand("-dus", dus);
			return;
		}
		else if (expunge != null) {
			runCommand("-expunge", expunge);
			return;
		}
		else if (get != null) {
			runCommand("-get", get);
			return;
		}
		else if (getmerge != null) {
			runCommand("-getmerge", getmerge);
			return;
		}
		else if (mkdir != null) {
			runCommand("-mkdir", mkdir);
			return;
		}
		else if (moveFromLocal != null) {
			runCommand("-moveFromLocal", moveFromLocal);
			return;
		}
		else if (moveToLocal != null) {
			runCommand("-moveToLocal", moveToLocal);
			return;
		}
		else if (mv != null) {
			runCommand("-mv", mv);
			return;
		}
		else if (put != null) {
			runCommand("-put", put);
			return;
		}
		else if (rm != null) {
			runCommand("-rm", rm);
			return;
		}
		else if (rmr != null) {
			runCommand("-rmr", rmr);
			return;
		}
		else if (setrep != null) {
			runCommand("-setrep", setrep);
			return;
		}
		else if (stat != null) {
			runCommand("-stat", stat);
			return;
		}
		else if (tail != null) {
			runCommand("-tail", tail);
			return;
		}
		else if (test != null) {
			runCommand("-test", test);
			return;
		}
		else if (text != null) {
			runCommand("-text", text);
			return;
		}
		else if (touchz != null) {
			runCommand("-touchz", touchz);
			return;
		}
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
