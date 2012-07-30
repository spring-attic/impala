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

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Jarred Li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class FsShellCommandsTest {

	@Autowired
	private FsShellCommands fsCmd;
	
	private String srcFile = "src/test/resources/test.properties";
	
	private String tmpFile = "/tmp/test.properties";
	
	private String newTmpFile = "/tmp/test.properties";
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		fsCmd.init();
		File f = new File(srcFile);
		String fullPath = f.getAbsolutePath();
		fsCmd.put(fullPath, tmpFile);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		fsCmd.rm(tmpFile, false, false);
		fsCmd = null;
	}


	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#ls(java.lang.String, boolean)}.
	 */
	@Test
	public void testLs() {
		fsCmd.ls("/", false);
	}
	
	@Test
	public void testLs_withRecursion() {
		fsCmd.ls("/", true);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#cat(java.lang.String)}.
	 */
	@Test
	public void testCat() {
		fsCmd.cat(tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#chgrp(boolean, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testChgrp() {
		fsCmd.chgrp(false, "hadoop", tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#chown(boolean, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testChown() {
		fsCmd.chown(false, "hadoop", tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#chmod(boolean, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testChmod() {
		fsCmd.chmod(false, "755", tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#copyFromLocal(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testCopyFromLocal() {
		fsCmd.rm(tmpFile, false, false);
		File f = new File(srcFile);
		String fullPath = f.getAbsolutePath();
		fsCmd.copyFromLocal(fullPath, tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#put(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testPut() {
		fsCmd.rm(tmpFile, false, false);
		File f = new File(srcFile);
		String fullPath = f.getAbsolutePath();
		fsCmd.put(fullPath, tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#moveFromLocal(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testMoveFromLocal() {
		File file = new File(tmpFile);
		if(!file.exists()){
			fsCmd.copyToLocal(tmpFile, tmpFile, true, false);
		}
		fsCmd.rm(tmpFile, false, false);
		fsCmd.moveFromLocal(tmpFile, tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#copyToLocal(java.lang.String, java.lang.String, boolean, boolean)}.
	 */
	@Test
	public void testCopyToLocal() {
		fsCmd.copyToLocal(tmpFile, tmpFile, true, false);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#get(java.lang.String, java.lang.String, boolean, boolean)}.
	 */
	@Test
	public void testGet() {
		File file = new File(tmpFile);
		if(file.exists()){
			file.delete();
		}
		fsCmd.get(tmpFile, tmpFile, true, false);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#count(boolean, java.lang.String)}.
	 */
	@Test
	public void testCount() {
		fsCmd.count(false, tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#cp(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testCp() {
		fsCmd.cp(tmpFile, newTmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#mv(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testMv() {
		fsCmd.mv(tmpFile, newTmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#du(java.lang.String, boolean)}.
	 */
	@Test
	public void testDu() {
		fsCmd.du("/tmp", false);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#expunge()}.
	 */
	@Test
	public void testExpunge() {
		fsCmd.expunge();
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#mkdir(java.lang.String)}.
	 */
	@Test
	public void testMkdir() {
		fsCmd.mkdir("/tmp/tmp");
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#rm(java.lang.String, boolean, boolean)}.
	 */
	@Test
	public void testRm() {
		fsCmd.rm("/tmp/tmp", false, true);
	}

	


	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#text(java.lang.String)}.
	 */
	@Test
	public void testText() {
		fsCmd.text(tmpFile);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.hdfs.FsShellCommands#touchz(java.lang.String)}.
	 */
	@Test
	public void testTouchz() {
		fsCmd.touchz("/tmp/touch");
	}

}
