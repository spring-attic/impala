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
package org.springframework.datda.hadoop.impala.hdfs.provider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.hadoop.impala.provider.ImpalaluginBannerProvider;


/**
 * @author Jarred Li
 *
 */
public class SpringHadoopAdminBannerProviderTest{

	private ImpalaluginBannerProvider bannerProvider;
	

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		bannerProvider = new ImpalaluginBannerProvider();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		bannerProvider = null;
	}


	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.provider.ImpalaluginBannerProvider#getBanner()}.
	 */
	@Test
	public void testGetBanner() {
		String banner = bannerProvider.getBanner();
		Assert.assertNotNull(banner);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.provider.ImpalaluginBannerProvider#getVersion()}.
	 */
	@Test
	public void testGetVersion() {
		String version = bannerProvider.getVersion();
		Assert.assertNotNull(version);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.provider.ImpalaluginBannerProvider#getWelcomMessage()}.
	 */
	@Test
	public void testGetWelcomMessage() {
		String msg = bannerProvider.getWelcomeMessage();
		Assert.assertNotNull(msg);
	}

	/**
	 * Test method for {@link org.springframework.data.hadoop.impala.provider.ImpalaluginBannerProvider#name()}.
	 */
	@Test
	public void testName() {
		String name = bannerProvider.name();
		Assert.assertNotNull(name);
	}

}
