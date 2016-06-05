/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry
{
	public class AbstractZKRegistryTest : RegistryTestHelper
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractZKRegistryTest
			));

		private static readonly AddingCompositeService servicesToTeardown = new AddingCompositeService
			("teardown");

		static AbstractZKRegistryTest()
		{
			// static initializer guarantees it is always started
			// ahead of any @BeforeClass methods
			servicesToTeardown.Init(new Configuration());
			servicesToTeardown.Start();
		}

		[Rule]
		public readonly Timeout testTimeout = new Timeout(10000);

		[Rule]
		public TestName methodName = new TestName();

		protected internal static void AddToTeardown(Org.Apache.Hadoop.Service.Service svc
			)
		{
			servicesToTeardown.AddService(svc);
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TeardownServices()
		{
			Describe(Log, "teardown of static services");
			servicesToTeardown.Close();
		}

		protected internal static MicroZookeeperService zookeeper;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void CreateZKServer()
		{
			FilePath zkDir = new FilePath("target/zookeeper");
			FileUtils.DeleteDirectory(zkDir);
			NUnit.Framework.Assert.IsTrue(zkDir.Mkdirs());
			zookeeper = new MicroZookeeperService("InMemoryZKService");
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(MicroZookeeperServiceKeys.KeyZkserviceDir, zkDir.GetAbsolutePath());
			zookeeper.Init(conf);
			zookeeper.Start();
			AddToTeardown(zookeeper);
		}

		/// <summary>give our thread a name</summary>
		[SetUp]
		public virtual void NameThread()
		{
			Sharpen.Thread.CurrentThread().SetName("JUnit");
		}

		/// <summary>Returns the connection string to use</summary>
		/// <returns>connection string</returns>
		public virtual string GetConnectString()
		{
			return zookeeper.GetConnectionString();
		}

		public virtual YarnConfiguration CreateRegistryConfiguration()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(RegistryConstants.KeyRegistryZkConnectionTimeout, 1000);
			conf.SetInt(RegistryConstants.KeyRegistryZkRetryInterval, 500);
			conf.SetInt(RegistryConstants.KeyRegistryZkRetryTimes, 10);
			conf.SetInt(RegistryConstants.KeyRegistryZkRetryCeiling, 10);
			conf.Set(RegistryConstants.KeyRegistryZkQuorum, zookeeper.GetConnectionString());
			return conf;
		}
	}
}
