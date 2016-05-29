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
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Service;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server;
using Org.Apache.Zookeeper.Server.Auth;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	/// <summary>Verify that the Mini ZK service can be started up securely</summary>
	public class TestSecureRegistry : AbstractSecureRegistryTest
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestSecureRegistry
			));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void BeforeTestSecureZKService()
		{
			EnableKerberosDebugging();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void AfterTestSecureZKService()
		{
			DisableKerberosDebugging();
			RegistrySecurity.ClearZKSaslClientProperties();
		}

		/// <summary>
		/// this is a cut and paste of some of the ZK internal code that was
		/// failing on windows and swallowing its exceptions
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLowlevelZKSaslLogin()
		{
			RegistrySecurity.BindZKToServerJAASContext(ZookeeperServerContext);
			string serverSection = Runtime.GetProperty(ZooKeeperSaslServer.LoginContextNameKey
				, ZooKeeperSaslServer.DefaultLoginContextName);
			NUnit.Framework.Assert.AreEqual(ZookeeperServerContext, serverSection);
			AppConfigurationEntry[] entries;
			entries = Configuration.GetConfiguration().GetAppConfigurationEntry(serverSection
				);
			NUnit.Framework.Assert.IsNotNull("null entries", entries);
			SaslServerCallbackHandler saslServerCallbackHandler = new SaslServerCallbackHandler
				(Configuration.GetConfiguration());
			Org.Apache.Zookeeper.Login login = new Org.Apache.Zookeeper.Login(serverSection, 
				saslServerCallbackHandler);
			try
			{
				login.StartThreadIfNeeded();
			}
			finally
			{
				login.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateSecureZK()
		{
			StartSecureZK();
			secureZK.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInsecureClientToZK()
		{
			StartSecureZK();
			UserZookeeperToCreateRoot();
			RegistrySecurity.ClearZKSaslClientProperties();
			CuratorService curatorService = StartCuratorServiceInstance("insecure client", false
				);
			curatorService.ZkList("/");
			curatorService.ZkMkPath(string.Empty, CreateMode.Persistent, false, RegistrySecurity
				.WorldReadWriteACL);
		}

		/// <summary>test that ZK can write as itself</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZookeeperCanWrite()
		{
			Runtime.SetProperty("curator-log-events", "true");
			StartSecureZK();
			CuratorService curator = null;
			LoginContext login = Login(ZookeeperLocalhost, ZookeeperClientContext, keytab_zk);
			try
			{
				LogLoginDetails(Zookeeper, login);
				RegistrySecurity.SetZKSaslClientProperties(Zookeeper, ZookeeperClientContext);
				curator = StartCuratorServiceInstance("ZK", true);
				Log.Info(curator.ToString());
				AddToTeardown(curator);
				curator.ZkMkPath("/", CreateMode.Persistent, false, RegistrySecurity.WorldReadWriteACL
					);
				curator.ZkList("/");
				curator.ZkMkPath("/zookeeper", CreateMode.Persistent, false, RegistrySecurity.WorldReadWriteACL
					);
			}
			finally
			{
				Logout(login);
				ServiceOperations.Stop(curator);
			}
		}

		/// <summary>Start a curator service instance</summary>
		/// <param name="name">name</param>
		/// <param name="secure">flag to indicate the cluster is secure</param>
		/// <returns>an inited and started curator service</returns>
		protected internal virtual CuratorService StartCuratorServiceInstance(string name
			, bool secure)
		{
			Configuration clientConf = new Configuration();
			clientConf.Set(KeyRegistryZkRoot, "/");
			clientConf.SetBoolean(KeyRegistrySecure, secure);
			Describe(Log, "Starting Curator service");
			CuratorService curatorService = new CuratorService(name, secureZK);
			curatorService.Init(clientConf);
			curatorService.Start();
			Log.Info("Curator Binding {}", curatorService.BindingDiagnosticDetails());
			return curatorService;
		}

		/// <summary>have the ZK user create the root dir.</summary>
		/// <remarks>
		/// have the ZK user create the root dir.
		/// This logs out the ZK user after and stops its curator instance,
		/// to avoid contamination
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void UserZookeeperToCreateRoot()
		{
			Runtime.SetProperty("curator-log-events", "true");
			CuratorService curator = null;
			LoginContext login = Login(ZookeeperLocalhost, ZookeeperClientContext, keytab_zk);
			try
			{
				LogLoginDetails(Zookeeper, login);
				RegistrySecurity.SetZKSaslClientProperties(Zookeeper, ZookeeperClientContext);
				curator = StartCuratorServiceInstance("ZK", true);
				Log.Info(curator.ToString());
				AddToTeardown(curator);
				curator.ZkMkPath("/", CreateMode.Persistent, false, RegistrySecurity.WorldReadWriteACL
					);
				ZKPathDumper pathDumper = curator.DumpPath(true);
				Log.Info(pathDumper.ToString());
			}
			finally
			{
				Logout(login);
				ServiceOperations.Stop(curator);
			}
		}
	}
}
