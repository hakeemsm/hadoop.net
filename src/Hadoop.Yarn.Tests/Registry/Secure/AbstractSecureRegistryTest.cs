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
using System.Collections.Generic;
using System.IO;
using System.Text;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	/// <summary>Add kerberos tests.</summary>
	/// <remarks>
	/// Add kerberos tests. This is based on the (JUnit3) KerberosSecurityTestcase
	/// and its test case, <code>TestMiniKdc</code>
	/// </remarks>
	public class AbstractSecureRegistryTest : RegistryTestHelper
	{
		public const string Realm = "EXAMPLE.COM";

		public const string Zookeeper = "zookeeper";

		public const string ZookeeperLocalhost = "zookeeper/localhost";

		public const string Zookeeper1270001 = "zookeeper/127.0.0.1";

		public const string ZookeeperRealm = "zookeeper@" + Realm;

		public const string ZookeeperClientContext = Zookeeper;

		public const string ZookeeperServerContext = "ZOOKEEPER_SERVER";

		public const string ZookeeperLocalhostRealm = ZookeeperLocalhost + "@" + Realm;

		public const string Alice = "alice";

		public const string AliceClientContext = "alice";

		public const string AliceLocalhost = "alice/localhost";

		public const string Bob = "bob";

		public const string BobClientContext = "bob";

		public const string BobLocalhost = "bob/localhost";

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractSecureRegistryTest
			));

		public static readonly Configuration Conf;

		static AbstractSecureRegistryTest()
		{
			Conf = new Configuration();
			Conf.Set("hadoop.security.authentication", "kerberos");
			Conf.SetBoolean("hadoop.security.authorization", true);
		}

		private static readonly AddingCompositeService classTeardown = new AddingCompositeService
			("classTeardown");

		static AbstractSecureRegistryTest()
		{
			// static initializer guarantees it is always started
			// ahead of any @BeforeClass methods
			classTeardown.Init(Conf);
			classTeardown.Start();
		}

		public const string SunSecurityKrb5Debug = "sun.security.krb5.debug";

		private readonly AddingCompositeService teardown = new AddingCompositeService("teardown"
			);

		protected internal static MiniKdc kdc;

		protected internal static FilePath keytab_zk;

		protected internal static FilePath keytab_bob;

		protected internal static FilePath keytab_alice;

		protected internal static FilePath kdcWorkDir;

		protected internal static Properties kdcConf;

		protected internal static RegistrySecurity registrySecurity;

		[Rule]
		public readonly Timeout testTimeout = new Timeout(900000);

		[Rule]
		public TestName methodName = new TestName();

		protected internal MicroZookeeperService secureZK;

		protected internal static FilePath jaasFile;

		private LoginContext zookeeperLogin;

		private static string zkServerPrincipal;

		/// <summary>All class initialization for this test class</summary>
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void BeforeSecureRegistryTestClass()
		{
			registrySecurity = new RegistrySecurity("registrySecurity");
			registrySecurity.Init(Conf);
			SetupKDCAndPrincipals();
			RegistrySecurity.ClearJaasSystemProperties();
			RegistrySecurity.BindJVMtoJAASFile(jaasFile);
			InitHadoopSecurity();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void AfterSecureRegistryTestClass()
		{
			Describe(Log, "teardown of class");
			classTeardown.Close();
			TeardownKDC();
		}

		/// <summary>give our thread a name</summary>
		[SetUp]
		public virtual void NameThread()
		{
			Sharpen.Thread.CurrentThread().SetName("JUnit");
		}

		/// <summary>
		/// For unknown reasons, the before-class setting of the JVM properties were
		/// not being picked up.
		/// </summary>
		/// <remarks>
		/// For unknown reasons, the before-class setting of the JVM properties were
		/// not being picked up. This method addresses that by setting them
		/// before every test case
		/// </remarks>
		[SetUp]
		public virtual void BeforeSecureRegistryTest()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void AfterSecureRegistryTest()
		{
			Describe(Log, "teardown of instance");
			teardown.Close();
			StopSecureZK();
		}

		protected internal static void AddToClassTeardown(Org.Apache.Hadoop.Service.Service
			 svc)
		{
			classTeardown.AddService(svc);
		}

		protected internal virtual void AddToTeardown(Org.Apache.Hadoop.Service.Service svc
			)
		{
			teardown.AddService(svc);
		}

		/// <exception cref="System.Exception"/>
		public static void TeardownKDC()
		{
			if (kdc != null)
			{
				kdc.Stop();
				kdc = null;
			}
		}

		/// <summary>Sets up the KDC and a set of principals in the JAAS file</summary>
		/// <exception cref="System.Exception"/>
		public static void SetupKDCAndPrincipals()
		{
			// set up the KDC
			FilePath target = new FilePath(Runtime.GetProperty("test.dir", "target"));
			kdcWorkDir = new FilePath(target, "kdc");
			kdcWorkDir.Mkdirs();
			if (!kdcWorkDir.Mkdirs())
			{
				NUnit.Framework.Assert.IsTrue(kdcWorkDir.IsDirectory());
			}
			kdcConf = MiniKdc.CreateConf();
			kdcConf.SetProperty(MiniKdc.Debug, "true");
			kdc = new MiniKdc(kdcConf, kdcWorkDir);
			kdc.Start();
			keytab_zk = CreateKeytab(Zookeeper, "zookeeper.keytab");
			keytab_alice = CreateKeytab(Alice, "alice.keytab");
			keytab_bob = CreateKeytab(Bob, "bob.keytab");
			zkServerPrincipal = Shell.Windows ? Zookeeper1270001 : ZookeeperLocalhost;
			StringBuilder jaas = new StringBuilder(1024);
			jaas.Append(registrySecurity.CreateJAASEntry(ZookeeperClientContext, Zookeeper, keytab_zk
				));
			jaas.Append(registrySecurity.CreateJAASEntry(ZookeeperServerContext, zkServerPrincipal
				, keytab_zk));
			jaas.Append(registrySecurity.CreateJAASEntry(AliceClientContext, AliceLocalhost, 
				keytab_alice));
			jaas.Append(registrySecurity.CreateJAASEntry(BobClientContext, BobLocalhost, keytab_bob
				));
			jaasFile = new FilePath(kdcWorkDir, "jaas.txt");
			FileUtils.Write(jaasFile, jaas.ToString());
			Log.Info("\n" + jaas);
			RegistrySecurity.BindJVMtoJAASFile(jaasFile);
		}

		protected internal const string kerberosRule = "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";

		//
		/// <summary>Init hadoop security by setting up the UGI config</summary>
		public static void InitHadoopSecurity()
		{
			UserGroupInformation.SetConfiguration(Conf);
			KerberosName.SetRules(kerberosRule);
		}

		/// <summary>Stop the secure ZK and log out the ZK account</summary>
		public virtual void StopSecureZK()
		{
			lock (this)
			{
				ServiceOperations.Stop(secureZK);
				secureZK = null;
				Logout(zookeeperLogin);
				zookeeperLogin = null;
			}
		}

		public static MiniKdc GetKdc()
		{
			return kdc;
		}

		public static FilePath GetKdcWorkDir()
		{
			return kdcWorkDir;
		}

		public static Properties GetKdcConf()
		{
			return kdcConf;
		}

		/// <summary>Create a secure instance</summary>
		/// <param name="name">instance name</param>
		/// <returns>the instance</returns>
		/// <exception cref="System.Exception"/>
		protected internal static MicroZookeeperService CreateSecureZKInstance(string name
			)
		{
			string context = ZookeeperServerContext;
			Configuration conf = new Configuration();
			FilePath testdir = new FilePath(Runtime.GetProperty("test.dir", "target"));
			FilePath workDir = new FilePath(testdir, name);
			if (!workDir.Mkdirs())
			{
				NUnit.Framework.Assert.IsTrue(workDir.IsDirectory());
			}
			Runtime.SetProperty(ZookeeperConfigOptions.PropZkServerMaintainConnectionDespiteSaslFailure
				, "false");
			RegistrySecurity.ValidateContext(context);
			conf.Set(MicroZookeeperServiceKeys.KeyRegistryZkserviceJaasContext, context);
			MicroZookeeperService secureZK = new MicroZookeeperService(name);
			secureZK.Init(conf);
			Log.Info(secureZK.GetDiagnostics());
			return secureZK;
		}

		/// <summary>
		/// Create the keytabl for the given principal, includes
		/// raw principal and $principal/localhost
		/// </summary>
		/// <param name="principal">principal short name</param>
		/// <param name="filename">filename of keytab</param>
		/// <returns>file of keytab</returns>
		/// <exception cref="System.Exception"/>
		public static FilePath CreateKeytab(string principal, string filename)
		{
			AssertNotEmpty("empty principal", principal);
			AssertNotEmpty("empty host", filename);
			NUnit.Framework.Assert.IsNotNull("Null KDC", kdc);
			FilePath keytab = new FilePath(kdcWorkDir, filename);
			kdc.CreatePrincipal(keytab, principal, principal + "/localhost", principal + "/127.0.0.1"
				);
			return keytab;
		}

		public static string GetPrincipalAndRealm(string principal)
		{
			return principal + "@" + GetRealm();
		}

		protected internal static string GetRealm()
		{
			return kdc.GetRealm();
		}

		/// <summary>Log in, defaulting to the client context</summary>
		/// <param name="principal">principal</param>
		/// <param name="context">context</param>
		/// <param name="keytab">keytab</param>
		/// <returns>the logged in context</returns>
		/// <exception cref="Javax.Security.Auth.Login.LoginException">failure to log in</exception>
		/// <exception cref="System.IO.FileNotFoundException">no keytab</exception>
		protected internal virtual LoginContext Login(string principal, string context, FilePath
			 keytab)
		{
			Log.Info("Logging in as {} in context {} with keytab {}", principal, context, keytab
				);
			if (!keytab.Exists())
			{
				throw new FileNotFoundException(keytab.GetAbsolutePath());
			}
			ICollection<Principal> principals = new HashSet<Principal>();
			principals.AddItem(new KerberosPrincipal(principal));
			Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
				<object>());
			LoginContext login;
			login = new LoginContext(context, subject, null, KerberosConfiguration.CreateClientConfig
				(principal, keytab));
			login.Login();
			return login;
		}

		/// <summary>Start the secure ZK instance using the test method name as the path.</summary>
		/// <remarks>
		/// Start the secure ZK instance using the test method name as the path.
		/// As the entry is saved to the
		/// <see cref="secureZK"/>
		/// field, it
		/// is automatically stopped after the test case.
		/// </remarks>
		/// <exception cref="System.Exception">on any failure</exception>
		protected internal virtual void StartSecureZK()
		{
			lock (this)
			{
				NUnit.Framework.Assert.IsNull("Zookeeper is already running", secureZK);
				zookeeperLogin = Login(zkServerPrincipal, ZookeeperServerContext, keytab_zk);
				secureZK = CreateSecureZKInstance("test-" + methodName.GetMethodName());
				secureZK.Start();
			}
		}
	}
}
