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
using Com.Sun.Security.Auth.Module;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	/// <summary>Verify that logins work</summary>
	public class TestSecureLogins : AbstractSecureRegistryTest
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestSecureLogins
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHasRealm()
		{
			NUnit.Framework.Assert.IsNotNull(GetRealm());
			Log.Info("ZK principal = {}", GetPrincipalAndRealm(ZookeeperLocalhost));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJaasFileSetup()
		{
			// the JVM has seemed inconsistent on setting up here
			NUnit.Framework.Assert.IsNotNull("jaasFile", jaasFile);
			string confFilename = Runtime.GetProperty(Environment.JaasConfKey);
			NUnit.Framework.Assert.AreEqual(jaasFile.GetAbsolutePath(), confFilename);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJaasFileBinding()
		{
			// the JVM has seemed inconsistent on setting up here
			NUnit.Framework.Assert.IsNotNull("jaasFile", jaasFile);
			RegistrySecurity.BindJVMtoJAASFile(jaasFile);
			string confFilename = Runtime.GetProperty(Environment.JaasConfKey);
			NUnit.Framework.Assert.AreEqual(jaasFile.GetAbsolutePath(), confFilename);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClientLogin()
		{
			LoginContext client = Login(AliceLocalhost, AliceClientContext, keytab_alice);
			try
			{
				LogLoginDetails(AliceLocalhost, client);
				string confFilename = Runtime.GetProperty(Environment.JaasConfKey);
				NUnit.Framework.Assert.IsNotNull("Unset: " + Environment.JaasConfKey, confFilename
					);
				string config = FileUtils.ReadFileToString(new FilePath(confFilename));
				Log.Info("{}=\n{}", confFilename, config);
				RegistrySecurity.SetZKSaslClientProperties(Alice, AliceClientContext);
			}
			finally
			{
				client.Logout();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZKServerContextLogin()
		{
			LoginContext client = Login(ZookeeperLocalhost, ZookeeperServerContext, keytab_zk
				);
			LogLoginDetails(ZookeeperLocalhost, client);
			client.Logout();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestServerLogin()
		{
			LoginContext loginContext = CreateLoginContextZookeeperLocalhost();
			loginContext.Login();
			loginContext.Logout();
		}

		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		public virtual LoginContext CreateLoginContextZookeeperLocalhost()
		{
			string principalAndRealm = GetPrincipalAndRealm(ZookeeperLocalhost);
			ICollection<Principal> principals = new HashSet<Principal>();
			principals.AddItem(new KerberosPrincipal(ZookeeperLocalhost));
			Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
				<object>());
			return new LoginContext(string.Empty, subject, null, KerberosConfiguration.CreateServerConfig
				(ZookeeperLocalhost, keytab_zk));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosAuth()
		{
			FilePath krb5conf = GetKdc().GetKrb5conf();
			string krbConfig = FileUtils.ReadFileToString(krb5conf);
			Log.Info("krb5.conf at {}:\n{}", krb5conf, krbConfig);
			Subject subject = new Subject();
			Krb5LoginModule krb5LoginModule = new Krb5LoginModule();
			IDictionary<string, string> options = new Dictionary<string, string>();
			options["keyTab"] = keytab_alice.GetAbsolutePath();
			options["principal"] = AliceLocalhost;
			options["debug"] = "true";
			options["doNotPrompt"] = "true";
			options["isInitiator"] = "true";
			options["refreshKrb5Config"] = "true";
			options["renewTGT"] = "true";
			options["storeKey"] = "true";
			options["useKeyTab"] = "true";
			options["useTicketCache"] = "true";
			krb5LoginModule.Initialize(subject, null, new Dictionary<string, string>(), options
				);
			bool loginOk = krb5LoginModule.Login();
			NUnit.Framework.Assert.IsTrue("Failed to login", loginOk);
			bool commitOk = krb5LoginModule.Commit();
			NUnit.Framework.Assert.IsTrue("Failed to Commit", commitOk);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRealmValid()
		{
			string defaultRealm = KerberosUtil.GetDefaultRealm();
			AssertNotEmpty("No default Kerberos Realm", defaultRealm);
			Log.Info("Default Realm '{}'", defaultRealm);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosRulesValid()
		{
			NUnit.Framework.Assert.IsTrue("!KerberosName.hasRulesBeenSet()", KerberosName.HasRulesBeenSet
				());
			string rules = KerberosName.GetRules();
			NUnit.Framework.Assert.AreEqual(kerberosRule, rules);
			Log.Info(rules);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidKerberosName()
		{
			new HadoopKerberosName(Zookeeper).GetShortName();
			new HadoopKerberosName(ZookeeperLocalhost).GetShortName();
			new HadoopKerberosName(ZookeeperRealm).GetShortName();
		}

		// standard rules don't pick this up
		// new HadoopKerberosName(ZOOKEEPER_LOCALHOST_REALM).getShortName();
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUGILogin()
		{
			UserGroupInformation ugi = LoginUGI(Zookeeper, keytab_zk);
			RegistrySecurity.UgiInfo ugiInfo = new RegistrySecurity.UgiInfo(ugi);
			Log.Info("logged in as: {}", ugiInfo);
			NUnit.Framework.Assert.IsTrue("security is not enabled: " + ugiInfo, UserGroupInformation
				.IsSecurityEnabled());
			NUnit.Framework.Assert.IsTrue("login is keytab based: " + ugiInfo, ugi.IsFromKeytab
				());
			// now we are here, build a SASL ACL
			ACL acl = ugi.DoAs(new _PrivilegedExceptionAction_202());
			NUnit.Framework.Assert.AreEqual(ZookeeperRealm, acl.GetId().GetId());
			NUnit.Framework.Assert.AreEqual(ZookeeperConfigOptions.SchemeSasl, acl.GetId().GetScheme
				());
			registrySecurity.AddSystemACL(acl);
		}

		private sealed class _PrivilegedExceptionAction_202 : PrivilegedExceptionAction<ACL
			>
		{
			public _PrivilegedExceptionAction_202()
			{
			}

			/// <exception cref="System.Exception"/>
			public ACL Run()
			{
				return AbstractSecureRegistryTest.registrySecurity.CreateSaslACLFromCurrentUser(0
					);
			}
		}
	}
}
