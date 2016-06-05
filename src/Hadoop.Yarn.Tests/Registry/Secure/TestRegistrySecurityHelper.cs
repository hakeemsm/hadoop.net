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
using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Security;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Secure
{
	/// <summary>Test for registry security operations</summary>
	public class TestRegistrySecurityHelper : Assert
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRegistrySecurityHelper
			));

		public const string YarnExampleCom = "yarn@example.com";

		public const string SaslYarnExampleCom = "sasl:" + YarnExampleCom;

		public const string MapredExampleCom = "mapred@example.com";

		public const string SaslMapredExampleCom = "sasl:" + MapredExampleCom;

		public const string SaslMapredApache = "sasl:mapred@APACHE";

		public const string DigestF0af = "digest:f0afbeeb00baa";

		public const string SaslYarnShort = "sasl:yarn@";

		public const string SaslMapredShort = "sasl:mapred@";

		public const string RealmExampleCom = "example.com";

		private static RegistrySecurity registrySecurity;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupTestRegistrySecurityHelper()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(KeyRegistrySecure, true);
			conf.Set(KeyRegistryKerberosRealm, "KERBEROS");
			registrySecurity = new RegistrySecurity(string.Empty);
			// init the ACLs OUTSIDE A KERBEROS CLUSTER
			registrySecurity.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLSplitRealmed()
		{
			IList<string> pairs = registrySecurity.SplitAclPairs(SaslYarnExampleCom + ", " + 
				SaslMapredExampleCom, string.Empty);
			NUnit.Framework.Assert.AreEqual(SaslYarnExampleCom, pairs[0]);
			NUnit.Framework.Assert.AreEqual(SaslMapredExampleCom, pairs[1]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBuildAclsRealmed()
		{
			IList<ACL> acls = registrySecurity.BuildACLs(SaslYarnExampleCom + ", " + SaslMapredExampleCom
				, string.Empty, ZooDefs.Perms.All);
			NUnit.Framework.Assert.AreEqual(YarnExampleCom, acls[0].GetId().GetId());
			NUnit.Framework.Assert.AreEqual(MapredExampleCom, acls[1].GetId().GetId());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLDefaultRealm()
		{
			IList<string> pairs = registrySecurity.SplitAclPairs(SaslYarnShort + ", " + SaslMapredShort
				, RealmExampleCom);
			NUnit.Framework.Assert.AreEqual(SaslYarnExampleCom, pairs[0]);
			NUnit.Framework.Assert.AreEqual(SaslMapredExampleCom, pairs[1]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBuildAclsDefaultRealm()
		{
			IList<ACL> acls = registrySecurity.BuildACLs(SaslYarnShort + ", " + SaslMapredShort
				, RealmExampleCom, ZooDefs.Perms.All);
			NUnit.Framework.Assert.AreEqual(YarnExampleCom, acls[0].GetId().GetId());
			NUnit.Framework.Assert.AreEqual(MapredExampleCom, acls[1].GetId().GetId());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLSplitNullRealm()
		{
			IList<string> pairs = registrySecurity.SplitAclPairs(SaslYarnShort + ", " + SaslMapredShort
				, string.Empty);
			NUnit.Framework.Assert.AreEqual(SaslYarnShort, pairs[0]);
			NUnit.Framework.Assert.AreEqual(SaslMapredShort, pairs[1]);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBuildAclsNullRealm()
		{
			registrySecurity.BuildACLs(SaslYarnShort + ", " + SaslMapredShort, string.Empty, 
				ZooDefs.Perms.All);
			NUnit.Framework.Assert.Fail(string.Empty);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLDefaultRealmOnlySASL()
		{
			IList<string> pairs = registrySecurity.SplitAclPairs(SaslYarnShort + ", " + DigestF0af
				, RealmExampleCom);
			NUnit.Framework.Assert.AreEqual(SaslYarnExampleCom, pairs[0]);
			NUnit.Framework.Assert.AreEqual(DigestF0af, pairs[1]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLSplitMixed()
		{
			IList<string> pairs = registrySecurity.SplitAclPairs(SaslYarnShort + ", " + SaslMapredApache
				 + ", ,," + DigestF0af, RealmExampleCom);
			NUnit.Framework.Assert.AreEqual(SaslYarnExampleCom, pairs[0]);
			NUnit.Framework.Assert.AreEqual(SaslMapredApache, pairs[1]);
			NUnit.Framework.Assert.AreEqual(DigestF0af, pairs[2]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultAClsValid()
		{
			registrySecurity.BuildACLs(RegistryConstants.DefaultRegistrySystemAccounts, RealmExampleCom
				, ZooDefs.Perms.All);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultRealm()
		{
			string realm = RegistrySecurity.GetDefaultRealmInJVM();
			Log.Info("Realm {}", realm);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUGIProperties()
		{
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			ACL acl = registrySecurity.CreateACLForUser(user, ZooDefs.Perms.All);
			NUnit.Framework.Assert.IsFalse(RegistrySecurity.AllReadwriteAccess.Equals(acl));
			Log.Info("User {} has ACL {}", user, acl);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecurityImpliesKerberos()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean("hadoop.security.authentication", true);
			conf.SetBoolean(KeyRegistrySecure, true);
			conf.Set(KeyRegistryKerberosRealm, "KERBEROS");
			RegistrySecurity security = new RegistrySecurity("registry security");
			try
			{
				security.Init(conf);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("did not find " + RegistrySecurity.ENoKerberos + " in "
					 + e, e.ToString().Contains(RegistrySecurity.ENoKerberos));
			}
		}
	}
}
