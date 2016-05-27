using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class TestKMSACLs
	{
		[NUnit.Framework.Test]
		public virtual void TestDefaults()
		{
			KMSACLs acls = new KMSACLs(new Configuration(false));
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				NUnit.Framework.Assert.IsTrue(acls.HasAccess(type, UserGroupInformation.CreateRemoteUser
					("foo")));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCustom()
		{
			Configuration conf = new Configuration(false);
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), type.ToString() + " ");
			}
			KMSACLs acls = new KMSACLs(conf);
			foreach (KMSACLs.Type type_1 in KMSACLs.Type.Values())
			{
				NUnit.Framework.Assert.IsTrue(acls.HasAccess(type_1, UserGroupInformation.CreateRemoteUser
					(type_1.ToString())));
				NUnit.Framework.Assert.IsFalse(acls.HasAccess(type_1, UserGroupInformation.CreateRemoteUser
					("foo")));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestKeyAclConfigurationLoad()
		{
			Configuration conf = new Configuration(false);
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "test_key_1.MANAGEMENT", "CREATE");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "test_key_2.ALL", "CREATE");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "test_key_3.NONEXISTOPERATION", "CREATE"
				);
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "MANAGEMENT", "ROLLOVER");
			conf.Set(KMSConfiguration.WhitelistKeyAclPrefix + "MANAGEMENT", "DECRYPT_EEK");
			KMSACLs acls = new KMSACLs(conf);
			NUnit.Framework.Assert.IsTrue("expected key ACL size is 2 but got " + acls.keyAcls
				.Count, acls.keyAcls.Count == 2);
		}
	}
}
