using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class TestKeyAuthorizationKeyProvider
	{
		private const string Cipher = "AES";

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateKey()
		{
			Configuration conf = new Configuration();
			KeyProvider kp = new UserProvider.Factory().CreateProvider(new URI("user:///"), conf
				);
			KeyAuthorizationKeyProvider.KeyACLs mock = Org.Mockito.Mockito.Mock<KeyAuthorizationKeyProvider.KeyACLs
				>();
			Org.Mockito.Mockito.When(mock.IsACLPresent("foo", KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			UserGroupInformation u1 = UserGroupInformation.CreateRemoteUser("u1");
			Org.Mockito.Mockito.When(mock.HasAccessToKey("foo", u1, KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			KeyProviderCryptoExtension kpExt = new KeyAuthorizationKeyProvider(KeyProviderCryptoExtension
				.CreateKeyProviderCryptoExtension(kp), mock);
			u1.DoAs(new _PrivilegedExceptionAction_62(kpExt, conf));
			// "bar" key not configured
			// Ignore
			// Unauthorized User
			UserGroupInformation.CreateRemoteUser("badGuy").DoAs(new _PrivilegedExceptionAction_87
				(kpExt, conf));
		}

		private sealed class _PrivilegedExceptionAction_62 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_62(KeyProviderCryptoExtension kpExt, Configuration
				 conf)
			{
				this.kpExt = kpExt;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					kpExt.CreateKey("foo", SecureRandom.GetSeed(16), TestKeyAuthorizationKeyProvider.
						NewOptions(conf));
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("User should be Authorized !!");
				}
				try
				{
					kpExt.CreateKey("bar", SecureRandom.GetSeed(16), TestKeyAuthorizationKeyProvider.
						NewOptions(conf));
					NUnit.Framework.Assert.Fail("User should NOT be Authorized !!");
				}
				catch (IOException)
				{
				}
				return null;
			}

			private readonly KeyProviderCryptoExtension kpExt;

			private readonly Configuration conf;
		}

		private sealed class _PrivilegedExceptionAction_87 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_87(KeyProviderCryptoExtension kpExt, Configuration
				 conf)
			{
				this.kpExt = kpExt;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					kpExt.CreateKey("foo", SecureRandom.GetSeed(16), TestKeyAuthorizationKeyProvider.
						NewOptions(conf));
					NUnit.Framework.Assert.Fail("User should NOT be Authorized !!");
				}
				catch (IOException)
				{
				}
				// Ignore
				return null;
			}

			private readonly KeyProviderCryptoExtension kpExt;

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOpsWhenACLAttributeExists()
		{
			Configuration conf = new Configuration();
			KeyProvider kp = new UserProvider.Factory().CreateProvider(new URI("user:///"), conf
				);
			KeyAuthorizationKeyProvider.KeyACLs mock = Org.Mockito.Mockito.Mock<KeyAuthorizationKeyProvider.KeyACLs
				>();
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.GenerateEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.DecryptEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.All)).ThenReturn(true);
			UserGroupInformation u1 = UserGroupInformation.CreateRemoteUser("u1");
			UserGroupInformation u2 = UserGroupInformation.CreateRemoteUser("u2");
			UserGroupInformation u3 = UserGroupInformation.CreateRemoteUser("u3");
			UserGroupInformation sudo = UserGroupInformation.CreateRemoteUser("sudo");
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u1, KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u2, KeyAuthorizationKeyProvider.KeyOpType
				.GenerateEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u3, KeyAuthorizationKeyProvider.KeyOpType
				.DecryptEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", sudo, KeyAuthorizationKeyProvider.KeyOpType
				.All)).ThenReturn(true);
			KeyProviderCryptoExtension kpExt = new KeyAuthorizationKeyProvider(KeyProviderCryptoExtension
				.CreateKeyProviderCryptoExtension(kp), mock);
			KeyProvider.KeyVersion barKv = u1.DoAs(new _PrivilegedExceptionAction_127(conf, kpExt
				));
			KeyProviderCryptoExtension.EncryptedKeyVersion barEKv = u2.DoAs(new _PrivilegedExceptionAction_159
				(kpExt, barKv));
			u3.DoAs(new _PrivilegedExceptionAction_173(kpExt, barKv, barEKv));
			sudo.DoAs(new _PrivilegedExceptionAction_187(conf, kpExt));
		}

		private sealed class _PrivilegedExceptionAction_127 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_127(Configuration conf, KeyProviderCryptoExtension
				 kpExt)
			{
				this.conf = conf;
				this.kpExt = kpExt;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				KeyProvider.Options opt = TestKeyAuthorizationKeyProvider.NewOptions(conf);
				IDictionary<string, string> m = new Dictionary<string, string>();
				m["key.acl.name"] = "testKey";
				opt.SetAttributes(m);
				try
				{
					KeyProvider.KeyVersion kv = kpExt.CreateKey("foo", SecureRandom.GetSeed(16), opt);
					kpExt.RollNewVersion(kv.GetName());
					kpExt.RollNewVersion(kv.GetName(), SecureRandom.GetSeed(16));
					kpExt.DeleteKey(kv.GetName());
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("User should be Authorized !!");
				}
				KeyProvider.KeyVersion retkv = null;
				try
				{
					retkv = kpExt.CreateKey("bar", SecureRandom.GetSeed(16), opt);
					kpExt.GenerateEncryptedKey(retkv.GetName());
					NUnit.Framework.Assert.Fail("User should NOT be Authorized to generate EEK !!");
				}
				catch (IOException)
				{
				}
				NUnit.Framework.Assert.IsNotNull(retkv);
				return retkv;
			}

			private readonly Configuration conf;

			private readonly KeyProviderCryptoExtension kpExt;
		}

		private sealed class _PrivilegedExceptionAction_159 : PrivilegedExceptionAction<KeyProviderCryptoExtension.EncryptedKeyVersion
			>
		{
			public _PrivilegedExceptionAction_159(KeyProviderCryptoExtension kpExt, KeyProvider.KeyVersion
				 barKv)
			{
				this.kpExt = kpExt;
				this.barKv = barKv;
			}

			/// <exception cref="System.Exception"/>
			public KeyProviderCryptoExtension.EncryptedKeyVersion Run()
			{
				try
				{
					kpExt.DeleteKey(barKv.GetName());
					NUnit.Framework.Assert.Fail("User should NOT be Authorized to " + "perform any other operation !!"
						);
				}
				catch (IOException)
				{
				}
				return kpExt.GenerateEncryptedKey(barKv.GetName());
			}

			private readonly KeyProviderCryptoExtension kpExt;

			private readonly KeyProvider.KeyVersion barKv;
		}

		private sealed class _PrivilegedExceptionAction_173 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_173(KeyProviderCryptoExtension kpExt, KeyProvider.KeyVersion
				 barKv, KeyProviderCryptoExtension.EncryptedKeyVersion barEKv)
			{
				this.kpExt = kpExt;
				this.barKv = barKv;
				this.barEKv = barEKv;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				try
				{
					kpExt.DeleteKey(barKv.GetName());
					NUnit.Framework.Assert.Fail("User should NOT be Authorized to " + "perform any other operation !!"
						);
				}
				catch (IOException)
				{
				}
				return kpExt.DecryptEncryptedKey(barEKv);
			}

			private readonly KeyProviderCryptoExtension kpExt;

			private readonly KeyProvider.KeyVersion barKv;

			private readonly KeyProviderCryptoExtension.EncryptedKeyVersion barEKv;
		}

		private sealed class _PrivilegedExceptionAction_187 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_187(Configuration conf, KeyProviderCryptoExtension
				 kpExt)
			{
				this.conf = conf;
				this.kpExt = kpExt;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				KeyProvider.Options opt = TestKeyAuthorizationKeyProvider.NewOptions(conf);
				IDictionary<string, string> m = new Dictionary<string, string>();
				m["key.acl.name"] = "testKey";
				opt.SetAttributes(m);
				try
				{
					KeyProvider.KeyVersion kv = kpExt.CreateKey("foo", SecureRandom.GetSeed(16), opt);
					kpExt.RollNewVersion(kv.GetName());
					kpExt.RollNewVersion(kv.GetName(), SecureRandom.GetSeed(16));
					KeyProviderCryptoExtension.EncryptedKeyVersion ekv = kpExt.GenerateEncryptedKey(kv
						.GetName());
					kpExt.DecryptEncryptedKey(ekv);
					kpExt.DeleteKey(kv.GetName());
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("User should be Allowed to do everything !!");
				}
				return null;
			}

			private readonly Configuration conf;

			private readonly KeyProviderCryptoExtension kpExt;
		}

		private static KeyProvider.Options NewOptions(Configuration conf)
		{
			KeyProvider.Options options = new KeyProvider.Options(conf);
			options.SetCipher(Cipher);
			options.SetBitLength(128);
			return options;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDecryptWithKeyVersionNameKeyMismatch()
		{
			Configuration conf = new Configuration();
			KeyProvider kp = new UserProvider.Factory().CreateProvider(new URI("user:///"), conf
				);
			KeyAuthorizationKeyProvider.KeyACLs mock = Org.Mockito.Mockito.Mock<KeyAuthorizationKeyProvider.KeyACLs
				>();
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.GenerateEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.DecryptEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.IsACLPresent("testKey", KeyAuthorizationKeyProvider.KeyOpType
				.All)).ThenReturn(true);
			UserGroupInformation u1 = UserGroupInformation.CreateRemoteUser("u1");
			UserGroupInformation u2 = UserGroupInformation.CreateRemoteUser("u2");
			UserGroupInformation u3 = UserGroupInformation.CreateRemoteUser("u3");
			UserGroupInformation sudo = UserGroupInformation.CreateRemoteUser("sudo");
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u1, KeyAuthorizationKeyProvider.KeyOpType
				.Management)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u2, KeyAuthorizationKeyProvider.KeyOpType
				.GenerateEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", u3, KeyAuthorizationKeyProvider.KeyOpType
				.DecryptEek)).ThenReturn(true);
			Org.Mockito.Mockito.When(mock.HasAccessToKey("testKey", sudo, KeyAuthorizationKeyProvider.KeyOpType
				.All)).ThenReturn(true);
			KeyProviderCryptoExtension kpExt = new KeyAuthorizationKeyProvider(KeyProviderCryptoExtension
				.CreateKeyProviderCryptoExtension(kp), mock);
			sudo.DoAs(new _PrivilegedExceptionAction_247(conf, kpExt));
		}

		private sealed class _PrivilegedExceptionAction_247 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_247(Configuration conf, KeyProviderCryptoExtension
				 kpExt)
			{
				this.conf = conf;
				this.kpExt = kpExt;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				KeyProvider.Options opt = TestKeyAuthorizationKeyProvider.NewOptions(conf);
				IDictionary<string, string> m = new Dictionary<string, string>();
				m["key.acl.name"] = "testKey";
				opt.SetAttributes(m);
				KeyProvider.KeyVersion kv = kpExt.CreateKey("foo", SecureRandom.GetSeed(16), opt);
				kpExt.RollNewVersion(kv.GetName());
				kpExt.RollNewVersion(kv.GetName(), SecureRandom.GetSeed(16));
				KeyProviderCryptoExtension.EncryptedKeyVersion ekv = kpExt.GenerateEncryptedKey(kv
					.GetName());
				ekv = KeyProviderCryptoExtension.EncryptedKeyVersion.CreateForDecryption(ekv.GetEncryptionKeyName
					() + "x", ekv.GetEncryptionKeyVersionName(), ekv.GetEncryptedKeyIv(), ekv.GetEncryptedKeyVersion
					().GetMaterial());
				kpExt.DecryptEncryptedKey(ekv);
				return null;
			}

			private readonly Configuration conf;

			private readonly KeyProviderCryptoExtension kpExt;
		}
	}
}
