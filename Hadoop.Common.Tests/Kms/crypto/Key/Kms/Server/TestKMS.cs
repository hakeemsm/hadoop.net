using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Org.Apache.Curator.Test;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class TestKMS
	{
		[SetUp]
		public virtual void CleanUp()
		{
			// resetting kerberos security
			Configuration conf = new Configuration();
			UserGroupInformation.SetConfiguration(conf);
		}

		/// <exception cref="System.Exception"/>
		public static FilePath GetTestDir()
		{
			FilePath file = new FilePath("dummy");
			file = file.GetAbsoluteFile();
			file = file.GetParentFile();
			file = new FilePath(file, "target");
			file = new FilePath(file, UUID.RandomUUID().ToString());
			if (!file.Mkdirs())
			{
				throw new RuntimeException("Could not create test directory: " + file);
			}
			return file;
		}

		public abstract class KMSCallable<T> : Callable<T>
		{
			private Uri kmsUrl;

			protected internal virtual Uri GetKMSUrl()
			{
				return kmsUrl;
			}

			public abstract T Call();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual KeyProvider CreateProvider(URI uri, Configuration conf
			)
		{
			return new LoadBalancingKMSClientProvider(new KMSClientProvider[] { new KMSClientProvider
				(uri, conf) }, conf);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual T RunServer<T>(string keystore, string password, FilePath
			 confDir, TestKMS.KMSCallable<T> callable)
		{
			return RunServer(-1, keystore, password, confDir, callable);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual T RunServer<T>(int port, string keystore, string password
			, FilePath confDir, TestKMS.KMSCallable<T> callable)
		{
			MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder().SetKmsConfDir(confDir).SetLog4jConfFile
				("log4j.properties");
			if (keystore != null)
			{
				miniKMSBuilder.SetSslConf(new FilePath(keystore), password);
			}
			if (port > 0)
			{
				miniKMSBuilder.SetPort(port);
			}
			MiniKMS miniKMS = miniKMSBuilder.Build();
			miniKMS.Start();
			try
			{
				System.Console.Out.WriteLine("Test KMS running at: " + miniKMS.GetKMSUrl());
				callable.kmsUrl = miniKMS.GetKMSUrl();
				return callable.Call();
			}
			finally
			{
				miniKMS.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual Configuration CreateBaseKMSConf(FilePath keyStoreDir)
		{
			Configuration conf = new Configuration(false);
			conf.Set(KMSConfiguration.KeyProviderUri, "jceks://file@" + new Path(keyStoreDir.
				GetAbsolutePath(), "kms.keystore").ToUri());
			conf.Set("hadoop.kms.authentication.type", "simple");
			return conf;
		}

		/// <exception cref="System.Exception"/>
		public static void WriteConf(FilePath confDir, Configuration conf)
		{
			TextWriter writer = new FileWriter(new FilePath(confDir, KMSConfiguration.KmsSiteXml
				));
			conf.WriteXml(writer);
			writer.Close();
			writer = new FileWriter(new FilePath(confDir, KMSConfiguration.KmsAclsXml));
			conf.WriteXml(writer);
			writer.Close();
			//create empty core-site.xml
			writer = new FileWriter(new FilePath(confDir, "core-site.xml"));
			new Configuration(false).WriteXml(writer);
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		public static URI CreateKMSUri(Uri kmsUrl)
		{
			string str = kmsUrl.ToString();
			str = str.ReplaceFirst("://", "@");
			return new URI("kms://" + str);
		}

		private class KerberosConfiguration : Configuration
		{
			private string principal;

			private string keytab;

			private bool isInitiator;

			private KerberosConfiguration(string principal, FilePath keytab, bool client)
			{
				this.principal = principal;
				this.keytab = keytab.GetAbsolutePath();
				this.isInitiator = client;
			}

			public static Configuration CreateClientConfig(string principal, FilePath keytab)
			{
				return new TestKMS.KerberosConfiguration(principal, keytab, true);
			}

			private static string GetKrb5LoginModuleName()
			{
				return Runtime.GetProperty("java.vendor").Contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule"
					 : "com.sun.security.auth.module.Krb5LoginModule";
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				IDictionary<string, string> options = new Dictionary<string, string>();
				options["keyTab"] = keytab;
				options["principal"] = principal;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["doNotPrompt"] = "true";
				options["useTicketCache"] = "true";
				options["renewTGT"] = "true";
				options["refreshKrb5Config"] = "true";
				options["isInitiator"] = bool.ToString(isInitiator);
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					options["ticketCache"] = ticketCache;
				}
				options["debug"] = "true";
				return new AppConfigurationEntry[] { new AppConfigurationEntry(GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}
		}

		private static MiniKdc kdc;

		private static FilePath keytab;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpMiniKdc()
		{
			FilePath kdcDir = GetTestDir();
			Properties kdcConf = MiniKdc.CreateConf();
			kdc = new MiniKdc(kdcConf, kdcDir);
			kdc.Start();
			keytab = new FilePath(kdcDir, "keytab");
			IList<string> principals = new AList<string>();
			principals.AddItem("HTTP/localhost");
			principals.AddItem("client");
			principals.AddItem("hdfs");
			principals.AddItem("otheradmin");
			principals.AddItem("client/host");
			principals.AddItem("client1");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				principals.AddItem(type.ToString());
			}
			principals.AddItem("CREATE_MATERIAL");
			principals.AddItem("ROLLOVER_MATERIAL");
			kdc.CreatePrincipal(keytab, Sharpen.Collections.ToArray(principals, new string[principals
				.Count]));
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownMiniKdc()
		{
			if (kdc != null)
			{
				kdc.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private T DoAs<T>(string user, PrivilegedExceptionAction<T> action)
		{
			ICollection<Principal> principals = new HashSet<Principal>();
			principals.AddItem(new KerberosPrincipal(user));
			//client login
			Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
				<object>());
			LoginContext loginContext = new LoginContext(string.Empty, subject, null, TestKMS.KerberosConfiguration
				.CreateClientConfig(user, keytab));
			try
			{
				loginContext.Login();
				subject = loginContext.GetSubject();
				UserGroupInformation ugi = UserGroupInformation.GetUGIFromSubject(subject);
				return ugi.DoAs(action);
			}
			finally
			{
				loginContext.Logout();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStartStop(bool ssl, bool kerberos)
		{
			Configuration conf = new Configuration();
			if (kerberos)
			{
				conf.Set("hadoop.security.authentication", "kerberos");
			}
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			string keystore;
			string password;
			if (ssl)
			{
				string sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestKMS));
				KeyStoreTestUtil.SetupSSLConfig(testDir.GetAbsolutePath(), sslConfDir, conf, false
					);
				keystore = testDir.GetAbsolutePath() + "/serverKS.jks";
				password = "serverP";
			}
			else
			{
				keystore = null;
				password = null;
			}
			conf.Set("hadoop.kms.authentication.token.validity", "1");
			if (kerberos)
			{
				conf.Set("hadoop.kms.authentication.type", "kerberos");
				conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
				conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
				conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			}
			WriteConf(testDir, conf);
			RunServer(keystore, password, testDir, new _KMSCallable_306(this, keystore, ssl, 
				kerberos));
		}

		private sealed class _KMSCallable_306 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_306(TestKMS _enclosing, string keystore, bool ssl, bool kerberos
				)
			{
				this._enclosing = _enclosing;
				this.keystore = keystore;
				this.ssl = ssl;
				this.kerberos = kerberos;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				Uri url = this.GetKMSUrl();
				NUnit.Framework.Assert.AreEqual(keystore != null, url.Scheme.Equals("https"));
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				if (ssl)
				{
					KeyProvider testKp = this._enclosing.CreateProvider(uri, conf);
					ThreadGroup threadGroup = Sharpen.Thread.CurrentThread().GetThreadGroup();
					while (threadGroup.GetParent() != null)
					{
						threadGroup = threadGroup.GetParent();
					}
					Sharpen.Thread[] threads = new Sharpen.Thread[threadGroup.ActiveCount()];
					threadGroup.Enumerate(threads);
					Sharpen.Thread reloaderThread = null;
					foreach (Sharpen.Thread thread in threads)
					{
						if ((thread.GetName() != null) && (thread.GetName().Contains("Truststore reloader thread"
							)))
						{
							reloaderThread = thread;
						}
					}
					NUnit.Framework.Assert.IsTrue("Reloader is not alive", reloaderThread.IsAlive());
					testKp.Close();
					bool reloaderStillAlive = true;
					for (int i = 0; i < 10; i++)
					{
						reloaderStillAlive = reloaderThread.IsAlive();
						if (!reloaderStillAlive)
						{
							break;
						}
						Sharpen.Thread.Sleep(1000);
					}
					NUnit.Framework.Assert.IsFalse("Reloader is still alive", reloaderStillAlive);
				}
				if (kerberos)
				{
					foreach (string user in new string[] { "client", "client/host" })
					{
						this._enclosing.DoAs(user, new _PrivilegedExceptionAction_343(this, uri, conf));
					}
				}
				else
				{
					// getKeys() empty
					KeyProvider kp = this._enclosing.CreateProvider(uri, conf);
					// getKeys() empty
					NUnit.Framework.Assert.IsTrue(kp.GetKeys().IsEmpty());
					Sharpen.Thread.Sleep(4000);
					Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = ((KeyProviderDelegationTokenExtension.DelegationTokenExtension
						)kp).AddDelegationTokens("myuser", new Credentials());
					NUnit.Framework.Assert.AreEqual(1, tokens.Length);
					NUnit.Framework.Assert.AreEqual("kms-dt", tokens[0].GetKind().ToString());
				}
				return null;
			}

			private sealed class _PrivilegedExceptionAction_343 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_343(_KMSCallable_306 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					NUnit.Framework.Assert.IsTrue(kp.GetKeys().IsEmpty());
					Sharpen.Thread.Sleep(4000);
					Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = ((KeyProviderDelegationTokenExtension.DelegationTokenExtension
						)kp).AddDelegationTokens("myuser", new Credentials());
					NUnit.Framework.Assert.AreEqual(1, tokens.Length);
					NUnit.Framework.Assert.AreEqual("kms-dt", tokens[0].GetKind().ToString());
					return null;
				}

				private readonly _KMSCallable_306 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;

			private readonly string keystore;

			private readonly bool ssl;

			private readonly bool kerberos;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartStopHttpPseudo()
		{
			TestStartStop(false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartStopHttpsPseudo()
		{
			TestStartStop(true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartStopHttpKerberos()
		{
			TestStartStop(false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartStopHttpsKerberos()
		{
			TestStartStop(true, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSProvider()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath confDir = GetTestDir();
			conf = CreateBaseKMSConf(confDir);
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k1.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k2.MANAGEMENT", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k2.READ", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k3.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k4.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k5.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k6.ALL", "*");
			WriteConf(confDir, conf);
			RunServer(null, null, confDir, new _KMSCallable_413(this));
		}

		private sealed class _KMSCallable_413 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_413(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				DateTime started = new DateTime();
				Configuration conf = new Configuration();
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				KeyProvider kp = this._enclosing.CreateProvider(uri, conf);
				// getKeys() empty
				NUnit.Framework.Assert.IsTrue(kp.GetKeys().IsEmpty());
				// getKeysMetadata() empty
				NUnit.Framework.Assert.AreEqual(0, kp.GetKeysMetadata().Length);
				// createKey()
				KeyProvider.Options options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				options.SetDescription("l1");
				KeyProvider.KeyVersion kv0 = kp.CreateKey("k1", options);
				NUnit.Framework.Assert.IsNotNull(kv0);
				NUnit.Framework.Assert.IsNotNull(kv0.GetVersionName());
				NUnit.Framework.Assert.IsNotNull(kv0.GetMaterial());
				// getKeyVersion()
				KeyProvider.KeyVersion kv1 = kp.GetKeyVersion(kv0.GetVersionName());
				NUnit.Framework.Assert.AreEqual(kv0.GetVersionName(), kv1.GetVersionName());
				NUnit.Framework.Assert.IsNotNull(kv1.GetMaterial());
				// getCurrent()
				KeyProvider.KeyVersion cv1 = kp.GetCurrentKey("k1");
				NUnit.Framework.Assert.AreEqual(kv0.GetVersionName(), cv1.GetVersionName());
				NUnit.Framework.Assert.IsNotNull(cv1.GetMaterial());
				// getKeyMetadata() 1 version
				KeyProvider.Metadata m1 = kp.GetMetadata("k1");
				NUnit.Framework.Assert.AreEqual("AES/CTR/NoPadding", m1.GetCipher());
				NUnit.Framework.Assert.AreEqual("AES", m1.GetAlgorithm());
				NUnit.Framework.Assert.AreEqual(128, m1.GetBitLength());
				NUnit.Framework.Assert.AreEqual(1, m1.GetVersions());
				NUnit.Framework.Assert.IsNotNull(m1.GetCreated());
				NUnit.Framework.Assert.IsTrue(started.Before(m1.GetCreated()));
				// getKeyVersions() 1 version
				IList<KeyProvider.KeyVersion> lkv1 = kp.GetKeyVersions("k1");
				NUnit.Framework.Assert.AreEqual(1, lkv1.Count);
				NUnit.Framework.Assert.AreEqual(kv0.GetVersionName(), lkv1[0].GetVersionName());
				NUnit.Framework.Assert.IsNotNull(kv1.GetMaterial());
				// rollNewVersion()
				KeyProvider.KeyVersion kv2 = kp.RollNewVersion("k1");
				NUnit.Framework.Assert.AreNotSame(kv0.GetVersionName(), kv2.GetVersionName());
				NUnit.Framework.Assert.IsNotNull(kv2.GetMaterial());
				// getKeyVersion()
				kv2 = kp.GetKeyVersion(kv2.GetVersionName());
				bool eq = true;
				for (int i = 0; i < kv1.GetMaterial().Length; i++)
				{
					eq = eq && kv1.GetMaterial()[i] == kv2.GetMaterial()[i];
				}
				NUnit.Framework.Assert.IsFalse(eq);
				// getCurrent()
				KeyProvider.KeyVersion cv2 = kp.GetCurrentKey("k1");
				NUnit.Framework.Assert.AreEqual(kv2.GetVersionName(), cv2.GetVersionName());
				NUnit.Framework.Assert.IsNotNull(cv2.GetMaterial());
				eq = true;
				for (int i_1 = 0; i_1 < kv1.GetMaterial().Length; i_1++)
				{
					eq = eq && cv2.GetMaterial()[i_1] == kv2.GetMaterial()[i_1];
				}
				NUnit.Framework.Assert.IsTrue(eq);
				// getKeyVersions() 2 versions
				IList<KeyProvider.KeyVersion> lkv2 = kp.GetKeyVersions("k1");
				NUnit.Framework.Assert.AreEqual(2, lkv2.Count);
				NUnit.Framework.Assert.AreEqual(kv1.GetVersionName(), lkv2[0].GetVersionName());
				NUnit.Framework.Assert.IsNotNull(lkv2[0].GetMaterial());
				NUnit.Framework.Assert.AreEqual(kv2.GetVersionName(), lkv2[1].GetVersionName());
				NUnit.Framework.Assert.IsNotNull(lkv2[1].GetMaterial());
				// getKeyMetadata() 2 version
				KeyProvider.Metadata m2 = kp.GetMetadata("k1");
				NUnit.Framework.Assert.AreEqual("AES/CTR/NoPadding", m2.GetCipher());
				NUnit.Framework.Assert.AreEqual("AES", m2.GetAlgorithm());
				NUnit.Framework.Assert.AreEqual(128, m2.GetBitLength());
				NUnit.Framework.Assert.AreEqual(2, m2.GetVersions());
				NUnit.Framework.Assert.IsNotNull(m2.GetCreated());
				NUnit.Framework.Assert.IsTrue(started.Before(m2.GetCreated()));
				// getKeys() 1 key
				IList<string> ks1 = kp.GetKeys();
				NUnit.Framework.Assert.AreEqual(1, ks1.Count);
				NUnit.Framework.Assert.AreEqual("k1", ks1[0]);
				// getKeysMetadata() 1 key 2 versions
				KeyProvider.Metadata[] kms1 = kp.GetKeysMetadata("k1");
				NUnit.Framework.Assert.AreEqual(1, kms1.Length);
				NUnit.Framework.Assert.AreEqual("AES/CTR/NoPadding", kms1[0].GetCipher());
				NUnit.Framework.Assert.AreEqual("AES", kms1[0].GetAlgorithm());
				NUnit.Framework.Assert.AreEqual(128, kms1[0].GetBitLength());
				NUnit.Framework.Assert.AreEqual(2, kms1[0].GetVersions());
				NUnit.Framework.Assert.IsNotNull(kms1[0].GetCreated());
				NUnit.Framework.Assert.IsTrue(started.Before(kms1[0].GetCreated()));
				// test generate and decryption of EEK
				KeyProvider.KeyVersion kv = kp.GetCurrentKey("k1");
				KeyProviderCryptoExtension kpExt = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
					(kp);
				KeyProviderCryptoExtension.EncryptedKeyVersion ek1 = kpExt.GenerateEncryptedKey(kv
					.GetName());
				NUnit.Framework.Assert.AreEqual(KeyProviderCryptoExtension.Eek, ek1.GetEncryptedKeyVersion
					().GetVersionName());
				NUnit.Framework.Assert.IsNotNull(ek1.GetEncryptedKeyVersion().GetMaterial());
				NUnit.Framework.Assert.AreEqual(kv.GetMaterial().Length, ek1.GetEncryptedKeyVersion
					().GetMaterial().Length);
				KeyProvider.KeyVersion k1 = kpExt.DecryptEncryptedKey(ek1);
				NUnit.Framework.Assert.AreEqual(KeyProviderCryptoExtension.Ek, k1.GetVersionName(
					));
				KeyProvider.KeyVersion k1a = kpExt.DecryptEncryptedKey(ek1);
				Assert.AssertArrayEquals(k1.GetMaterial(), k1a.GetMaterial());
				NUnit.Framework.Assert.AreEqual(kv.GetMaterial().Length, k1.GetMaterial().Length);
				KeyProviderCryptoExtension.EncryptedKeyVersion ek2 = kpExt.GenerateEncryptedKey(kv
					.GetName());
				KeyProvider.KeyVersion k2 = kpExt.DecryptEncryptedKey(ek2);
				bool isEq = true;
				for (int i_2 = 0; isEq && i_2 < ek2.GetEncryptedKeyVersion().GetMaterial().Length
					; i_2++)
				{
					isEq = k2.GetMaterial()[i_2] == k1.GetMaterial()[i_2];
				}
				NUnit.Framework.Assert.IsFalse(isEq);
				// deleteKey()
				kp.DeleteKey("k1");
				// Check decryption after Key deletion
				try
				{
					kpExt.DecryptEncryptedKey(ek1);
					NUnit.Framework.Assert.Fail("Should not be allowed !!");
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("'k1@1' not found"));
				}
				// getKey()
				NUnit.Framework.Assert.IsNull(kp.GetKeyVersion("k1"));
				// getKeyVersions()
				NUnit.Framework.Assert.IsNull(kp.GetKeyVersions("k1"));
				// getMetadata()
				NUnit.Framework.Assert.IsNull(kp.GetMetadata("k1"));
				// getKeys() empty
				NUnit.Framework.Assert.IsTrue(kp.GetKeys().IsEmpty());
				// getKeysMetadata() empty
				NUnit.Framework.Assert.AreEqual(0, kp.GetKeysMetadata().Length);
				// createKey() no description, no tags
				options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				KeyProvider.KeyVersion kVer2 = kp.CreateKey("k2", options);
				KeyProvider.Metadata meta = kp.GetMetadata("k2");
				NUnit.Framework.Assert.IsNull(meta.GetDescription());
				NUnit.Framework.Assert.AreEqual("k2", meta.GetAttributes()["key.acl.name"]);
				// test key ACL.. k2 is granted only MANAGEMENT Op access
				try
				{
					kpExt = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension(kp);
					kpExt.GenerateEncryptedKey(kVer2.GetName());
					NUnit.Framework.Assert.Fail("User should not be allowed to encrypt !!");
				}
				catch (Exception)
				{
				}
				// 
				// createKey() description, no tags
				options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				options.SetDescription("d");
				kp.CreateKey("k3", options);
				meta = kp.GetMetadata("k3");
				NUnit.Framework.Assert.AreEqual("d", meta.GetDescription());
				NUnit.Framework.Assert.AreEqual("k3", meta.GetAttributes()["key.acl.name"]);
				IDictionary<string, string> attributes = new Dictionary<string, string>();
				attributes["a"] = "A";
				// createKey() no description, tags
				options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				attributes["key.acl.name"] = "k4";
				options.SetAttributes(attributes);
				kp.CreateKey("k4", options);
				meta = kp.GetMetadata("k4");
				NUnit.Framework.Assert.IsNull(meta.GetDescription());
				NUnit.Framework.Assert.AreEqual(attributes, meta.GetAttributes());
				// createKey() description, tags
				options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				options.SetDescription("d");
				attributes["key.acl.name"] = "k5";
				options.SetAttributes(attributes);
				kp.CreateKey("k5", options);
				meta = kp.GetMetadata("k5");
				NUnit.Framework.Assert.AreEqual("d", meta.GetDescription());
				NUnit.Framework.Assert.AreEqual(attributes, meta.GetAttributes());
				// test delegation token retrieval
				KeyProviderDelegationTokenExtension kpdte = KeyProviderDelegationTokenExtension.CreateKeyProviderDelegationTokenExtension
					(kp);
				Credentials credentials = new Credentials();
				kpdte.AddDelegationTokens("foo", credentials);
				NUnit.Framework.Assert.AreEqual(1, credentials.GetAllTokens().Count);
				IPEndPoint kmsAddr = new IPEndPoint(this.GetKMSUrl().GetHost(), this.GetKMSUrl().
					Port);
				NUnit.Framework.Assert.AreEqual(new Text("kms-dt"), credentials.GetToken(SecurityUtil
					.BuildTokenService(kmsAddr)).GetKind());
				// test rollover draining
				KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
					(kp);
				options = new KeyProvider.Options(conf);
				options.SetCipher("AES/CTR/NoPadding");
				options.SetBitLength(128);
				kpce.CreateKey("k6", options);
				KeyProviderCryptoExtension.EncryptedKeyVersion ekv1 = kpce.GenerateEncryptedKey("k6"
					);
				kpce.RollNewVersion("k6");
				KeyProviderCryptoExtension.EncryptedKeyVersion ekv2 = kpce.GenerateEncryptedKey("k6"
					);
				Assert.AssertNotEquals(ekv1.GetEncryptionKeyVersionName(), ekv2.GetEncryptionKeyVersionName
					());
				return null;
			}

			private readonly TestKMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKeyACLs()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.type", "kerberos");
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), type.ToString());
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), "CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK"
				);
			conf.Set(KMSACLs.Type.Rollover.GetAclConfigKey(), "CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK"
				);
			conf.Set(KMSACLs.Type.GenerateEek.GetAclConfigKey(), "CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK,DECRYPT_EEK"
				);
			conf.Set(KMSACLs.Type.DecryptEek.GetAclConfigKey(), "CREATE,ROLLOVER,GET,SET_KEY_MATERIAL,GENERATE_EEK"
				);
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "test_key.MANAGEMENT", "CREATE");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "some_key.MANAGEMENT", "ROLLOVER");
			conf.Set(KMSConfiguration.WhitelistKeyAclPrefix + "MANAGEMENT", "DECRYPT_EEK");
			conf.Set(KMSConfiguration.WhitelistKeyAclPrefix + "ALL", "DECRYPT_EEK");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "all_access.ALL", "GENERATE_EEK");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "all_access.DECRYPT_EEK", "ROLLOVER"
				);
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "MANAGEMENT", "ROLLOVER");
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "GENERATE_EEK", "SOMEBODY");
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "ALL", "ROLLOVER");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_691(this));
			// Ignore
			// Ignore
			// Test whitelist key access..
			// DECRYPT_EEK is whitelisted for MANAGEMENT operations only
			// Ignore
			// Ignore
			// Ignore
			// Ignore
			// Ignore
			// Ignore
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "MANAGEMENT", string.Empty);
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "GENERATE_EEK", "*");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_895(this));
		}

		private sealed class _KMSCallable_691 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_691(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("CREATE", new _PrivilegedExceptionAction_699(this, uri, conf
					));
				this._enclosing.DoAs("DECRYPT_EEK", new _PrivilegedExceptionAction_739(this, uri, 
					conf));
				this._enclosing.DoAs("ROLLOVER", new _PrivilegedExceptionAction_772(this, uri, conf
					));
				this._enclosing.DoAs("GET", new _PrivilegedExceptionAction_816(this, uri, conf));
				KeyProviderCryptoExtension.EncryptedKeyVersion ekv = this._enclosing.DoAs("GENERATE_EEK"
					, new _PrivilegedExceptionAction_848(this, uri, conf));
				this._enclosing.DoAs("ROLLOVER", new _PrivilegedExceptionAction_873(this, uri, conf
					, ekv));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_699 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_699(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.Options options = new KeyProvider.Options(conf);
						IDictionary<string, string> attributes = options.GetAttributes();
						Dictionary<string, string> newAttribs = new Dictionary<string, string>(attributes
							);
						newAttribs["key.acl.name"] = "test_key";
						options.SetAttributes(newAttribs);
						KeyProvider.KeyVersion kv = kp.CreateKey("k0", options);
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
						KeyProvider.KeyVersion rollVersion = kp.RollNewVersion("k0");
						NUnit.Framework.Assert.IsNull(rollVersion.GetMaterial());
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						try
						{
							kpce.GenerateEncryptedKey("k0");
							NUnit.Framework.Assert.Fail("User [CREATE] should not be allowed to generate_eek on k0"
								);
						}
						catch (Exception)
						{
						}
						newAttribs = new Dictionary<string, string>(attributes);
						newAttribs["key.acl.name"] = "all_access";
						options.SetAttributes(newAttribs);
						try
						{
							kp.CreateKey("kx", options);
							NUnit.Framework.Assert.Fail("User [CREATE] should not be allowed to create kx");
						}
						catch (Exception)
						{
						}
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_739 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_739(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.Options options = new KeyProvider.Options(conf);
						IDictionary<string, string> attributes = options.GetAttributes();
						Dictionary<string, string> newAttribs = new Dictionary<string, string>(attributes
							);
						newAttribs["key.acl.name"] = "some_key";
						options.SetAttributes(newAttribs);
						KeyProvider.KeyVersion kv = kp.CreateKey("kk0", options);
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
						KeyProvider.KeyVersion rollVersion = kp.RollNewVersion("kk0");
						NUnit.Framework.Assert.IsNull(rollVersion.GetMaterial());
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						try
						{
							kpce.GenerateEncryptedKey("kk0");
							NUnit.Framework.Assert.Fail("User [DECRYPT_EEK] should not be allowed to generate_eek on kk0"
								);
						}
						catch (Exception)
						{
						}
						newAttribs = new Dictionary<string, string>(attributes);
						newAttribs["key.acl.name"] = "all_access";
						options.SetAttributes(newAttribs);
						kp.CreateKey("kkx", options);
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_772 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_772(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.Options options = new KeyProvider.Options(conf);
						IDictionary<string, string> attributes = options.GetAttributes();
						Dictionary<string, string> newAttribs = new Dictionary<string, string>(attributes
							);
						newAttribs["key.acl.name"] = "test_key2";
						options.SetAttributes(newAttribs);
						KeyProvider.KeyVersion kv = kp.CreateKey("k1", options);
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
						KeyProvider.KeyVersion rollVersion = kp.RollNewVersion("k1");
						NUnit.Framework.Assert.IsNull(rollVersion.GetMaterial());
						try
						{
							kp.RollNewVersion("k0");
							NUnit.Framework.Assert.Fail("User [ROLLOVER] should not be allowed to rollover k0"
								);
						}
						catch (Exception)
						{
						}
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						try
						{
							kpce.GenerateEncryptedKey("k1");
							NUnit.Framework.Assert.Fail("User [ROLLOVER] should not be allowed to generate_eek on k1"
								);
						}
						catch (Exception)
						{
						}
						newAttribs = new Dictionary<string, string>(attributes);
						newAttribs["key.acl.name"] = "all_access";
						options.SetAttributes(newAttribs);
						try
						{
							kp.CreateKey("kx", options);
							NUnit.Framework.Assert.Fail("User [ROLLOVER] should not be allowed to create kx");
						}
						catch (Exception)
						{
						}
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_816 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_816(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.Options options = new KeyProvider.Options(conf);
						IDictionary<string, string> attributes = options.GetAttributes();
						Dictionary<string, string> newAttribs = new Dictionary<string, string>(attributes
							);
						newAttribs["key.acl.name"] = "test_key";
						options.SetAttributes(newAttribs);
						try
						{
							kp.CreateKey("k2", options);
							NUnit.Framework.Assert.Fail("User [GET] should not be allowed to create key..");
						}
						catch (Exception)
						{
						}
						newAttribs = new Dictionary<string, string>(attributes);
						newAttribs["key.acl.name"] = "all_access";
						options.SetAttributes(newAttribs);
						try
						{
							kp.CreateKey("kx", options);
							NUnit.Framework.Assert.Fail("User [GET] should not be allowed to create kx");
						}
						catch (Exception)
						{
						}
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_848 : PrivilegedExceptionAction<KeyProviderCryptoExtension.EncryptedKeyVersion
				>
			{
				public _PrivilegedExceptionAction_848(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public KeyProviderCryptoExtension.EncryptedKeyVersion Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.Options options = new KeyProvider.Options(conf);
						IDictionary<string, string> attributes = options.GetAttributes();
						Dictionary<string, string> newAttribs = new Dictionary<string, string>(attributes
							);
						newAttribs["key.acl.name"] = "all_access";
						options.SetAttributes(newAttribs);
						kp.CreateKey("kx", options);
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						try
						{
							return kpce.GenerateEncryptedKey("kx");
						}
						catch (Exception)
						{
							NUnit.Framework.Assert.Fail("User [GENERATE_EEK] should be allowed to generate_eek on kx"
								);
						}
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_873 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_873(_KMSCallable_691 _enclosing, URI uri, Configuration
					 conf, KeyProviderCryptoExtension.EncryptedKeyVersion ekv)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.ekv = ekv;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						kpce.DecryptEncryptedKey(ekv);
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_691 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly KeyProviderCryptoExtension.EncryptedKeyVersion ekv;
			}

			private readonly TestKMS _enclosing;
		}

		private sealed class _KMSCallable_895 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_895(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("GENERATE_EEK", new _PrivilegedExceptionAction_903(this, uri
					, conf));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_903 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_903(_KMSCallable_895 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProviderCryptoExtension kpce = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						try
						{
							kpce.GenerateEncryptedKey("k1");
						}
						catch (Exception)
						{
							NUnit.Framework.Assert.Fail("User [GENERATE_EEK] should be allowed to generate_eek on k1"
								);
						}
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_895 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSRestartKerberosAuth()
		{
			DoKMSRestart(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSRestartSimpleAuth()
		{
			DoKMSRestart(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoKMSRestart(bool useKrb)
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			if (useKrb)
			{
				conf.Set("hadoop.kms.authentication.type", "kerberos");
			}
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), type.ToString());
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), KMSACLs.Type.Create.ToString() + 
				",SET_KEY_MATERIAL");
			conf.Set(KMSACLs.Type.Rollover.GetAclConfigKey(), KMSACLs.Type.Rollover.ToString(
				) + ",SET_KEY_MATERIAL");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k0.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k1.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k2.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k3.ALL", "*");
			WriteConf(testDir, conf);
			TestKMS.KMSCallable<KeyProvider> c = new _KMSCallable_967(this);
			KeyProvider retKp = RunServer(null, null, testDir, c);
			// Restart server (using the same port)
			RunServer(c.GetKMSUrl().Port, null, null, testDir, new _KMSCallable_994(this, retKp
				));
		}

		private sealed class _KMSCallable_967 : TestKMS.KMSCallable<KeyProvider>
		{
			public _KMSCallable_967(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override KeyProvider Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				KeyProvider kp = this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_976
					(this, uri, conf));
				return kp;
			}

			private sealed class _PrivilegedExceptionAction_976 : PrivilegedExceptionAction<KeyProvider
				>
			{
				public _PrivilegedExceptionAction_976(_KMSCallable_967 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public KeyProvider Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey("k1", new byte[16], new KeyProvider.Options(conf));
					return kp;
				}

				private readonly _KMSCallable_967 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		private sealed class _KMSCallable_994 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_994(TestKMS _enclosing, KeyProvider retKp)
			{
				this._enclosing = _enclosing;
				this.retKp = retKp;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1000(retKp
					, conf));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1000 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1000(KeyProvider retKp, Configuration conf)
				{
					this.retKp = retKp;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					retKp.CreateKey("k2", new byte[16], new KeyProvider.Options(conf));
					retKp.CreateKey("k3", new byte[16], new KeyProvider.Options(conf));
					return null;
				}

				private readonly KeyProvider retKp;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;

			private readonly KeyProvider retKp;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSAuthFailureRetry()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			conf.Set("hadoop.kms.authentication.token.validity", "1");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), type.ToString());
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), KMSACLs.Type.Create.ToString() + 
				",SET_KEY_MATERIAL");
			conf.Set(KMSACLs.Type.Rollover.GetAclConfigKey(), KMSACLs.Type.Rollover.ToString(
				) + ",SET_KEY_MATERIAL");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k0.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k1.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k2.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k3.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k4.ALL", "*");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1046(this));
			// This happens before rollover
			// Atleast 2 rollovers.. so should induce signer Exception
			// Test retry count
			RunServer(null, null, testDir, new _KMSCallable_1076(this));
		}

		private sealed class _KMSCallable_1046 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1046(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1053(this
					, uri, conf));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1053 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1053(_KMSCallable_1046 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey("k0", new byte[16], new KeyProvider.Options(conf));
					kp.CreateKey("k1", new byte[16], new KeyProvider.Options(conf));
					Sharpen.Thread.Sleep(3500);
					kp.CreateKey("k2", new byte[16], new KeyProvider.Options(conf));
					return null;
				}

				private readonly _KMSCallable_1046 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		private sealed class _KMSCallable_1076 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1076(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				conf.SetInt(KMSClientProvider.AuthRetry, 0);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1084(this
					, uri, conf));
				// Atleast 2 rollovers.. so should induce signer Exception
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1084 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1084(_KMSCallable_1076 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey("k3", new byte[16], new KeyProvider.Options(conf));
					Sharpen.Thread.Sleep(3500);
					try
					{
						kp.CreateKey("k4", new byte[16], new KeyProvider.Options(conf));
						NUnit.Framework.Assert.Fail("This should not succeed..");
					}
					catch (IOException e)
					{
						NUnit.Framework.Assert.IsTrue("HTTP exception must be a 401 : " + e.Message, e.Message
							.Contains("401"));
					}
					return null;
				}

				private readonly _KMSCallable_1076 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestACLs()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.type", "kerberos");
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), type.ToString());
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), KMSACLs.Type.Create.ToString() + 
				",SET_KEY_MATERIAL");
			conf.Set(KMSACLs.Type.Rollover.GetAclConfigKey(), KMSACLs.Type.Rollover.ToString(
				) + ",SET_KEY_MATERIAL");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k0.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k1.ALL", "*");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1136(this, testDir));
		}

		private sealed class _KMSCallable_1136 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1136(TestKMS _enclosing, FilePath testDir)
			{
				this._enclosing = _enclosing;
				this.testDir = testDir;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				//nothing allowed
				this._enclosing.DoAs("client", new _PrivilegedExceptionAction_1144(this, uri, conf
					));
				//NOP
				//NOP
				//NOP
				//NOP
				//NOP
				//NOP
				// we are using JavaKeyStoreProvider for testing, so we know how
				// the keyversion is created.
				//NOP
				//NOP
				//NOP
				//NOP
				this._enclosing.DoAs("CREATE", new _PrivilegedExceptionAction_1235(this, uri, conf
					));
				this._enclosing.DoAs("DELETE", new _PrivilegedExceptionAction_1250(this, uri, conf
					));
				this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1263(this
					, uri, conf));
				this._enclosing.DoAs("ROLLOVER", new _PrivilegedExceptionAction_1278(this, uri, conf
					));
				this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1292(this
					, uri, conf));
				KeyProvider.KeyVersion currKv = this._enclosing.DoAs("GET", new _PrivilegedExceptionAction_1308
					(this, uri, conf));
				KeyProviderCryptoExtension.EncryptedKeyVersion encKv = this._enclosing.DoAs("GENERATE_EEK"
					, new _PrivilegedExceptionAction_1325(this, uri, conf, currKv));
				this._enclosing.DoAs("DECRYPT_EEK", new _PrivilegedExceptionAction_1342(this, uri
					, conf, encKv));
				this._enclosing.DoAs("GET_KEYS", new _PrivilegedExceptionAction_1357(this, uri, conf
					));
				this._enclosing.DoAs("GET_METADATA", new _PrivilegedExceptionAction_1370(this, uri
					, conf));
				//stop the reloader, to avoid running while we are writing the new file
				KMSWebApp.GetACLs().StopReloader();
				// test ACL reloading
				Sharpen.Thread.Sleep(10);
				// to ensure the ACLs file modifiedTime is newer
				conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), "foo");
				TestKMS.WriteConf(testDir, conf);
				Sharpen.Thread.Sleep(1000);
				KMSWebApp.GetACLs().Run();
				// forcing a reload by hand.
				// should not be able to create a key now
				this._enclosing.DoAs("CREATE", new _PrivilegedExceptionAction_1396(this, uri, conf
					));
				//NOP
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1144 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1144(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						kp.CreateKey("k", new KeyProvider.Options(conf));
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.CreateKey("k", new byte[16], new KeyProvider.Options(conf));
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.RollNewVersion("k");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.RollNewVersion("k", new byte[16]);
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetKeys();
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetKeysMetadata("k");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetKeyVersion("k@0");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetCurrentKey("k");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetMetadata("k");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					try
					{
						kp.GetKeyVersions("k");
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1235 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1235(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.KeyVersion kv = kp.CreateKey("k0", new KeyProvider.Options(conf));
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1250 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1250(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						kp.DeleteKey("k0");
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1263 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1263(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.KeyVersion kv = kp.CreateKey("k1", new byte[16], new KeyProvider.Options
							(conf));
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1278 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1278(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.KeyVersion kv = kp.RollNewVersion("k1");
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1292 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1292(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProvider.KeyVersion kv = kp.RollNewVersion("k1", new byte[16]);
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1308 : PrivilegedExceptionAction<
				KeyProvider.KeyVersion>
			{
				public _PrivilegedExceptionAction_1308(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public KeyProvider.KeyVersion Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						kp.GetKeyVersion("k1@0");
						KeyProvider.KeyVersion kv = kp.GetCurrentKey("k1");
						return kv;
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.ToString());
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1325 : PrivilegedExceptionAction<
				KeyProviderCryptoExtension.EncryptedKeyVersion>
			{
				public _PrivilegedExceptionAction_1325(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf, KeyProvider.KeyVersion currKv)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.currKv = currKv;
				}

				/// <exception cref="System.Exception"/>
				public KeyProviderCryptoExtension.EncryptedKeyVersion Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						KeyProviderCryptoExtension.EncryptedKeyVersion ek1 = kpCE.GenerateEncryptedKey(currKv
							.GetName());
						return ek1;
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.ToString());
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly KeyProvider.KeyVersion currKv;
			}

			private sealed class _PrivilegedExceptionAction_1342 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1342(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf, KeyProviderCryptoExtension.EncryptedKeyVersion encKv)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.encKv = encKv;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						KeyProviderCryptoExtension kpCE = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
							(kp);
						kpCE.DecryptEncryptedKey(encKv);
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly KeyProviderCryptoExtension.EncryptedKeyVersion encKv;
			}

			private sealed class _PrivilegedExceptionAction_1357 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1357(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						kp.GetKeys();
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1370 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1370(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					try
					{
						kp.GetMetadata("k1");
						kp.GetKeysMetadata("k1");
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1396 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1396(_KMSCallable_1136 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("k2", new KeyProvider.Options(conf));
						NUnit.Framework.Assert.Fail();
					}
					catch (AuthorizationException)
					{
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1136 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;

			private readonly FilePath testDir;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSBlackList()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.type", "kerberos");
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), " ");
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), "client,hdfs,otheradmin");
			conf.Set(KMSACLs.Type.GenerateEek.GetAclConfigKey(), "client,hdfs,otheradmin");
			conf.Set(KMSACLs.Type.DecryptEek.GetAclConfigKey(), "client,hdfs,otheradmin");
			conf.Set(KMSACLs.Type.DecryptEek.GetBlacklistConfigKey(), "hdfs,otheradmin");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "ck0.ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "ck1.ALL", "*");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1444(this));
		}

		private sealed class _KMSCallable_1444 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1444(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("client", new _PrivilegedExceptionAction_1451(this, uri, conf
					));
				this._enclosing.DoAs("hdfs", new _PrivilegedExceptionAction_1469(this, uri, conf)
					);
				this._enclosing.DoAs("otheradmin", new _PrivilegedExceptionAction_1486(this, uri, 
					conf));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1451 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1451(_KMSCallable_1444 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("ck0", new KeyProvider.Options(conf));
						KeyProviderCryptoExtension.EncryptedKeyVersion eek = ((KeyProviderCryptoExtension.CryptoExtension
							)kp).GenerateEncryptedKey("ck0");
						((KeyProviderCryptoExtension.CryptoExtension)kp).DecryptEncryptedKey(eek);
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1444 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1469 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1469(_KMSCallable_1444 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("ck1", new KeyProvider.Options(conf));
						KeyProviderCryptoExtension.EncryptedKeyVersion eek = ((KeyProviderCryptoExtension.CryptoExtension
							)kp).GenerateEncryptedKey("ck1");
						((KeyProviderCryptoExtension.CryptoExtension)kp).DecryptEncryptedKey(eek);
						NUnit.Framework.Assert.Fail("admin user must not be allowed to decrypt !!");
					}
					catch (Exception)
					{
					}
					return null;
				}

				private readonly _KMSCallable_1444 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1486 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1486(_KMSCallable_1444 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("ck2", new KeyProvider.Options(conf));
						KeyProviderCryptoExtension.EncryptedKeyVersion eek = ((KeyProviderCryptoExtension.CryptoExtension
							)kp).GenerateEncryptedKey("ck2");
						((KeyProviderCryptoExtension.CryptoExtension)kp).DecryptEncryptedKey(eek);
						NUnit.Framework.Assert.Fail("admin user must not be allowed to decrypt !!");
					}
					catch (Exception)
					{
					}
					return null;
				}

				private readonly _KMSCallable_1444 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestServicePrincipalACLs()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.type", "kerberos");
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			foreach (KMSACLs.Type type in KMSACLs.Type.Values())
			{
				conf.Set(type.GetAclConfigKey(), " ");
			}
			conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), "client");
			conf.Set(KMSConfiguration.DefaultKeyAclPrefix + "MANAGEMENT", "client,client/host"
				);
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1528(this));
		}

		private sealed class _KMSCallable_1528 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1528(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				conf.SetInt(KeyProvider.DefaultBitlengthName, 64);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				this._enclosing.DoAs("client", new _PrivilegedExceptionAction_1536(this, uri, conf
					));
				this._enclosing.DoAs("client/host", new _PrivilegedExceptionAction_1551(this, uri
					, conf));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1536 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1536(_KMSCallable_1528 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("ck0", new KeyProvider.Options(conf));
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1528 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private sealed class _PrivilegedExceptionAction_1551 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1551(_KMSCallable_1528 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					try
					{
						KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
						KeyProvider.KeyVersion kv = kp.CreateKey("ck1", new KeyProvider.Options(conf));
						NUnit.Framework.Assert.IsNull(kv.GetMaterial());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(ex.Message);
					}
					return null;
				}

				private readonly _KMSCallable_1528 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		/// <summary>Test the configurable timeout in the KMSClientProvider.</summary>
		/// <remarks>
		/// Test the configurable timeout in the KMSClientProvider.  Open up a
		/// socket, but don't accept connections for it.  This leads to a timeout
		/// when the KMS client attempts to connect.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSTimeout()
		{
			FilePath confDir = GetTestDir();
			Configuration conf = CreateBaseKMSConf(confDir);
			conf.SetInt(KMSClientProvider.TimeoutAttr, 1);
			WriteConf(confDir, conf);
			Socket sock;
			int port;
			try
			{
				sock = Sharpen.Extensions.CreateServerSocket(0, 50, Sharpen.Extensions.GetAddressByName
					("localhost"));
				port = sock.GetLocalPort();
			}
			catch (Exception)
			{
				/* Problem creating socket?  Just bail. */
				return;
			}
			Uri url = new Uri("http://localhost:" + port + "/kms");
			URI uri = CreateKMSUri(url);
			bool caughtTimeout = false;
			try
			{
				KeyProvider kp = CreateProvider(uri, conf);
				kp.GetKeys();
			}
			catch (SocketTimeoutException)
			{
				caughtTimeout = true;
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("Caught unexpected exception" + e.ToString(), false
					);
			}
			caughtTimeout = false;
			try
			{
				KeyProvider kp = CreateProvider(uri, conf);
				KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension(kp).GenerateEncryptedKey
					("a");
			}
			catch (SocketTimeoutException)
			{
				caughtTimeout = true;
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("Caught unexpected exception" + e.ToString(), false
					);
			}
			caughtTimeout = false;
			try
			{
				KeyProvider kp = CreateProvider(uri, conf);
				KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension(kp).DecryptEncryptedKey
					(new KMSClientProvider.KMSEncryptedKeyVersion("a", "a", new byte[] { 1, 2 }, "EEK"
					, new byte[] { 1, 2 }));
			}
			catch (SocketTimeoutException)
			{
				caughtTimeout = true;
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("Caught unexpected exception" + e.ToString(), false
					);
			}
			NUnit.Framework.Assert.IsTrue(caughtTimeout);
			sock.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAccess()
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			conf.Set("hadoop.kms.authentication.type", "kerberos");
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			string keyA = "key_a";
			string keyD = "key_d";
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + keyA + ".ALL", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + keyD + ".ALL", "*");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1655(this, keyA, keyD));
		}

		private sealed class _KMSCallable_1655 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1655(TestKMS _enclosing, string keyA, string keyD)
			{
				this._enclosing = _enclosing;
				this.keyA = keyA;
				this.keyD = keyD;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 64);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				Credentials credentials = new Credentials();
				UserGroupInformation nonKerberosUgi = UserGroupInformation.GetCurrentUser();
				try
				{
					KeyProvider kp = this._enclosing.CreateProvider(uri, conf);
					kp.CreateKey(keyA, new KeyProvider.Options(conf));
				}
				catch (IOException ex)
				{
					System.Console.Out.WriteLine(ex.Message);
				}
				this._enclosing.DoAs("client", new _PrivilegedExceptionAction_1672(this, uri, conf
					, credentials));
				nonKerberosUgi.AddCredentials(credentials);
				try
				{
					KeyProvider kp = this._enclosing.CreateProvider(uri, conf);
					kp.CreateKey(keyA, new KeyProvider.Options(conf));
				}
				catch (IOException ex)
				{
					System.Console.Out.WriteLine(ex.Message);
				}
				nonKerberosUgi.DoAs(new _PrivilegedExceptionAction_1693(this, uri, conf, keyD));
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1672 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1672(_KMSCallable_1655 _enclosing, URI uri, Configuration
					 conf, Credentials credentials)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.credentials = credentials;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					KeyProviderDelegationTokenExtension kpdte = KeyProviderDelegationTokenExtension.CreateKeyProviderDelegationTokenExtension
						(kp);
					kpdte.AddDelegationTokens("foo", credentials);
					return null;
				}

				private readonly _KMSCallable_1655 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly Credentials credentials;
			}

			private sealed class _PrivilegedExceptionAction_1693 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1693(_KMSCallable_1655 _enclosing, URI uri, Configuration
					 conf, string keyD)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.keyD = keyD;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey(keyD, new KeyProvider.Options(conf));
					return null;
				}

				private readonly _KMSCallable_1655 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly string keyD;
			}

			private readonly TestKMS _enclosing;

			private readonly string keyA;

			private readonly string keyD;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSWithZKSigner()
		{
			DoKMSWithZK(true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSWithZKDTSM()
		{
			DoKMSWithZK(false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKMSWithZKSignerAndDTSM()
		{
			DoKMSWithZK(true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoKMSWithZK(bool zkDTSM, bool zkSigner)
		{
			TestingServer zkServer = null;
			try
			{
				zkServer = new TestingServer();
				zkServer.Start();
				Configuration conf = new Configuration();
				conf.Set("hadoop.security.authentication", "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				FilePath testDir = GetTestDir();
				conf = CreateBaseKMSConf(testDir);
				conf.Set("hadoop.kms.authentication.type", "kerberos");
				conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
				conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
				conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
				if (zkSigner)
				{
					conf.Set("hadoop.kms.authentication.signer.secret.provider", "zookeeper");
					conf.Set("hadoop.kms.authentication.signer.secret.provider.zookeeper.path", "/testKMSWithZKDTSM"
						);
					conf.Set("hadoop.kms.authentication.signer.secret.provider.zookeeper.connection.string"
						, zkServer.GetConnectString());
				}
				if (zkDTSM)
				{
					conf.Set("hadoop.kms.authentication.zk-dt-secret-manager.enable", "true");
				}
				if (zkDTSM && !zkSigner)
				{
					conf.Set("hadoop.kms.authentication.zk-dt-secret-manager.zkConnectionString", zkServer
						.GetConnectString());
					conf.Set("hadoop.kms.authentication.zk-dt-secret-manager.znodeWorkingPath", "testZKPath"
						);
					conf.Set("hadoop.kms.authentication.zk-dt-secret-manager.zkAuthType", "none");
				}
				foreach (KMSACLs.Type type in KMSACLs.Type.Values())
				{
					conf.Set(type.GetAclConfigKey(), type.ToString());
				}
				conf.Set(KMSACLs.Type.Create.GetAclConfigKey(), KMSACLs.Type.Create.ToString() + 
					",SET_KEY_MATERIAL");
				conf.Set(KMSACLs.Type.Rollover.GetAclConfigKey(), KMSACLs.Type.Rollover.ToString(
					) + ",SET_KEY_MATERIAL");
				conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k0.ALL", "*");
				conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k1.ALL", "*");
				conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k2.ALL", "*");
				conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "k3.ALL", "*");
				WriteConf(testDir, conf);
				TestKMS.KMSCallable<KeyProvider> c = new _KMSCallable_1770(this);
				RunServer(null, null, testDir, c);
			}
			finally
			{
				if (zkServer != null)
				{
					zkServer.Stop();
					zkServer.Close();
				}
			}
		}

		private sealed class _KMSCallable_1770 : TestKMS.KMSCallable<KeyProvider>
		{
			public _KMSCallable_1770(TestKMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override KeyProvider Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 128);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				KeyProvider kp = this._enclosing.DoAs("SET_KEY_MATERIAL", new _PrivilegedExceptionAction_1779
					(this, uri, conf));
				return kp;
			}

			private sealed class _PrivilegedExceptionAction_1779 : PrivilegedExceptionAction<
				KeyProvider>
			{
				public _PrivilegedExceptionAction_1779(_KMSCallable_1770 _enclosing, URI uri, Configuration
					 conf)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public KeyProvider Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey("k1", new byte[16], new KeyProvider.Options(conf));
					kp.CreateKey("k2", new byte[16], new KeyProvider.Options(conf));
					kp.CreateKey("k3", new byte[16], new KeyProvider.Options(conf));
					return kp;
				}

				private readonly _KMSCallable_1770 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProxyUserKerb()
		{
			DoProxyUserTest(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProxyUserSimple()
		{
			DoProxyUserTest(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoProxyUserTest(bool kerberos)
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			if (kerberos)
			{
				conf.Set("hadoop.kms.authentication.type", "kerberos");
			}
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			conf.Set("hadoop.kms.proxyuser.client.users", "foo,bar");
			conf.Set("hadoop.kms.proxyuser.client.hosts", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kaa.ALL", "client");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kbb.ALL", "foo");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kcc.ALL", "foo1");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kdd.ALL", "bar");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1839(this, kerberos));
		}

		private sealed class _KMSCallable_1839 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1839(TestKMS _enclosing, bool kerberos)
			{
				this._enclosing = _enclosing;
				this.kerberos = kerberos;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 64);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				UserGroupInformation proxyUgi = null;
				if (kerberos)
				{
					// proxyuser client using kerberos credentials
					proxyUgi = UserGroupInformation.LoginUserFromKeytabAndReturnUGI("client", TestKMS
						.keytab.GetAbsolutePath());
				}
				else
				{
					proxyUgi = UserGroupInformation.CreateRemoteUser("client");
				}
				UserGroupInformation clientUgi = proxyUgi;
				clientUgi.DoAs(new _PrivilegedExceptionAction_1856(this, uri, conf, clientUgi));
				// authorized proxyuser
				// unauthorized proxyuser
				// OK
				// authorized proxyuser
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1856 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1856(_KMSCallable_1839 _enclosing, URI uri, Configuration
					 conf, UserGroupInformation clientUgi)
				{
					this._enclosing = _enclosing;
					this.uri = uri;
					this.conf = conf;
					this.clientUgi = clientUgi;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					KeyProvider kp = this._enclosing._enclosing.CreateProvider(uri, conf);
					kp.CreateKey("kaa", new KeyProvider.Options(conf));
					UserGroupInformation fooUgi = UserGroupInformation.CreateProxyUser("foo", clientUgi
						);
					fooUgi.DoAs(new _PrivilegedExceptionAction_1865(kp, conf));
					UserGroupInformation foo1Ugi = UserGroupInformation.CreateProxyUser("foo1", clientUgi
						);
					foo1Ugi.DoAs(new _PrivilegedExceptionAction_1877(kp, conf));
					UserGroupInformation barUgi = UserGroupInformation.CreateProxyUser("bar", clientUgi
						);
					barUgi.DoAs(new _PrivilegedExceptionAction_1895(kp, conf));
					return null;
				}

				private sealed class _PrivilegedExceptionAction_1865 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1865(KeyProvider kp, Configuration conf)
					{
						this.kp = kp;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						NUnit.Framework.Assert.IsNotNull(kp.CreateKey("kbb", new KeyProvider.Options(conf
							)));
						return null;
					}

					private readonly KeyProvider kp;

					private readonly Configuration conf;
				}

				private sealed class _PrivilegedExceptionAction_1877 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1877(KeyProvider kp, Configuration conf)
					{
						this.kp = kp;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						try
						{
							kp.CreateKey("kcc", new KeyProvider.Options(conf));
							NUnit.Framework.Assert.Fail();
						}
						catch (AuthorizationException)
						{
						}
						catch (Exception ex)
						{
							NUnit.Framework.Assert.Fail(ex.Message);
						}
						return null;
					}

					private readonly KeyProvider kp;

					private readonly Configuration conf;
				}

				private sealed class _PrivilegedExceptionAction_1895 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1895(KeyProvider kp, Configuration conf)
					{
						this.kp = kp;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						NUnit.Framework.Assert.IsNotNull(kp.CreateKey("kdd", new KeyProvider.Options(conf
							)));
						return null;
					}

					private readonly KeyProvider kp;

					private readonly Configuration conf;
				}

				private readonly _KMSCallable_1839 _enclosing;

				private readonly URI uri;

				private readonly Configuration conf;

				private readonly UserGroupInformation clientUgi;
			}

			private readonly TestKMS _enclosing;

			private readonly bool kerberos;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHDFSProxyUserKerb()
		{
			DoWebHDFSProxyUserTest(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHDFSProxyUserSimple()
		{
			DoWebHDFSProxyUserTest(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoWebHDFSProxyUserTest(bool kerberos)
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = GetTestDir();
			conf = CreateBaseKMSConf(testDir);
			if (kerberos)
			{
				conf.Set("hadoop.kms.authentication.type", "kerberos");
			}
			conf.Set("hadoop.kms.authentication.kerberos.keytab", keytab.GetAbsolutePath());
			conf.Set("hadoop.kms.authentication.kerberos.principal", "HTTP/localhost");
			conf.Set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
			conf.Set("hadoop.security.kms.client.timeout", "300");
			conf.Set("hadoop.kms.proxyuser.client.users", "foo,bar");
			conf.Set("hadoop.kms.proxyuser.client.hosts", "*");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kaa.ALL", "foo");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kbb.ALL", "foo1");
			conf.Set(KeyAuthorizationKeyProvider.KeyAcl + "kcc.ALL", "bar");
			WriteConf(testDir, conf);
			RunServer(null, null, testDir, new _KMSCallable_1944(this, kerberos));
		}

		private sealed class _KMSCallable_1944 : TestKMS.KMSCallable<Void>
		{
			public _KMSCallable_1944(TestKMS _enclosing, bool kerberos)
			{
				this._enclosing = _enclosing;
				this.kerberos = kerberos;
			}

			/// <exception cref="System.Exception"/>
			public override Void Call()
			{
				Configuration conf = new Configuration();
				conf.SetInt(KeyProvider.DefaultBitlengthName, 64);
				URI uri = TestKMS.CreateKMSUri(this.GetKMSUrl());
				UserGroupInformation proxyUgi = null;
				if (kerberos)
				{
					// proxyuser client using kerberos credentials
					proxyUgi = UserGroupInformation.LoginUserFromKeytabAndReturnUGI("client", TestKMS
						.keytab.GetAbsolutePath());
				}
				else
				{
					proxyUgi = UserGroupInformation.CreateRemoteUser("client");
				}
				UserGroupInformation clientUgi = proxyUgi;
				clientUgi.DoAs(new _PrivilegedExceptionAction_1961(this, clientUgi, uri, conf));
				// authorized proxyuser
				// unauthorized proxyuser
				// authorized proxyuser
				return null;
			}

			private sealed class _PrivilegedExceptionAction_1961 : PrivilegedExceptionAction<
				Void>
			{
				public _PrivilegedExceptionAction_1961(_KMSCallable_1944 _enclosing, UserGroupInformation
					 clientUgi, URI uri, Configuration conf)
				{
					this._enclosing = _enclosing;
					this.clientUgi = clientUgi;
					this.uri = uri;
					this.conf = conf;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					UserGroupInformation fooUgi = UserGroupInformation.CreateProxyUser("foo", clientUgi
						);
					fooUgi.DoAs(new _PrivilegedExceptionAction_1968(this, uri, conf));
					UserGroupInformation foo1Ugi = UserGroupInformation.CreateProxyUser("foo1", clientUgi
						);
					foo1Ugi.DoAs(new _PrivilegedExceptionAction_1981(this, uri, conf));
					UserGroupInformation barUgi = UserGroupInformation.CreateProxyUser("bar", clientUgi
						);
					barUgi.DoAs(new _PrivilegedExceptionAction_1998(this, uri, conf));
					return null;
				}

				private sealed class _PrivilegedExceptionAction_1968 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1968(_PrivilegedExceptionAction_1961 _enclosing
						, URI uri, Configuration conf)
					{
						this._enclosing = _enclosing;
						this.uri = uri;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						KeyProvider kp = this._enclosing._enclosing._enclosing.CreateProvider(uri, conf);
						NUnit.Framework.Assert.IsNotNull(kp.CreateKey("kaa", new KeyProvider.Options(conf
							)));
						return null;
					}

					private readonly _PrivilegedExceptionAction_1961 _enclosing;

					private readonly URI uri;

					private readonly Configuration conf;
				}

				private sealed class _PrivilegedExceptionAction_1981 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1981(_PrivilegedExceptionAction_1961 _enclosing
						, URI uri, Configuration conf)
					{
						this._enclosing = _enclosing;
						this.uri = uri;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						try
						{
							KeyProvider kp = this._enclosing._enclosing._enclosing.CreateProvider(uri, conf);
							kp.CreateKey("kbb", new KeyProvider.Options(conf));
							NUnit.Framework.Assert.Fail();
						}
						catch (Exception ex)
						{
							NUnit.Framework.Assert.IsTrue(ex.Message, ex.Message.Contains("Forbidden"));
						}
						return null;
					}

					private readonly _PrivilegedExceptionAction_1961 _enclosing;

					private readonly URI uri;

					private readonly Configuration conf;
				}

				private sealed class _PrivilegedExceptionAction_1998 : PrivilegedExceptionAction<
					Void>
				{
					public _PrivilegedExceptionAction_1998(_PrivilegedExceptionAction_1961 _enclosing
						, URI uri, Configuration conf)
					{
						this._enclosing = _enclosing;
						this.uri = uri;
						this.conf = conf;
					}

					/// <exception cref="System.Exception"/>
					public Void Run()
					{
						KeyProvider kp = this._enclosing._enclosing._enclosing.CreateProvider(uri, conf);
						NUnit.Framework.Assert.IsNotNull(kp.CreateKey("kcc", new KeyProvider.Options(conf
							)));
						return null;
					}

					private readonly _PrivilegedExceptionAction_1961 _enclosing;

					private readonly URI uri;

					private readonly Configuration conf;
				}

				private readonly _KMSCallable_1944 _enclosing;

				private readonly UserGroupInformation clientUgi;

				private readonly URI uri;

				private readonly Configuration conf;
			}

			private readonly TestKMS _enclosing;

			private readonly bool kerberos;
		}
	}
}
