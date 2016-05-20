using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestKeyProviderFactory
	{
		private org.apache.hadoop.fs.FileSystemTestHelper fsHelper;

		private java.io.File testRootDir;

		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			fsHelper = new org.apache.hadoop.fs.FileSystemTestHelper();
			string testRoot = fsHelper.getTestRootDir();
			testRootDir = new java.io.File(testRoot).getAbsoluteFile();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFactory()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string userUri = org.apache.hadoop.crypto.key.UserProvider.SCHEME_NAME + ":///";
			org.apache.hadoop.fs.Path jksPath = new org.apache.hadoop.fs.Path(testRootDir.ToString
				(), "test.jks");
			string jksUri = org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME + "://file"
				 + jksPath.toUri().ToString();
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, userUri
				 + "," + jksUri);
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider> providers
				 = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf);
			NUnit.Framework.Assert.AreEqual(2, providers.Count);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.key.UserProvider
				)), Sharpen.Runtime.getClassForObject(providers[0]));
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.key.JavaKeyStoreProvider
				)), Sharpen.Runtime.getClassForObject(providers[1]));
			NUnit.Framework.Assert.AreEqual(userUri, providers[0].ToString());
			NUnit.Framework.Assert.AreEqual(jksUri, providers[1].ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFactoryErrors()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, "unknown:///"
				);
			try
			{
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider> providers
					 = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf);
				NUnit.Framework.Assert.IsTrue("should throw!", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("No KeyProviderFactory for unknown:/// in " + org.apache.hadoop.crypto.key.KeyProviderFactory
					.KEY_PROVIDER_PATH, e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUriErrors()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, "unkn@own:/x/y"
				);
			try
			{
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider> providers
					 = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf);
				NUnit.Framework.Assert.IsTrue("should throw!", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Bad configuration of " + org.apache.hadoop.crypto.key.KeyProviderFactory
					.KEY_PROVIDER_PATH + " at unkn@own:/x/y", e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void checkSpecificProvider(org.apache.hadoop.conf.Configuration conf
			, string ourUrl)
		{
			org.apache.hadoop.crypto.key.KeyProvider provider = org.apache.hadoop.crypto.key.KeyProviderFactory
				.getProviders(conf)[0];
			byte[] key1 = new byte[16];
			byte[] key2 = new byte[16];
			byte[] key3 = new byte[16];
			for (int i = 0; i < key1.Length; ++i)
			{
				key1[i] = unchecked((byte)i);
				key2[i] = unchecked((byte)(i * 2));
				key3[i] = unchecked((byte)(i * 3));
			}
			// ensure that we get nulls when the key isn't there
			NUnit.Framework.Assert.AreEqual(null, provider.getKeyVersion("no-such-key"));
			NUnit.Framework.Assert.AreEqual(null, provider.getMetadata("key"));
			// create a new key
			try
			{
				provider.createKey("key3", key3, org.apache.hadoop.crypto.key.KeyProvider.options
					(conf));
			}
			catch (System.Exception e)
			{
				Sharpen.Runtime.printStackTrace(e);
				throw;
			}
			// check the metadata for key3
			org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = provider.getMetadata("key3"
				);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_CIPHER
				, meta.getCipher());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_BITLENGTH
				, meta.getBitLength());
			NUnit.Framework.Assert.AreEqual(1, meta.getVersions());
			// make sure we get back the right key
			NUnit.Framework.Assert.assertArrayEquals(key3, provider.getCurrentKey("key3").getMaterial
				());
			NUnit.Framework.Assert.AreEqual("key3@0", provider.getCurrentKey("key3").getVersionName
				());
			// try recreating key3
			try
			{
				provider.createKey("key3", key3, org.apache.hadoop.crypto.key.KeyProvider.options
					(conf));
				NUnit.Framework.Assert.IsTrue("should throw", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Key key3 already exists in " + ourUrl, e.Message
					);
			}
			provider.deleteKey("key3");
			try
			{
				provider.deleteKey("key3");
				NUnit.Framework.Assert.IsTrue("should throw", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Key key3 does not exist in " + ourUrl, e.Message
					);
			}
			provider.createKey("key3", key3, org.apache.hadoop.crypto.key.KeyProvider.options
				(conf));
			try
			{
				provider.createKey("key4", key3, org.apache.hadoop.crypto.key.KeyProvider.options
					(conf).setBitLength(8));
				NUnit.Framework.Assert.IsTrue("should throw", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Wrong key length. Required 8, but got 128", e.Message
					);
			}
			provider.createKey("key4", new byte[] { 1 }, org.apache.hadoop.crypto.key.KeyProvider
				.options(conf).setBitLength(8));
			provider.rollNewVersion("key4", new byte[] { 2 });
			meta = provider.getMetadata("key4");
			NUnit.Framework.Assert.AreEqual(2, meta.getVersions());
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 2 }, provider.getCurrentKey
				("key4").getMaterial());
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 1 }, provider.getKeyVersion
				("key4@0").getMaterial());
			NUnit.Framework.Assert.AreEqual("key4@1", provider.getCurrentKey("key4").getVersionName
				());
			try
			{
				provider.rollNewVersion("key4", key1);
				NUnit.Framework.Assert.IsTrue("should throw", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Wrong key length. Required 8, but got 128", e.Message
					);
			}
			try
			{
				provider.rollNewVersion("no-such-key", key1);
				NUnit.Framework.Assert.IsTrue("should throw", false);
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Key no-such-key not found", e.Message);
			}
			provider.flush();
			// get a new instance of the provider to ensure it was saved correctly
			provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 2 }, provider.getCurrentKey
				("key4").getMaterial());
			NUnit.Framework.Assert.assertArrayEquals(key3, provider.getCurrentKey("key3").getMaterial
				());
			NUnit.Framework.Assert.AreEqual("key3@0", provider.getCurrentKey("key3").getVersionName
				());
			System.Collections.Generic.IList<string> keys = provider.getKeys();
			NUnit.Framework.Assert.IsTrue("Keys should have been returned.", keys.Count == 2);
			NUnit.Framework.Assert.IsTrue("Returned Keys should have included key3.", keys.contains
				("key3"));
			NUnit.Framework.Assert.IsTrue("Returned Keys should have included key4.", keys.contains
				("key4"));
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> kvl = provider.getKeyVersions("key3");
			NUnit.Framework.Assert.IsTrue("KeyVersions should have been returned for key3.", 
				kvl.Count == 1);
			NUnit.Framework.Assert.IsTrue("KeyVersions should have included key3@0.", kvl[0].
				getVersionName().Equals("key3@0"));
			NUnit.Framework.Assert.assertArrayEquals(key3, kvl[0].getMaterial());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUserProvider()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string ourUrl = org.apache.hadoop.crypto.key.UserProvider.SCHEME_NAME + ":///";
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
				);
			checkSpecificProvider(conf, ourUrl);
			// see if the credentials are actually in the UGI
			org.apache.hadoop.security.Credentials credentials = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser().getCredentials();
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 1 }, credentials.getSecretKey
				(new org.apache.hadoop.io.Text("key4@0")));
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 2 }, credentials.getSecretKey
				(new org.apache.hadoop.io.Text("key4@1")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testJksProvider()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.Path jksPath = new org.apache.hadoop.fs.Path(testRootDir.ToString
				(), "test.jks");
			string ourUrl = org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME + "://file"
				 + jksPath.toUri();
			java.io.File file = new java.io.File(testRootDir, "test.jks");
			file.delete();
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
				);
			checkSpecificProvider(conf, ourUrl);
			// START : Test flush error by failure injection
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
				.Replace(org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME, org.apache.hadoop.crypto.key.FailureInjectingJavaKeyStoreProvider
				.SCHEME_NAME));
			// get a new instance of the provider to ensure it was saved correctly
			org.apache.hadoop.crypto.key.KeyProvider provider = org.apache.hadoop.crypto.key.KeyProviderFactory
				.getProviders(conf)[0];
			// inject failure during keystore write
			org.apache.hadoop.crypto.key.FailureInjectingJavaKeyStoreProvider fProvider = (org.apache.hadoop.crypto.key.FailureInjectingJavaKeyStoreProvider
				)provider;
			fProvider.setWriteFail(true);
			provider.createKey("key5", new byte[] { 1 }, org.apache.hadoop.crypto.key.KeyProvider
				.options(conf).setBitLength(8));
			NUnit.Framework.Assert.IsNotNull(provider.getCurrentKey("key5"));
			try
			{
				provider.flush();
				NUnit.Framework.Assert.Fail("Should not succeed");
			}
			catch (System.Exception)
			{
			}
			// Ignore
			// SHould be reset to pre-flush state
			NUnit.Framework.Assert.IsNull(provider.getCurrentKey("key5"));
			// Un-inject last failure and
			// inject failure during keystore backup
			fProvider.setWriteFail(false);
			fProvider.setBackupFail(true);
			provider.createKey("key6", new byte[] { 1 }, org.apache.hadoop.crypto.key.KeyProvider
				.options(conf).setBitLength(8));
			NUnit.Framework.Assert.IsNotNull(provider.getCurrentKey("key6"));
			try
			{
				provider.flush();
				NUnit.Framework.Assert.Fail("Should not succeed");
			}
			catch (System.Exception)
			{
			}
			// Ignore
			// SHould be reset to pre-flush state
			NUnit.Framework.Assert.IsNull(provider.getCurrentKey("key6"));
			// END : Test flush error by failure injection
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
				.Replace(org.apache.hadoop.crypto.key.FailureInjectingJavaKeyStoreProvider.SCHEME_NAME
				, org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME));
			org.apache.hadoop.fs.Path path = org.apache.hadoop.security.ProviderUtils.unnestUri
				(new java.net.URI(ourUrl));
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
			org.apache.hadoop.fs.FileStatus s = fs.getFileStatus(path);
			NUnit.Framework.Assert.IsTrue(s.getPermission().ToString().Equals("rwx------"));
			NUnit.Framework.Assert.IsTrue(file + " should exist", file.isFile());
			// Corrupt file and Check if JKS can reload from _OLD file
			java.io.File oldFile = new java.io.File(file.getPath() + "_OLD");
			file.renameTo(oldFile);
			file.delete();
			file.createNewFile();
			NUnit.Framework.Assert.IsTrue(oldFile.exists());
			provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
			NUnit.Framework.Assert.IsTrue(file.exists());
			NUnit.Framework.Assert.IsTrue(oldFile + "should be deleted", !oldFile.exists());
			verifyAfterReload(file, provider);
			NUnit.Framework.Assert.IsTrue(!oldFile.exists());
			// _NEW and current file should not exist together
			java.io.File newFile = new java.io.File(file.getPath() + "_NEW");
			newFile.createNewFile();
			try
			{
				provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.Fail("_NEW and current file should not exist together !!");
			}
			catch (System.Exception)
			{
			}
			finally
			{
				// Ignore
				if (newFile.exists())
				{
					newFile.delete();
				}
			}
			// Load from _NEW file
			file.renameTo(newFile);
			file.delete();
			try
			{
				provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.IsFalse(newFile.exists());
				NUnit.Framework.Assert.IsFalse(oldFile.exists());
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("JKS should load from _NEW file !!");
			}
			// Ignore
			verifyAfterReload(file, provider);
			// _NEW exists but corrupt.. must load from _OLD
			newFile.createNewFile();
			file.renameTo(oldFile);
			file.delete();
			try
			{
				provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.IsFalse(newFile.exists());
				NUnit.Framework.Assert.IsFalse(oldFile.exists());
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("JKS should load from _OLD file !!");
			}
			finally
			{
				// Ignore
				if (newFile.exists())
				{
					newFile.delete();
				}
			}
			verifyAfterReload(file, provider);
			// check permission retention after explicit change
			fs.setPermission(path, new org.apache.hadoop.fs.permission.FsPermission("777"));
			checkPermissionRetention(conf, ourUrl, path);
			// Check that an uppercase keyname results in an error
			provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
			try
			{
				provider.createKey("UPPERCASE", org.apache.hadoop.crypto.key.KeyProvider.options(
					conf));
				NUnit.Framework.Assert.Fail("Expected failure on creating key name with uppercase "
					 + "characters");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Uppercase key names"
					, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyAfterReload(java.io.File file, org.apache.hadoop.crypto.key.KeyProvider
			 provider)
		{
			System.Collections.Generic.IList<string> existingKeys = provider.getKeys();
			NUnit.Framework.Assert.IsTrue(existingKeys.contains("key4"));
			NUnit.Framework.Assert.IsTrue(existingKeys.contains("key3"));
			NUnit.Framework.Assert.IsTrue(file.exists());
		}

		/// <exception cref="System.Exception"/>
		public virtual void checkPermissionRetention(org.apache.hadoop.conf.Configuration
			 conf, string ourUrl, org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.crypto.key.KeyProvider provider = org.apache.hadoop.crypto.key.KeyProviderFactory
				.getProviders(conf)[0];
			// let's add a new key and flush and check that permissions are still set to 777
			byte[] key = new byte[16];
			for (int i = 0; i < key.Length; ++i)
			{
				key[i] = unchecked((byte)i);
			}
			// create a new key
			try
			{
				provider.createKey("key5", key, org.apache.hadoop.crypto.key.KeyProvider.options(
					conf));
			}
			catch (System.Exception e)
			{
				Sharpen.Runtime.printStackTrace(e);
				throw;
			}
			provider.flush();
			// get a new instance of the provider to ensure it was saved correctly
			provider = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
			NUnit.Framework.Assert.assertArrayEquals(key, provider.getCurrentKey("key5").getMaterial
				());
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
			org.apache.hadoop.fs.FileStatus s = fs.getFileStatus(path);
			NUnit.Framework.Assert.IsTrue("Permissions should have been retained from the preexisting keystore."
				, s.getPermission().ToString().Equals("rwxrwxrwx"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testJksProviderPasswordViaConfig()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.Path jksPath = new org.apache.hadoop.fs.Path(testRootDir.ToString
				(), "test.jks");
			string ourUrl = org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME + "://file"
				 + jksPath.toUri();
			java.io.File file = new java.io.File(testRootDir, "test.jks");
			file.delete();
			try
			{
				conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
					);
				conf.set(org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY
					, "javakeystoreprovider.password");
				org.apache.hadoop.crypto.key.KeyProvider provider = org.apache.hadoop.crypto.key.KeyProviderFactory
					.getProviders(conf)[0];
				provider.createKey("key3", new byte[16], org.apache.hadoop.crypto.key.KeyProvider
					.options(conf));
				provider.flush();
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("could not create keystore with password file");
			}
			org.apache.hadoop.crypto.key.KeyProvider provider_1 = org.apache.hadoop.crypto.key.KeyProviderFactory
				.getProviders(conf)[0];
			NUnit.Framework.Assert.IsNotNull(provider_1.getCurrentKey("key3"));
			try
			{
				conf.set(org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY
					, "bar");
				org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.Fail("using non existing password file, it should fail");
			}
			catch (System.IO.IOException)
			{
			}
			//NOP
			try
			{
				conf.set(org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY
					, "core-site.xml");
				org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.Fail("using different password file, it should fail");
			}
			catch (System.IO.IOException)
			{
			}
			//NOP
			try
			{
				conf.unset(org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY
					);
				org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(conf)[0];
				NUnit.Framework.Assert.Fail("No password file property, env not set, it should fail"
					);
			}
			catch (System.IO.IOException)
			{
			}
		}

		//NOP
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetProviderViaURI()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			org.apache.hadoop.fs.Path jksPath = new org.apache.hadoop.fs.Path(testRootDir.ToString
				(), "test.jks");
			java.net.URI uri = new java.net.URI(org.apache.hadoop.crypto.key.JavaKeyStoreProvider
				.SCHEME_NAME + "://file" + jksPath.toUri());
			org.apache.hadoop.crypto.key.KeyProvider kp = org.apache.hadoop.crypto.key.KeyProviderFactory
				.get(uri, conf);
			NUnit.Framework.Assert.IsNotNull(kp);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.key.JavaKeyStoreProvider
				)), Sharpen.Runtime.getClassForObject(kp));
			uri = new java.net.URI("foo://bar");
			kp = org.apache.hadoop.crypto.key.KeyProviderFactory.get(uri, conf);
			NUnit.Framework.Assert.IsNull(kp);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testJksProviderWithKeytoolKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			string keystoreDirAbsolutePath = conf.getResource("hdfs7067.keystore").getPath();
			string ourUrl = org.apache.hadoop.crypto.key.JavaKeyStoreProvider.SCHEME_NAME + "://file@/"
				 + keystoreDirAbsolutePath;
			conf.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl
				);
			org.apache.hadoop.crypto.key.KeyProvider provider = org.apache.hadoop.crypto.key.KeyProviderFactory
				.getProviders(conf)[0];
			// Sanity check that we are using the right keystore
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersion = provider.getKeyVersion
				("testkey5@0");
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersionWrongKeyNameFormat = 
					provider.getKeyVersion("testkey2");
				NUnit.Framework.Assert.Fail("should have thrown an exception");
			}
			catch (System.IO.IOException e)
			{
				// No version in key path testkey2/
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("No version in key path"
					, e);
			}
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersionCurrentKeyNotWrongKeyNameFormat
					 = provider.getCurrentKey("testkey5@0");
				NUnit.Framework.Assert.Fail("should have thrown an exception getting testkey5@0");
			}
			catch (System.IO.IOException e)
			{
				// javax.crypto.spec.SecretKeySpec cannot be cast to
				// org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("other non-Hadoop method"
					, e);
			}
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersionCurrentKeyNotReally
					 = provider.getCurrentKey("testkey2");
				NUnit.Framework.Assert.Fail("should have thrown an exception getting testkey2");
			}
			catch (System.IO.IOException e)
			{
				// javax.crypto.spec.SecretKeySpec cannot be cast to
				// org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("other non-Hadoop method"
					, e);
			}
		}
	}
}
