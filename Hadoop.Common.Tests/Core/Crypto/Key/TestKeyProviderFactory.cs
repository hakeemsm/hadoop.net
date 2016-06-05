using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;


namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyProviderFactory
	{
		private FileSystemTestHelper fsHelper;

		private FilePath testRootDir;

		[SetUp]
		public virtual void Setup()
		{
			fsHelper = new FileSystemTestHelper();
			string testRoot = fsHelper.GetTestRootDir();
			testRootDir = new FilePath(testRoot).GetAbsoluteFile();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFactory()
		{
			Configuration conf = new Configuration();
			string userUri = UserProvider.SchemeName + ":///";
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			string jksUri = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri().ToString
				();
			conf.Set(KeyProviderFactory.KeyProviderPath, userUri + "," + jksUri);
			IList<KeyProvider> providers = KeyProviderFactory.GetProviders(conf);
			Assert.Equal(2, providers.Count);
			Assert.Equal(typeof(UserProvider), providers[0].GetType());
			Assert.Equal(typeof(JavaKeyStoreProvider), providers[1].GetType
				());
			Assert.Equal(userUri, providers[0].ToString());
			Assert.Equal(jksUri, providers[1].ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFactoryErrors()
		{
			Configuration conf = new Configuration();
			conf.Set(KeyProviderFactory.KeyProviderPath, "unknown:///");
			try
			{
				IList<KeyProvider> providers = KeyProviderFactory.GetProviders(conf);
				Assert.True("should throw!", false);
			}
			catch (IOException e)
			{
				Assert.Equal("No KeyProviderFactory for unknown:/// in " + KeyProviderFactory
					.KeyProviderPath, e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUriErrors()
		{
			Configuration conf = new Configuration();
			conf.Set(KeyProviderFactory.KeyProviderPath, "unkn@own:/x/y");
			try
			{
				IList<KeyProvider> providers = KeyProviderFactory.GetProviders(conf);
				Assert.True("should throw!", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Bad configuration of " + KeyProviderFactory.KeyProviderPath
					 + " at unkn@own:/x/y", e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void CheckSpecificProvider(Configuration conf, string ourUrl)
		{
			KeyProvider provider = KeyProviderFactory.GetProviders(conf)[0];
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
			Assert.Equal(null, provider.GetKeyVersion("no-such-key"));
			Assert.Equal(null, provider.GetMetadata("key"));
			// create a new key
			try
			{
				provider.CreateKey("key3", key3, KeyProvider.Options(conf));
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				throw;
			}
			// check the metadata for key3
			KeyProvider.Metadata meta = provider.GetMetadata("key3");
			Assert.Equal(KeyProvider.DefaultCipher, meta.GetCipher());
			Assert.Equal(KeyProvider.DefaultBitlength, meta.GetBitLength()
				);
			Assert.Equal(1, meta.GetVersions());
			// make sure we get back the right key
			Assert.AssertArrayEquals(key3, provider.GetCurrentKey("key3").GetMaterial());
			Assert.Equal("key3@0", provider.GetCurrentKey("key3").GetVersionName
				());
			// try recreating key3
			try
			{
				provider.CreateKey("key3", key3, KeyProvider.Options(conf));
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Key key3 already exists in " + ourUrl, e.Message
					);
			}
			provider.DeleteKey("key3");
			try
			{
				provider.DeleteKey("key3");
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Key key3 does not exist in " + ourUrl, e.Message
					);
			}
			provider.CreateKey("key3", key3, KeyProvider.Options(conf));
			try
			{
				provider.CreateKey("key4", key3, KeyProvider.Options(conf).SetBitLength(8));
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Wrong key length. Required 8, but got 128", e.Message
					);
			}
			provider.CreateKey("key4", new byte[] { 1 }, KeyProvider.Options(conf).SetBitLength
				(8));
			provider.RollNewVersion("key4", new byte[] { 2 });
			meta = provider.GetMetadata("key4");
			Assert.Equal(2, meta.GetVersions());
			Assert.AssertArrayEquals(new byte[] { 2 }, provider.GetCurrentKey("key4").GetMaterial
				());
			Assert.AssertArrayEquals(new byte[] { 1 }, provider.GetKeyVersion("key4@0").GetMaterial
				());
			Assert.Equal("key4@1", provider.GetCurrentKey("key4").GetVersionName
				());
			try
			{
				provider.RollNewVersion("key4", key1);
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Wrong key length. Required 8, but got 128", e.Message
					);
			}
			try
			{
				provider.RollNewVersion("no-such-key", key1);
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Key no-such-key not found", e.Message);
			}
			provider.Flush();
			// get a new instance of the provider to ensure it was saved correctly
			provider = KeyProviderFactory.GetProviders(conf)[0];
			Assert.AssertArrayEquals(new byte[] { 2 }, provider.GetCurrentKey("key4").GetMaterial
				());
			Assert.AssertArrayEquals(key3, provider.GetCurrentKey("key3").GetMaterial());
			Assert.Equal("key3@0", provider.GetCurrentKey("key3").GetVersionName
				());
			IList<string> keys = provider.GetKeys();
			Assert.True("Keys should have been returned.", keys.Count == 2);
			Assert.True("Returned Keys should have included key3.", keys.Contains
				("key3"));
			Assert.True("Returned Keys should have included key4.", keys.Contains
				("key4"));
			IList<KeyProvider.KeyVersion> kvl = provider.GetKeyVersions("key3");
			Assert.True("KeyVersions should have been returned for key3.", 
				kvl.Count == 1);
			Assert.True("KeyVersions should have included key3@0.", kvl[0].
				GetVersionName().Equals("key3@0"));
			Assert.AssertArrayEquals(key3, kvl[0].GetMaterial());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUserProvider()
		{
			Configuration conf = new Configuration();
			string ourUrl = UserProvider.SchemeName + ":///";
			conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl);
			CheckSpecificProvider(conf, ourUrl);
			// see if the credentials are actually in the UGI
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			Assert.AssertArrayEquals(new byte[] { 1 }, credentials.GetSecretKey(new Text("key4@0"
				)));
			Assert.AssertArrayEquals(new byte[] { 2 }, credentials.GetSecretKey(new Text("key4@1"
				)));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestJksProvider()
		{
			Configuration conf = new Configuration();
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testRootDir, "test.jks");
			file.Delete();
			conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl);
			CheckSpecificProvider(conf, ourUrl);
			// START : Test flush error by failure injection
			conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl.Replace(JavaKeyStoreProvider.
				SchemeName, FailureInjectingJavaKeyStoreProvider.SchemeName));
			// get a new instance of the provider to ensure it was saved correctly
			KeyProvider provider = KeyProviderFactory.GetProviders(conf)[0];
			// inject failure during keystore write
			FailureInjectingJavaKeyStoreProvider fProvider = (FailureInjectingJavaKeyStoreProvider
				)provider;
			fProvider.SetWriteFail(true);
			provider.CreateKey("key5", new byte[] { 1 }, KeyProvider.Options(conf).SetBitLength
				(8));
			NUnit.Framework.Assert.IsNotNull(provider.GetCurrentKey("key5"));
			try
			{
				provider.Flush();
				NUnit.Framework.Assert.Fail("Should not succeed");
			}
			catch (Exception)
			{
			}
			// Ignore
			// SHould be reset to pre-flush state
			NUnit.Framework.Assert.IsNull(provider.GetCurrentKey("key5"));
			// Un-inject last failure and
			// inject failure during keystore backup
			fProvider.SetWriteFail(false);
			fProvider.SetBackupFail(true);
			provider.CreateKey("key6", new byte[] { 1 }, KeyProvider.Options(conf).SetBitLength
				(8));
			NUnit.Framework.Assert.IsNotNull(provider.GetCurrentKey("key6"));
			try
			{
				provider.Flush();
				NUnit.Framework.Assert.Fail("Should not succeed");
			}
			catch (Exception)
			{
			}
			// Ignore
			// SHould be reset to pre-flush state
			NUnit.Framework.Assert.IsNull(provider.GetCurrentKey("key6"));
			// END : Test flush error by failure injection
			conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl.Replace(FailureInjectingJavaKeyStoreProvider
				.SchemeName, JavaKeyStoreProvider.SchemeName));
			Path path = ProviderUtils.UnnestUri(new URI(ourUrl));
			FileSystem fs = path.GetFileSystem(conf);
			FileStatus s = fs.GetFileStatus(path);
			Assert.True(s.GetPermission().ToString().Equals("rwx------"));
			Assert.True(file + " should exist", file.IsFile());
			// Corrupt file and Check if JKS can reload from _OLD file
			FilePath oldFile = new FilePath(file.GetPath() + "_OLD");
			file.RenameTo(oldFile);
			file.Delete();
			file.CreateNewFile();
			Assert.True(oldFile.Exists());
			provider = KeyProviderFactory.GetProviders(conf)[0];
			Assert.True(file.Exists());
			Assert.True(oldFile + "should be deleted", !oldFile.Exists());
			VerifyAfterReload(file, provider);
			Assert.True(!oldFile.Exists());
			// _NEW and current file should not exist together
			FilePath newFile = new FilePath(file.GetPath() + "_NEW");
			newFile.CreateNewFile();
			try
			{
				provider = KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.Fail("_NEW and current file should not exist together !!");
			}
			catch (Exception)
			{
			}
			finally
			{
				// Ignore
				if (newFile.Exists())
				{
					newFile.Delete();
				}
			}
			// Load from _NEW file
			file.RenameTo(newFile);
			file.Delete();
			try
			{
				provider = KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.IsFalse(newFile.Exists());
				NUnit.Framework.Assert.IsFalse(oldFile.Exists());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("JKS should load from _NEW file !!");
			}
			// Ignore
			VerifyAfterReload(file, provider);
			// _NEW exists but corrupt.. must load from _OLD
			newFile.CreateNewFile();
			file.RenameTo(oldFile);
			file.Delete();
			try
			{
				provider = KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.IsFalse(newFile.Exists());
				NUnit.Framework.Assert.IsFalse(oldFile.Exists());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("JKS should load from _OLD file !!");
			}
			finally
			{
				// Ignore
				if (newFile.Exists())
				{
					newFile.Delete();
				}
			}
			VerifyAfterReload(file, provider);
			// check permission retention after explicit change
			fs.SetPermission(path, new FsPermission("777"));
			CheckPermissionRetention(conf, ourUrl, path);
			// Check that an uppercase keyname results in an error
			provider = KeyProviderFactory.GetProviders(conf)[0];
			try
			{
				provider.CreateKey("UPPERCASE", KeyProvider.Options(conf));
				NUnit.Framework.Assert.Fail("Expected failure on creating key name with uppercase "
					 + "characters");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Uppercase key names", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyAfterReload(FilePath file, KeyProvider provider)
		{
			IList<string> existingKeys = provider.GetKeys();
			Assert.True(existingKeys.Contains("key4"));
			Assert.True(existingKeys.Contains("key3"));
			Assert.True(file.Exists());
		}

		/// <exception cref="System.Exception"/>
		public virtual void CheckPermissionRetention(Configuration conf, string ourUrl, Path
			 path)
		{
			KeyProvider provider = KeyProviderFactory.GetProviders(conf)[0];
			// let's add a new key and flush and check that permissions are still set to 777
			byte[] key = new byte[16];
			for (int i = 0; i < key.Length; ++i)
			{
				key[i] = unchecked((byte)i);
			}
			// create a new key
			try
			{
				provider.CreateKey("key5", key, KeyProvider.Options(conf));
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				throw;
			}
			provider.Flush();
			// get a new instance of the provider to ensure it was saved correctly
			provider = KeyProviderFactory.GetProviders(conf)[0];
			Assert.AssertArrayEquals(key, provider.GetCurrentKey("key5").GetMaterial());
			FileSystem fs = path.GetFileSystem(conf);
			FileStatus s = fs.GetFileStatus(path);
			Assert.True("Permissions should have been retained from the preexisting keystore."
				, s.GetPermission().ToString().Equals("rwxrwxrwx"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestJksProviderPasswordViaConfig()
		{
			Configuration conf = new Configuration();
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testRootDir, "test.jks");
			file.Delete();
			try
			{
				conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl);
				conf.Set(JavaKeyStoreProvider.KeystorePasswordFileKey, "javakeystoreprovider.password"
					);
				KeyProvider provider = KeyProviderFactory.GetProviders(conf)[0];
				provider.CreateKey("key3", new byte[16], KeyProvider.Options(conf));
				provider.Flush();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("could not create keystore with password file");
			}
			KeyProvider provider_1 = KeyProviderFactory.GetProviders(conf)[0];
			NUnit.Framework.Assert.IsNotNull(provider_1.GetCurrentKey("key3"));
			try
			{
				conf.Set(JavaKeyStoreProvider.KeystorePasswordFileKey, "bar");
				KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.Fail("using non existing password file, it should fail");
			}
			catch (IOException)
			{
			}
			//NOP
			try
			{
				conf.Set(JavaKeyStoreProvider.KeystorePasswordFileKey, "core-site.xml");
				KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.Fail("using different password file, it should fail");
			}
			catch (IOException)
			{
			}
			//NOP
			try
			{
				conf.Unset(JavaKeyStoreProvider.KeystorePasswordFileKey);
				KeyProviderFactory.GetProviders(conf)[0];
				NUnit.Framework.Assert.Fail("No password file property, env not set, it should fail"
					);
			}
			catch (IOException)
			{
			}
		}

		//NOP
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetProviderViaURI()
		{
			Configuration conf = new Configuration(false);
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			URI uri = new URI(JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri());
			KeyProvider kp = KeyProviderFactory.Get(uri, conf);
			NUnit.Framework.Assert.IsNotNull(kp);
			Assert.Equal(typeof(JavaKeyStoreProvider), kp.GetType());
			uri = new URI("foo://bar");
			kp = KeyProviderFactory.Get(uri, conf);
			NUnit.Framework.Assert.IsNull(kp);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestJksProviderWithKeytoolKeys()
		{
			Configuration conf = new Configuration();
			string keystoreDirAbsolutePath = conf.GetResource("hdfs7067.keystore").AbsolutePath;
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file@/" + keystoreDirAbsolutePath;
			conf.Set(KeyProviderFactory.KeyProviderPath, ourUrl);
			KeyProvider provider = KeyProviderFactory.GetProviders(conf)[0];
			// Sanity check that we are using the right keystore
			KeyProvider.KeyVersion keyVersion = provider.GetKeyVersion("testkey5@0");
			try
			{
				KeyProvider.KeyVersion keyVersionWrongKeyNameFormat = provider.GetKeyVersion("testkey2"
					);
				NUnit.Framework.Assert.Fail("should have thrown an exception");
			}
			catch (IOException e)
			{
				// No version in key path testkey2/
				GenericTestUtils.AssertExceptionContains("No version in key path", e);
			}
			try
			{
				KeyProvider.KeyVersion keyVersionCurrentKeyNotWrongKeyNameFormat = provider.GetCurrentKey
					("testkey5@0");
				NUnit.Framework.Assert.Fail("should have thrown an exception getting testkey5@0");
			}
			catch (IOException e)
			{
				// javax.crypto.spec.SecretKeySpec cannot be cast to
				// org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata
				GenericTestUtils.AssertExceptionContains("other non-Hadoop method", e);
			}
			try
			{
				KeyProvider.KeyVersion keyVersionCurrentKeyNotReally = provider.GetCurrentKey("testkey2"
					);
				NUnit.Framework.Assert.Fail("should have thrown an exception getting testkey2");
			}
			catch (IOException e)
			{
				// javax.crypto.spec.SecretKeySpec cannot be cast to
				// org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata
				GenericTestUtils.AssertExceptionContains("other non-Hadoop method", e);
			}
		}
	}
}
