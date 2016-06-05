using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Security.Alias
{
	public class TestCredentialProviderFactory
	{
		private static char[] chars = new char[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'
			, 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A'
			, 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'P', 'Q', 'R', 'S', 'T'
			, 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7', '8', '9' };

		private static readonly FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "creds");

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFactory()
		{
			Configuration conf = new Configuration();
			string userUri = UserProvider.SchemeName + ":///";
			Path jksPath = new Path(tmpDir.ToString(), "test.jks");
			string jksUri = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, userUri + "," + jksUri
				);
			IList<CredentialProvider> providers = CredentialProviderFactory.GetProviders(conf
				);
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
			conf.Set(CredentialProviderFactory.CredentialProviderPath, "unknown:///");
			try
			{
				IList<CredentialProvider> providers = CredentialProviderFactory.GetProviders(conf
					);
				Assert.True("should throw!", false);
			}
			catch (IOException e)
			{
				Assert.Equal("No CredentialProviderFactory for unknown:/// in "
					 + CredentialProviderFactory.CredentialProviderPath, e.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUriErrors()
		{
			Configuration conf = new Configuration();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, "unkn@own:/x/y");
			try
			{
				IList<CredentialProvider> providers = CredentialProviderFactory.GetProviders(conf
					);
				Assert.True("should throw!", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Bad configuration of " + CredentialProviderFactory
					.CredentialProviderPath + " at unkn@own:/x/y", e.Message);
			}
		}

		private static char[] GeneratePassword(int length)
		{
			StringBuilder sb = new StringBuilder();
			Random r = new Random();
			for (int i = 0; i < length; i++)
			{
				sb.Append(chars[r.Next(chars.Length)]);
			}
			return sb.ToString().ToCharArray();
		}

		/// <exception cref="System.Exception"/>
		internal static void CheckSpecificProvider(Configuration conf, string ourUrl)
		{
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			char[] passwd = GeneratePassword(16);
			// ensure that we get nulls when the key isn't there
			Assert.Equal(null, provider.GetCredentialEntry("no-such-key"));
			Assert.Equal(null, provider.GetCredentialEntry("key"));
			// create a new key
			try
			{
				provider.CreateCredentialEntry("pass", passwd);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				throw;
			}
			// make sure we get back the right key
			Assert.AssertArrayEquals(passwd, provider.GetCredentialEntry("pass").GetCredential
				());
			// try recreating pass
			try
			{
				provider.CreateCredentialEntry("pass", passwd);
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Credential pass already exists in " + ourUrl, e.
					Message);
			}
			provider.DeleteCredentialEntry("pass");
			try
			{
				provider.DeleteCredentialEntry("pass");
				Assert.True("should throw", false);
			}
			catch (IOException e)
			{
				Assert.Equal("Credential pass does not exist in " + ourUrl, e.
					Message);
			}
			char[] passTwo = new char[] { '1', '2', '3' };
			provider.CreateCredentialEntry("pass", passwd);
			provider.CreateCredentialEntry("pass2", passTwo);
			Assert.AssertArrayEquals(passTwo, provider.GetCredentialEntry("pass2").GetCredential
				());
			// write them to disk so that configuration.getPassword will find them
			provider.Flush();
			// configuration.getPassword should get this from provider
			Assert.AssertArrayEquals(passTwo, conf.GetPassword("pass2"));
			// configuration.getPassword should get this from config
			conf.Set("onetwothree", "123");
			Assert.AssertArrayEquals(passTwo, conf.GetPassword("onetwothree"));
			// configuration.getPassword should NOT get this from config since
			// we are disabling the fallback to clear text config
			conf.Set(CredentialProvider.ClearTextFallback, "false");
			Assert.AssertArrayEquals(null, conf.GetPassword("onetwothree"));
			// get a new instance of the provider to ensure it was saved correctly
			provider = CredentialProviderFactory.GetProviders(conf)[0];
			Assert.True(provider != null);
			Assert.AssertArrayEquals(new char[] { '1', '2', '3' }, provider.GetCredentialEntry
				("pass2").GetCredential());
			Assert.AssertArrayEquals(passwd, provider.GetCredentialEntry("pass").GetCredential
				());
			IList<string> creds = provider.GetAliases();
			Assert.True("Credentials should have been returned.", creds.Count
				 == 2);
			Assert.True("Returned Credentials should have included pass.", 
				creds.Contains("pass"));
			Assert.True("Returned Credentials should have included pass2.", 
				creds.Contains("pass2"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUserProvider()
		{
			Configuration conf = new Configuration();
			string ourUrl = UserProvider.SchemeName + ":///";
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CheckSpecificProvider(conf, ourUrl);
			// see if the credentials are actually in the UGI
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			Assert.AssertArrayEquals(new byte[] { (byte)('1'), (byte)('2'), (byte)('3') }, credentials
				.GetSecretKey(new Org.Apache.Hadoop.IO.Text("pass2")));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestJksProvider()
		{
			Configuration conf = new Configuration();
			Path jksPath = new Path(tmpDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(tmpDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CheckSpecificProvider(conf, ourUrl);
			Path path = ProviderUtils.UnnestUri(new URI(ourUrl));
			FileSystem fs = path.GetFileSystem(conf);
			FileStatus s = fs.GetFileStatus(path);
			Assert.True(s.GetPermission().ToString().Equals("rwx------"));
			Assert.True(file + " should exist", file.IsFile());
			// check permission retention after explicit change
			fs.SetPermission(path, new FsPermission("777"));
			CheckPermissionRetention(conf, ourUrl, path);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLocalJksProvider()
		{
			Configuration conf = new Configuration();
			Path jksPath = new Path(tmpDir.ToString(), "test.jks");
			string ourUrl = LocalJavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri(
				);
			FilePath file = new FilePath(tmpDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CheckSpecificProvider(conf, ourUrl);
			Path path = ProviderUtils.UnnestUri(new URI(ourUrl));
			FileSystem fs = path.GetFileSystem(conf);
			FileStatus s = fs.GetFileStatus(path);
			Assert.True("Unexpected permissions: " + s.GetPermission().ToString
				(), s.GetPermission().ToString().Equals("rwx------"));
			Assert.True(file + " should exist", file.IsFile());
			// check permission retention after explicit change
			fs.SetPermission(path, new FsPermission("777"));
			CheckPermissionRetention(conf, ourUrl, path);
		}

		/// <exception cref="System.Exception"/>
		public virtual void CheckPermissionRetention(Configuration conf, string ourUrl, Path
			 path)
		{
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			// let's add a new credential and flush and check that permissions are still set to 777
			char[] cred = new char[32];
			for (int i = 0; i < cred.Length; ++i)
			{
				cred[i] = (char)i;
			}
			// create a new key
			try
			{
				provider.CreateCredentialEntry("key5", cred);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				throw;
			}
			provider.Flush();
			// get a new instance of the provider to ensure it was saved correctly
			provider = CredentialProviderFactory.GetProviders(conf)[0];
			Assert.AssertArrayEquals(cred, provider.GetCredentialEntry("key5").GetCredential(
				));
			FileSystem fs = path.GetFileSystem(conf);
			FileStatus s = fs.GetFileStatus(path);
			Assert.True("Permissions should have been retained from the preexisting "
				 + "keystore.", s.GetPermission().ToString().Equals("rwxrwxrwx"));
		}
	}
}
