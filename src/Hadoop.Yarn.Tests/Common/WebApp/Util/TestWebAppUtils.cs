using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Alias;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Util
{
	public class TestWebAppUtils
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPassword()
		{
			Configuration conf = ProvisionCredentialsForSSL();
			// use WebAppUtils as would be used by loadSslConfiguration
			NUnit.Framework.Assert.AreEqual("keypass", WebAppUtils.GetPassword(conf, WebAppUtils
				.WebAppKeyPasswordKey));
			NUnit.Framework.Assert.AreEqual("storepass", WebAppUtils.GetPassword(conf, WebAppUtils
				.WebAppKeystorePasswordKey));
			NUnit.Framework.Assert.AreEqual("trustpass", WebAppUtils.GetPassword(conf, WebAppUtils
				.WebAppTruststorePasswordKey));
			// let's make sure that a password that doesn't exist returns null
			NUnit.Framework.Assert.AreEqual(null, WebAppUtils.GetPassword(conf, "invalid-alias"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLoadSslConfiguration()
		{
			Configuration conf = ProvisionCredentialsForSSL();
			TestWebAppUtils.TestBuilder builder = (TestWebAppUtils.TestBuilder)new TestWebAppUtils.TestBuilder
				(this);
			builder = (TestWebAppUtils.TestBuilder)WebAppUtils.LoadSslConfiguration(builder, 
				conf);
			string keypass = "keypass";
			string storepass = "storepass";
			string trustpass = "trustpass";
			// make sure we get the right passwords in the builder
			NUnit.Framework.Assert.AreEqual(keypass, ((TestWebAppUtils.TestBuilder)builder).keypass
				);
			NUnit.Framework.Assert.AreEqual(storepass, ((TestWebAppUtils.TestBuilder)builder)
				.keystorePassword);
			NUnit.Framework.Assert.AreEqual(trustpass, ((TestWebAppUtils.TestBuilder)builder)
				.truststorePassword);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual Configuration ProvisionCredentialsForSSL()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			Configuration conf = new Configuration();
			Path jksPath = new Path(testDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			char[] keypass = new char[] { 'k', 'e', 'y', 'p', 'a', 's', 's' };
			char[] storepass = new char[] { 's', 't', 'o', 'r', 'e', 'p', 'a', 's', 's' };
			char[] trustpass = new char[] { 't', 'r', 'u', 's', 't', 'p', 'a', 's', 's' };
			// ensure that we get nulls when the key isn't there
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(WebAppUtils.WebAppKeyPasswordKey
				));
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(WebAppUtils.WebAppKeystorePasswordKey
				));
			NUnit.Framework.Assert.AreEqual(null, provider.GetCredentialEntry(WebAppUtils.WebAppTruststorePasswordKey
				));
			// create new aliases
			try
			{
				provider.CreateCredentialEntry(WebAppUtils.WebAppKeyPasswordKey, keypass);
				provider.CreateCredentialEntry(WebAppUtils.WebAppKeystorePasswordKey, storepass);
				provider.CreateCredentialEntry(WebAppUtils.WebAppTruststorePasswordKey, trustpass
					);
				// write out so that it can be found in checks
				provider.Flush();
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw;
			}
			// make sure we get back the right key directly from api
			Assert.AssertArrayEquals(keypass, provider.GetCredentialEntry(WebAppUtils.WebAppKeyPasswordKey
				).GetCredential());
			Assert.AssertArrayEquals(storepass, provider.GetCredentialEntry(WebAppUtils.WebAppKeystorePasswordKey
				).GetCredential());
			Assert.AssertArrayEquals(trustpass, provider.GetCredentialEntry(WebAppUtils.WebAppTruststorePasswordKey
				).GetCredential());
			return conf;
		}

		public class TestBuilder : HttpServer2.Builder
		{
			public string keypass;

			public string keystorePassword;

			public string truststorePassword;

			public override HttpServer2.Builder TrustStore(string location, string password, 
				string type)
			{
				this.truststorePassword = password;
				return base.TrustStore(location, password, type);
			}

			public override HttpServer2.Builder KeyStore(string location, string password, string
				 type)
			{
				this.keystorePassword = password;
				return base.KeyStore(location, password, type);
			}

			public override HttpServer2.Builder KeyPassword(string password)
			{
				this.keypass = password;
				return base.KeyPassword(password);
			}

			internal TestBuilder(TestWebAppUtils _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestWebAppUtils _enclosing;
		}
	}
}
