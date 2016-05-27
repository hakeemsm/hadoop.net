using System;
using NUnit.Framework;
using Org.Apache.Curator.Test;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class TestKMSWithZK
	{
		/// <exception cref="System.Exception"/>
		protected internal virtual Configuration CreateBaseKMSConf(FilePath keyStoreDir)
		{
			Configuration conf = new Configuration(false);
			conf.Set(KMSConfiguration.KeyProviderUri, "jceks://file@" + new Path(keyStoreDir.
				GetAbsolutePath(), "kms.keystore").ToUri());
			conf.Set("hadoop.kms.authentication.type", "simple");
			conf.SetBoolean(KMSConfiguration.KeyAuthorizationEnable, false);
			conf.Set(KMSACLs.Type.GetKeys.GetAclConfigKey(), "foo");
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleKMSInstancesWithZKSigner()
		{
			FilePath testDir = TestKMS.GetTestDir();
			Configuration conf = CreateBaseKMSConf(testDir);
			TestingServer zkServer = new TestingServer();
			zkServer.Start();
			MiniKMS kms1 = null;
			MiniKMS kms2 = null;
			conf.Set(KMSAuthenticationFilter.ConfigPrefix + AuthenticationFilter.SignerSecretProvider
				, "zookeeper");
			conf.Set(KMSAuthenticationFilter.ConfigPrefix + ZKSignerSecretProvider.ZookeeperConnectionString
				, zkServer.GetConnectString());
			conf.Set(KMSAuthenticationFilter.ConfigPrefix + ZKSignerSecretProvider.ZookeeperPath
				, "/secret");
			TestKMS.WriteConf(testDir, conf);
			try
			{
				kms1 = new MiniKMS.Builder().SetKmsConfDir(testDir).SetLog4jConfFile("log4j.properties"
					).Build();
				kms1.Start();
				kms2 = new MiniKMS.Builder().SetKmsConfDir(testDir).SetLog4jConfFile("log4j.properties"
					).Build();
				kms2.Start();
				Uri url1 = new Uri(kms1.GetKMSUrl().ToExternalForm() + KMSRESTConstants.ServiceVersion
					 + "/" + KMSRESTConstants.KeysNamesResource);
				Uri url2 = new Uri(kms2.GetKMSUrl().ToExternalForm() + KMSRESTConstants.ServiceVersion
					 + "/" + KMSRESTConstants.KeysNamesResource);
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				UserGroupInformation ugiFoo = UserGroupInformation.CreateUserForTesting("foo", new 
					string[] { "gfoo" });
				UserGroupInformation ugiBar = UserGroupInformation.CreateUserForTesting("bar", new 
					string[] { "gBar" });
				ugiFoo.DoAs(new _PrivilegedExceptionAction_135(aUrl, url1, token));
				ugiBar.DoAs(new _PrivilegedExceptionAction_145(aUrl, url2, token));
				ugiBar.DoAs(new _PrivilegedExceptionAction_155(aUrl, url2));
			}
			finally
			{
				if (kms2 != null)
				{
					kms2.Stop();
				}
				if (kms1 != null)
				{
					kms1.Stop();
				}
				zkServer.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_135 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_135(DelegationTokenAuthenticatedURL aUrl, Uri url1
				, DelegationTokenAuthenticatedURL.Token token)
			{
				this.aUrl = aUrl;
				this.url1 = url1;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				HttpURLConnection conn = aUrl.OpenConnection(url1, token);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				return null;
			}

			private readonly DelegationTokenAuthenticatedURL aUrl;

			private readonly Uri url1;

			private readonly DelegationTokenAuthenticatedURL.Token token;
		}

		private sealed class _PrivilegedExceptionAction_145 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_145(DelegationTokenAuthenticatedURL aUrl, Uri url2
				, DelegationTokenAuthenticatedURL.Token token)
			{
				this.aUrl = aUrl;
				this.url2 = url2;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				HttpURLConnection conn = aUrl.OpenConnection(url2, token);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				return null;
			}

			private readonly DelegationTokenAuthenticatedURL aUrl;

			private readonly Uri url2;

			private readonly DelegationTokenAuthenticatedURL.Token token;
		}

		private sealed class _PrivilegedExceptionAction_155 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_155(DelegationTokenAuthenticatedURL aUrl, Uri url2
				)
			{
				this.aUrl = aUrl;
				this.url2 = url2;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				DelegationTokenAuthenticatedURL.Token emptyToken = new DelegationTokenAuthenticatedURL.Token
					();
				HttpURLConnection conn = aUrl.OpenConnection(url2, emptyToken);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpForbidden, conn.GetResponseCode
					());
				return null;
			}

			private readonly DelegationTokenAuthenticatedURL aUrl;

			private readonly Uri url2;
		}
	}
}
