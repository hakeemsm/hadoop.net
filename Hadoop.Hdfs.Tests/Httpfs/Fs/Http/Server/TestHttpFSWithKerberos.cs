using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Http.Client;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Test;
using Org.Json.Simple;
using Org.Json.Simple.Parser;
using Org.Mortbay.Jetty.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	public class TestHttpFSWithKerberos : HFSTestCase
	{
		[TearDown]
		public virtual void ResetUGI()
		{
			Configuration conf = new Configuration();
			UserGroupInformation.SetConfiguration(conf);
		}

		/// <exception cref="System.Exception"/>
		private void CreateHttpFSServer()
		{
			FilePath homeDir = TestDirHelper.GetTestDir();
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			HttpFSServerWebApp.SetHomeDirForCurrentThread(homeDir.GetAbsolutePath());
			FilePath secretFile = new FilePath(new FilePath(homeDir, "conf"), "secret");
			TextWriter w = new FileWriter(secretFile);
			w.Write("secret");
			w.Close();
			//HDFS configuration
			FilePath hadoopConfDir = new FilePath(new FilePath(homeDir, "conf"), "hadoop-conf"
				);
			hadoopConfDir.Mkdirs();
			string fsDefaultName = TestHdfsHelper.GetHdfsConf().Get(CommonConfigurationKeysPublic
				.FsDefaultNameKey);
			Configuration conf = new Configuration(false);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, fsDefaultName);
			FilePath hdfsSite = new FilePath(hadoopConfDir, "hdfs-site.xml");
			OutputStream os = new FileOutputStream(hdfsSite);
			conf.WriteXml(os);
			os.Close();
			conf = new Configuration(false);
			conf.Set("httpfs.proxyuser.client.hosts", "*");
			conf.Set("httpfs.proxyuser.client.groups", "*");
			conf.Set("httpfs.authentication.type", "kerberos");
			conf.Set("httpfs.authentication.signature.secret.file", secretFile.GetAbsolutePath
				());
			FilePath httpfsSite = new FilePath(new FilePath(homeDir, "conf"), "httpfs-site.xml"
				);
			os = new FileOutputStream(httpfsSite);
			conf.WriteXml(os);
			os.Close();
			ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
			Uri url = cl.GetResource("webapp");
			WebAppContext context = new WebAppContext(url.AbsolutePath, "/webhdfs");
			Org.Mortbay.Jetty.Server server = TestJettyHelper.GetJettyServer();
			server.AddHandler(context);
			server.Start();
			HttpFSServerWebApp.Get().SetAuthority(TestJettyHelper.GetAuthority());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestValidHttpFSAccess()
		{
			CreateHttpFSServer();
			KerberosTestUtils.DoAsClient(new _Callable_120());
		}

		private sealed class _Callable_120 : Callable<Void>
		{
			public _Callable_120()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				Uri url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY"
					);
				AuthenticatedURL aUrl = new AuthenticatedURL();
				AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
				HttpURLConnection conn = aUrl.OpenConnection(url, aToken);
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestInvalidadHttpFSAccess()
		{
			CreateHttpFSServer();
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY"
				);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpUnauthorized
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDelegationTokenHttpFSAccess()
		{
			CreateHttpFSServer();
			KerberosTestUtils.DoAsClient(new _Callable_155());
		}

		private sealed class _Callable_155 : Callable<Void>
		{
			public _Callable_155()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				//get delegation token doing SPNEGO authentication
				Uri url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETDELEGATIONTOKEN"
					);
				AuthenticatedURL aUrl = new AuthenticatedURL();
				AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
				HttpURLConnection conn = aUrl.OpenConnection(url, aToken);
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
				JSONObject json = (JSONObject)new JSONParser().Parse(new InputStreamReader(conn.GetInputStream
					()));
				json = (JSONObject)json[DelegationTokenAuthenticator.DelegationTokenJson];
				string tokenStr = (string)json[DelegationTokenAuthenticator.DelegationTokenUrlStringJson
					];
				//access httpfs using the delegation token
				url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation="
					 + tokenStr);
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
				//try to renew the delegation token without SPNEGO credentials
				url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token="
					 + tokenStr);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpUnauthorized
					);
				//renew the delegation token with SPNEGO credentials
				url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token="
					 + tokenStr);
				conn = aUrl.OpenConnection(url, aToken);
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
				//cancel delegation token, no need for SPNEGO credentials
				url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token="
					 + tokenStr);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
				//try to access httpfs with the canceled delegation token
				url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation="
					 + tokenStr);
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpUnauthorized
					);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestDelegationTokenWithFS(Type fileSystemClass)
		{
			CreateHttpFSServer();
			Configuration conf = new Configuration();
			conf.Set("fs.webhdfs.impl", fileSystemClass.FullName);
			conf.Set("fs.hdfs.impl.disable.cache", "true");
			URI uri = new URI("webhdfs://" + TestJettyHelper.GetJettyURL().ToURI().GetAuthority
				());
			FileSystem fs = FileSystem.Get(uri, conf);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
				"foo", null);
			fs.Close();
			NUnit.Framework.Assert.AreEqual(1, tokens.Length);
			fs = FileSystem.Get(uri, conf);
			((DelegationTokenRenewer.Renewable)fs).SetDelegationToken(tokens[0]);
			fs.ListStatus(new Path("/"));
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		private void TestDelegationTokenWithinDoAs(Type fileSystemClass, bool proxyUser)
		{
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation.LoginUserFromKeytab("client", "/Users/tucu/tucu.keytab");
			UserGroupInformation ugi = UserGroupInformation.GetLoginUser();
			if (proxyUser)
			{
				ugi = UserGroupInformation.CreateProxyUser("foo", ugi);
			}
			conf = new Configuration();
			UserGroupInformation.SetConfiguration(conf);
			ugi.DoAs(new _PrivilegedExceptionAction_248(this, fileSystemClass));
		}

		private sealed class _PrivilegedExceptionAction_248 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_248(TestHttpFSWithKerberos _enclosing, Type fileSystemClass
				)
			{
				this._enclosing = _enclosing;
				this.fileSystemClass = fileSystemClass;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.TestDelegationTokenWithFS(fileSystemClass);
				return null;
			}

			private readonly TestHttpFSWithKerberos _enclosing;

			private readonly Type fileSystemClass;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDelegationTokenWithHttpFSFileSystem()
		{
			TestDelegationTokenWithinDoAs(typeof(HttpFSFileSystem), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDelegationTokenWithWebhdfsFileSystem()
		{
			TestDelegationTokenWithinDoAs(typeof(WebHdfsFileSystem), false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDelegationTokenWithHttpFSFileSystemProxyUser()
		{
			TestDelegationTokenWithinDoAs(typeof(HttpFSFileSystem), true);
		}
		// TODO: WebHdfsFilesystem does work with ProxyUser HDFS-3509
		//    @Test
		//    @TestDir
		//    @TestJetty
		//    @TestHdfs
		//    public void testDelegationTokenWithWebhdfsFileSystemProxyUser()
		//      throws Exception {
		//      testDelegationTokenWithinDoAs(WebHdfsFileSystem.class, true);
		//    }
	}
}
