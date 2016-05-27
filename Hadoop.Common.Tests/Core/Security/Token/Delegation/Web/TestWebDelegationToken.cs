using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Codehaus.Jackson.Map;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	public class TestWebDelegationToken
	{
		private const string OkUser = "ok-user";

		private const string FailUser = "fail-user";

		private const string FooUser = "foo";

		private Server jetty;

		public class DummyAuthenticationHandler : AuthenticationHandler
		{
			public override string GetType()
			{
				return "dummy";
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public override void Init(Properties config)
			{
			}

			public override void Destroy()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
			/// 	"/>
			public override bool ManagementOperation(AuthenticationToken token, HttpServletRequest
				 request, HttpServletResponse response)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
			/// 	"/>
			public override AuthenticationToken Authenticate(HttpServletRequest request, HttpServletResponse
				 response)
			{
				AuthenticationToken token = null;
				if (request.GetParameter("authenticated") != null)
				{
					token = new AuthenticationToken(request.GetParameter("authenticated"), "U", "test"
						);
				}
				else
				{
					response.SetStatus(HttpServletResponse.ScUnauthorized);
					response.SetHeader(KerberosAuthenticator.WwwAuthenticate, "dummy");
				}
				return token;
			}
		}

		public class DummyDelegationTokenAuthenticationHandler : DelegationTokenAuthenticationHandler
		{
			public DummyDelegationTokenAuthenticationHandler()
				: base(new TestWebDelegationToken.DummyAuthenticationHandler())
			{
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public override void Init(Properties config)
			{
				Properties conf = new Properties(config);
				conf.SetProperty(TokenKind, "token-kind");
				InitTokenManager(conf);
			}
		}

		public class AFilter : DelegationTokenAuthenticationFilter
		{
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties conf = new Properties();
				conf.SetProperty(AuthType, typeof(TestWebDelegationToken.DummyDelegationTokenAuthenticationHandler
					).FullName);
				return conf;
			}
		}

		[System.Serializable]
		public class PingServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				resp.SetStatus(HttpServletResponse.ScOk);
				resp.GetWriter().Write("ping");
				if (req.GetHeader(DelegationTokenAuthenticator.DelegationTokenHeader) != null)
				{
					resp.SetHeader("UsingHeader", "true");
				}
				if (req.GetQueryString() != null && req.GetQueryString().Contains(DelegationTokenAuthenticator
					.DelegationParam + "="))
				{
					resp.SetHeader("UsingQueryString", "true");
				}
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoPost(HttpServletRequest req, HttpServletResponse resp)
			{
				TextWriter writer = resp.GetWriter();
				writer.Write("ping: ");
				IOUtils.Copy(req.GetReader(), writer);
				resp.SetStatus(HttpServletResponse.ScOk);
			}
		}

		protected internal virtual Org.Mortbay.Jetty.Server CreateJettyServer()
		{
			try
			{
				IPAddress localhost = Sharpen.Runtime.GetLocalHost();
				Socket ss = Sharpen.Extensions.CreateServerSocket(0, 50, localhost);
				int port = ss.GetLocalPort();
				ss.Close();
				jetty = new Org.Mortbay.Jetty.Server(0);
				jetty.GetConnectors()[0].SetHost("localhost");
				jetty.GetConnectors()[0].SetPort(port);
				return jetty;
			}
			catch (Exception ex)
			{
				throw new RuntimeException("Could not setup Jetty: " + ex.Message, ex);
			}
		}

		protected internal virtual string GetJettyURL()
		{
			Connector c = jetty.GetConnectors()[0];
			return "http://" + c.GetHost() + ":" + c.GetPort();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			// resetting hadoop security to simple
			Configuration conf = new Configuration();
			UserGroupInformation.SetConfiguration(conf);
			jetty = CreateJettyServer();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanUp()
		{
			jetty.Stop();
			// resetting hadoop security to simple
			Configuration conf = new Configuration();
			UserGroupInformation.SetConfiguration(conf);
		}

		protected internal virtual Org.Mortbay.Jetty.Server GetJetty()
		{
			return jetty;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRawHttpCalls()
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.AFilter)), "/*", 
				0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.PingServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri nonAuthURL = new Uri(GetJettyURL() + "/foo/bar");
				Uri authURL = new Uri(GetJettyURL() + "/foo/bar?authenticated=foo");
				// unauthenticated access to URL
				HttpURLConnection conn = (HttpURLConnection)nonAuthURL.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				// authenticated access to URL
				conn = (HttpURLConnection)authURL.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				// unauthenticated access to get delegation token
				Uri url = new Uri(nonAuthURL.ToExternalForm() + "?op=GETDELEGATIONTOKEN");
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				// authenticated access to get delegation token
				url = new Uri(authURL.ToExternalForm() + "&op=GETDELEGATIONTOKEN&renewer=foo");
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				ObjectMapper mapper = new ObjectMapper();
				IDictionary map = mapper.ReadValue<IDictionary>(conn.GetInputStream());
				string dt = (string)((IDictionary)map["Token"])["urlString"];
				NUnit.Framework.Assert.IsNotNull(dt);
				// delegation token access to URL
				url = new Uri(nonAuthURL.ToExternalForm() + "?delegation=" + dt);
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				// delegation token and authenticated access to URL
				url = new Uri(authURL.ToExternalForm() + "&delegation=" + dt);
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				// renewew delegation token, unauthenticated access to URL
				url = new Uri(nonAuthURL.ToExternalForm() + "?op=RENEWDELEGATIONTOKEN&token=" + dt
					);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
					());
				// renewew delegation token, authenticated access to URL
				url = new Uri(authURL.ToExternalForm() + "&op=RENEWDELEGATIONTOKEN&token=" + dt);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				// renewew delegation token, authenticated access to URL, not renewer
				url = new Uri(GetJettyURL() + "/foo/bar?authenticated=bar&op=RENEWDELEGATIONTOKEN&token="
					 + dt);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpForbidden, conn.GetResponseCode
					());
				// cancel delegation token, nonauthenticated access to URL
				url = new Uri(nonAuthURL.ToExternalForm() + "?op=CANCELDELEGATIONTOKEN&token=" + 
					dt);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				// cancel canceled delegation token, nonauthenticated access to URL
				url = new Uri(nonAuthURL.ToExternalForm() + "?op=CANCELDELEGATIONTOKEN&token=" + 
					dt);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpNotFound, conn.GetResponseCode
					());
				// get new delegation token
				url = new Uri(authURL.ToExternalForm() + "&op=GETDELEGATIONTOKEN&renewer=foo");
				conn = (HttpURLConnection)url.OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				mapper = new ObjectMapper();
				map = mapper.ReadValue<IDictionary>(conn.GetInputStream());
				dt = (string)((IDictionary)map["Token"])["urlString"];
				NUnit.Framework.Assert.IsNotNull(dt);
				// cancel delegation token, authenticated access to URL
				url = new Uri(authURL.ToExternalForm() + "&op=CANCELDELEGATIONTOKEN&token=" + dt);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestMethod("PUT");
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			}
			finally
			{
				jetty.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAuthenticatorCallsWithHeader()
		{
			TestDelegationTokenAuthenticatorCalls(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAuthenticatorCallsWithQueryString()
		{
			TestDelegationTokenAuthenticatorCalls(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestDelegationTokenAuthenticatorCalls(bool useQS)
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.AFilter)), "/*", 
				0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.PingServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri nonAuthURL = new Uri(GetJettyURL() + "/foo/bar");
				Uri authURL = new Uri(GetJettyURL() + "/foo/bar?authenticated=foo");
				Uri authURL2 = new Uri(GetJettyURL() + "/foo/bar?authenticated=bar");
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				aUrl.SetUseQueryStringForDelegationToken(useQS);
				try
				{
					aUrl.GetDelegationToken(nonAuthURL, token, FooUser);
					NUnit.Framework.Assert.Fail();
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("401"));
				}
				aUrl.GetDelegationToken(authURL, token, FooUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				NUnit.Framework.Assert.AreEqual(new Text("token-kind"), token.GetDelegationToken(
					).GetKind());
				aUrl.RenewDelegationToken(authURL, token);
				try
				{
					aUrl.RenewDelegationToken(nonAuthURL, token);
					NUnit.Framework.Assert.Fail();
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("401"));
				}
				aUrl.GetDelegationToken(authURL, token, FooUser);
				try
				{
					aUrl.RenewDelegationToken(authURL2, token);
					NUnit.Framework.Assert.Fail();
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("403"));
				}
				aUrl.GetDelegationToken(authURL, token, FooUser);
				aUrl.CancelDelegationToken(authURL, token);
				aUrl.GetDelegationToken(authURL, token, FooUser);
				aUrl.CancelDelegationToken(nonAuthURL, token);
				aUrl.GetDelegationToken(authURL, token, FooUser);
				try
				{
					aUrl.RenewDelegationToken(nonAuthURL, token);
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("401"));
				}
				aUrl.GetDelegationToken(authURL, token, "foo");
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				ugi.AddToken(token.GetDelegationToken());
				ugi.DoAs(new _PrivilegedExceptionAction_412(aUrl, nonAuthURL, useQS));
			}
			finally
			{
				jetty.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_412 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_412(DelegationTokenAuthenticatedURL aUrl, Uri nonAuthURL
				, bool useQS)
			{
				this.aUrl = aUrl;
				this.nonAuthURL = nonAuthURL;
				this.useQS = useQS;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				HttpURLConnection conn = aUrl.OpenConnection(nonAuthURL, new DelegationTokenAuthenticatedURL.Token
					());
				NUnit.Framework.Assert.AreEqual(HttpServletResponse.ScOk, conn.GetResponseCode());
				if (useQS)
				{
					NUnit.Framework.Assert.IsNull(conn.GetHeaderField("UsingHeader"));
					NUnit.Framework.Assert.IsNotNull(conn.GetHeaderField("UsingQueryString"));
				}
				else
				{
					NUnit.Framework.Assert.IsNotNull(conn.GetHeaderField("UsingHeader"));
					NUnit.Framework.Assert.IsNull(conn.GetHeaderField("UsingQueryString"));
				}
				return null;
			}

			private readonly DelegationTokenAuthenticatedURL aUrl;

			private readonly Uri nonAuthURL;

			private readonly bool useQS;
		}

		private class DummyDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
			<DelegationTokenIdentifier>
		{
			public DummyDelegationTokenSecretManager()
				: base(10000, 10000, 10000, 10000)
			{
			}

			public override DelegationTokenIdentifier CreateIdentifier()
			{
				return new DelegationTokenIdentifier(new Text("fooKind"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExternalDelegationTokenSecretManager()
		{
			TestWebDelegationToken.DummyDelegationTokenSecretManager secretMgr = new TestWebDelegationToken.DummyDelegationTokenSecretManager
				();
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.AFilter)), "/*", 
				0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.PingServlet)), 
				"/bar");
			try
			{
				secretMgr.StartThreads();
				context.SetAttribute(DelegationTokenAuthenticationFilter.DelegationTokenSecretManagerAttr
					, secretMgr);
				jetty.Start();
				Uri authURL = new Uri(GetJettyURL() + "/foo/bar?authenticated=foo");
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				aUrl.GetDelegationToken(authURL, token, FooUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				NUnit.Framework.Assert.AreEqual(new Text("fooKind"), token.GetDelegationToken().GetKind
					());
			}
			finally
			{
				jetty.Stop();
				secretMgr.StopThreads();
			}
		}

		public class NoDTFilter : AuthenticationFilter
		{
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties conf = new Properties();
				conf.SetProperty(AuthType, PseudoAuthenticationHandler.Type);
				return conf;
			}
		}

		public class NoDTHandlerDTAFilter : DelegationTokenAuthenticationFilter
		{
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties conf = new Properties();
				conf.SetProperty(AuthType, PseudoAuthenticationHandler.Type);
				return conf;
			}
		}

		[System.Serializable]
		public class UserServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				resp.SetStatus(HttpServletResponse.ScOk);
				resp.GetWriter().Write(req.GetUserPrincipal().GetName());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAuthenticationURLWithNoDTFilter()
		{
			TestDelegationTokenAuthenticatedURLWithNoDT(typeof(TestWebDelegationToken.NoDTFilter
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenAuthenticationURLWithNoDTHandler()
		{
			TestDelegationTokenAuthenticatedURLWithNoDT(typeof(TestWebDelegationToken.NoDTHandlerDTAFilter
				));
		}

		// we are, also, implicitly testing  KerberosDelegationTokenAuthenticator
		// fallback here
		/// <exception cref="System.Exception"/>
		private void TestDelegationTokenAuthenticatedURLWithNoDT(Type filterClass)
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(filterClass), "/*", 0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.UserServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri url = new Uri(GetJettyURL() + "/foo/bar");
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(FooUser);
				ugi.DoAs(new _PrivilegedExceptionAction_543(url));
			}
			finally
			{
				jetty.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_543 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_543(Uri url)
			{
				this.url = url;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				HttpURLConnection conn = aUrl.OpenConnection(url, token);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				IList<string> ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(TestWebDelegationToken.FooUser, ret[0]);
				try
				{
					aUrl.GetDelegationToken(url, token, TestWebDelegationToken.FooUser);
					NUnit.Framework.Assert.Fail();
				}
				catch (AuthenticationException ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("delegation token operation"));
				}
				return null;
			}

			private readonly Uri url;
		}

		public class PseudoDTAFilter : DelegationTokenAuthenticationFilter
		{
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties conf = new Properties();
				conf.SetProperty(AuthType, typeof(PseudoDelegationTokenAuthenticationHandler).FullName
					);
				conf.SetProperty(DelegationTokenAuthenticationHandler.TokenKind, "token-kind");
				return conf;
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			protected internal override Configuration GetProxyuserConfiguration(FilterConfig 
				filterConfig)
			{
				Configuration conf = new Configuration(false);
				conf.Set("proxyuser.foo.users", OkUser);
				conf.Set("proxyuser.foo.hosts", "localhost");
				return conf;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackToPseudoDelegationTokenAuthenticator()
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.PseudoDTAFilter)
				), "/*", 0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.UserServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri url = new Uri(GetJettyURL() + "/foo/bar");
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(FooUser);
				ugi.DoAs(new _PrivilegedExceptionAction_612(url));
			}
			finally
			{
				jetty.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_612 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_612(Uri url)
			{
				this.url = url;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				HttpURLConnection conn = aUrl.OpenConnection(url, token);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				IList<string> ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(TestWebDelegationToken.FooUser, ret[0]);
				aUrl.GetDelegationToken(url, token, TestWebDelegationToken.FooUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				NUnit.Framework.Assert.AreEqual(new Text("token-kind"), token.GetDelegationToken(
					).GetKind());
				return null;
			}

			private readonly Uri url;
		}

		public class KDTAFilter : DelegationTokenAuthenticationFilter
		{
			internal static string keytabFile;

			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties conf = new Properties();
				conf.SetProperty(AuthType, typeof(KerberosDelegationTokenAuthenticationHandler).FullName
					);
				conf.SetProperty(KerberosAuthenticationHandler.Keytab, keytabFile);
				conf.SetProperty(KerberosAuthenticationHandler.Principal, "HTTP/localhost");
				conf.SetProperty(KerberosDelegationTokenAuthenticationHandler.TokenKind, "token-kind"
					);
				return conf;
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			protected internal override Configuration GetProxyuserConfiguration(FilterConfig 
				filterConfig)
			{
				Configuration conf = new Configuration(false);
				conf.Set("proxyuser.client.users", OkUser);
				conf.Set("proxyuser.client.hosts", "localhost");
				return conf;
			}
		}

		private class KerberosConfiguration : Configuration
		{
			private string principal;

			private string keytab;

			public KerberosConfiguration(string principal, string keytab)
			{
				this.principal = principal;
				this.keytab = keytab;
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				IDictionary<string, string> options = new Dictionary<string, string>();
				options["principal"] = principal;
				options["keyTab"] = keytab;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["doNotPrompt"] = "true";
				options["useTicketCache"] = "true";
				options["renewTGT"] = "true";
				options["refreshKrb5Config"] = "true";
				options["isInitiator"] = "true";
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					options["ticketCache"] = ticketCache;
				}
				options["debug"] = "true";
				return new AppConfigurationEntry[] { new AppConfigurationEntry(KerberosUtil.GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}
		}

		/// <exception cref="System.Exception"/>
		public static T DoAsKerberosUser<T>(string principal, string keytab, Callable<T> 
			callable)
		{
			LoginContext loginContext = null;
			try
			{
				ICollection<Principal> principals = new HashSet<Principal>();
				principals.AddItem(new KerberosPrincipal(principal));
				Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
					<object>());
				loginContext = new LoginContext(string.Empty, subject, null, new TestWebDelegationToken.KerberosConfiguration
					(principal, keytab));
				loginContext.Login();
				subject = loginContext.GetSubject();
				return Subject.DoAs(subject, new _PrivilegedExceptionAction_712(callable));
			}
			catch (PrivilegedActionException ex)
			{
				throw ex.GetException();
			}
			finally
			{
				if (loginContext != null)
				{
					loginContext.Logout();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_712 : PrivilegedExceptionAction<T
			>
		{
			public _PrivilegedExceptionAction_712(Callable<T> callable)
			{
				this.callable = callable;
			}

			/// <exception cref="System.Exception"/>
			public T Run()
			{
				return callable.Call();
			}

			private readonly Callable<T> callable;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosDelegationTokenAuthenticator()
		{
			TestKerberosDelegationTokenAuthenticator(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosDelegationTokenAuthenticatorWithDoAs()
		{
			TestKerberosDelegationTokenAuthenticator(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestKerberosDelegationTokenAuthenticator(bool doAs)
		{
			string doAsUser = doAs ? OkUser : null;
			// setting hadoop security to kerberos
			Configuration conf = new Configuration();
			conf.Set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath testDir = new FilePath("target/" + UUID.RandomUUID().ToString());
			NUnit.Framework.Assert.IsTrue(testDir.Mkdirs());
			MiniKdc kdc = new MiniKdc(MiniKdc.CreateConf(), testDir);
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.KDTAFilter)), "/*"
				, 0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.UserServlet)), 
				"/bar");
			try
			{
				kdc.Start();
				FilePath keytabFile = new FilePath(testDir, "test.keytab");
				kdc.CreatePrincipal(keytabFile, "client", "HTTP/localhost");
				TestWebDelegationToken.KDTAFilter.keytabFile = keytabFile.GetAbsolutePath();
				jetty.Start();
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				Uri url = new Uri(GetJettyURL() + "/foo/bar");
				try
				{
					aUrl.GetDelegationToken(url, token, FooUser, doAsUser);
					NUnit.Framework.Assert.Fail();
				}
				catch (AuthenticationException ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("GSSException"));
				}
				DoAsKerberosUser("client", keytabFile.GetAbsolutePath(), new _Callable_778(aUrl, 
					url, token, doAs, doAsUser));
			}
			finally
			{
				// Make sure the token belongs to the right owner
				jetty.Stop();
				kdc.Stop();
			}
		}

		private sealed class _Callable_778 : Callable<Void>
		{
			public _Callable_778(DelegationTokenAuthenticatedURL aUrl, Uri url, DelegationTokenAuthenticatedURL.Token
				 token, bool doAs, string doAsUser)
			{
				this.aUrl = aUrl;
				this.url = url;
				this.token = token;
				this.doAs = doAs;
				this.doAsUser = doAsUser;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				aUrl.GetDelegationToken(url, token, doAs ? doAsUser : "client", doAsUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				NUnit.Framework.Assert.AreEqual(new Text("token-kind"), token.GetDelegationToken(
					).GetKind());
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetDelegationToken().GetIdentifier
					());
				DataInputStream dis = new DataInputStream(buf);
				DelegationTokenIdentifier id = new DelegationTokenIdentifier(new Text("token-kind"
					));
				id.ReadFields(dis);
				dis.Close();
				NUnit.Framework.Assert.AreEqual(doAs ? new Text(TestWebDelegationToken.OkUser) : 
					new Text("client"), id.GetOwner());
				if (doAs)
				{
					NUnit.Framework.Assert.AreEqual(new Text("client"), id.GetRealUser());
				}
				aUrl.RenewDelegationToken(url, token, doAsUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				aUrl.GetDelegationToken(url, token, TestWebDelegationToken.FooUser, doAsUser);
				NUnit.Framework.Assert.IsNotNull(token.GetDelegationToken());
				try
				{
					aUrl.RenewDelegationToken(url, token, doAsUser);
					NUnit.Framework.Assert.Fail();
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("403"));
				}
				aUrl.GetDelegationToken(url, token, TestWebDelegationToken.FooUser, doAsUser);
				aUrl.CancelDelegationToken(url, token, doAsUser);
				NUnit.Framework.Assert.IsNull(token.GetDelegationToken());
				return null;
			}

			private readonly DelegationTokenAuthenticatedURL aUrl;

			private readonly Uri url;

			private readonly DelegationTokenAuthenticatedURL.Token token;

			private readonly bool doAs;

			private readonly string doAsUser;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProxyUser()
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.PseudoDTAFilter)
				), "/*", 0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.UserServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri url = new Uri(GetJettyURL() + "/foo/bar");
				// proxyuser using raw HTTP, verifying doAs is case insensitive
				string strUrl = string.Format("%s?user.name=%s&doas=%s", url.ToExternalForm(), FooUser
					, OkUser);
				HttpURLConnection conn = (HttpURLConnection)new Uri(strUrl).OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				IList<string> ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(OkUser, ret[0]);
				strUrl = string.Format("%s?user.name=%s&DOAS=%s", url.ToExternalForm(), FooUser, 
					OkUser);
				conn = (HttpURLConnection)new Uri(strUrl).OpenConnection();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(OkUser, ret[0]);
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(FooUser);
				ugi.DoAs(new _PrivilegedExceptionAction_858(url));
			}
			finally
			{
				// proxyuser using authentication handler authentication
				// unauthorized proxy user using authentication handler authentication
				// proxy using delegation token authentication
				// requests using delegation token as auth do not honor doAs
				jetty.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_858 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_858(Uri url)
			{
				this.url = url;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				HttpURLConnection conn = aUrl.OpenConnection(url, token, TestWebDelegationToken.OkUser
					);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				IList<string> ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(TestWebDelegationToken.OkUser, ret[0]);
				conn = aUrl.OpenConnection(url, token, TestWebDelegationToken.FailUser);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpForbidden, conn.GetResponseCode
					());
				aUrl.GetDelegationToken(url, token, TestWebDelegationToken.FooUser);
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				ugi.AddToken(token.GetDelegationToken());
				token = new DelegationTokenAuthenticatedURL.Token();
				conn = aUrl.OpenConnection(url, token, TestWebDelegationToken.OkUser);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual(TestWebDelegationToken.FooUser, ret[0]);
				return null;
			}

			private readonly Uri url;
		}

		[System.Serializable]
		public class UGIServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				UserGroupInformation ugi = HttpUserGroupInformation.Get();
				if (ugi != null)
				{
					string ret = "remoteuser=" + req.GetRemoteUser() + ":ugi=" + ugi.GetShortUserName
						();
					if (ugi.GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.Proxy)
					{
						ret = "realugi=" + ugi.GetRealUser().GetShortUserName() + ":" + ret;
					}
					resp.SetStatus(HttpServletResponse.ScOk);
					resp.GetWriter().Write(ret);
				}
				else
				{
					resp.SetStatus(HttpServletResponse.ScInternalServerError);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHttpUGI()
		{
			Org.Mortbay.Jetty.Server jetty = CreateJettyServer();
			Context context = new Context();
			context.SetContextPath("/foo");
			jetty.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(TestWebDelegationToken.PseudoDTAFilter)
				), "/*", 0);
			context.AddServlet(new ServletHolder(typeof(TestWebDelegationToken.UGIServlet)), 
				"/bar");
			try
			{
				jetty.Start();
				Uri url = new Uri(GetJettyURL() + "/foo/bar");
				UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(FooUser);
				ugi.DoAs(new _PrivilegedExceptionAction_938(url));
			}
			finally
			{
				// user foo
				// user ok-user via proxyuser foo
				jetty.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_938 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_938(Uri url)
			{
				this.url = url;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				DelegationTokenAuthenticatedURL.Token token = new DelegationTokenAuthenticatedURL.Token
					();
				DelegationTokenAuthenticatedURL aUrl = new DelegationTokenAuthenticatedURL();
				HttpURLConnection conn = aUrl.OpenConnection(url, token);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				IList<string> ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual("remoteuser=" + TestWebDelegationToken.FooUser + 
					":ugi=" + TestWebDelegationToken.FooUser, ret[0]);
				conn = aUrl.OpenConnection(url, token, TestWebDelegationToken.OkUser);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				ret = IOUtils.ReadLines(conn.GetInputStream());
				NUnit.Framework.Assert.AreEqual(1, ret.Count);
				NUnit.Framework.Assert.AreEqual("realugi=" + TestWebDelegationToken.FooUser + ":remoteuser="
					 + TestWebDelegationToken.OkUser + ":ugi=" + TestWebDelegationToken.OkUser, ret[
					0]);
				return null;
			}

			private readonly Uri url;
		}
	}
}
