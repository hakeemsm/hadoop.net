using System;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework.Rules;
using NUnit.Framework.Runners.Model;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestJettyHelper : MethodRule
	{
		private bool ssl;

		private string keyStoreType;

		private string keyStore;

		private string keyStorePassword;

		private Server server;

		public TestJettyHelper()
		{
			this.ssl = false;
		}

		public TestJettyHelper(string keyStoreType, string keyStore, string keyStorePassword
			)
		{
			ssl = true;
			this.keyStoreType = keyStoreType;
			this.keyStore = keyStore;
			this.keyStorePassword = keyStorePassword;
		}

		private static ThreadLocal<Org.Apache.Hadoop.Test.TestJettyHelper> TestJettyTl = 
			new InheritableThreadLocal<Org.Apache.Hadoop.Test.TestJettyHelper>();

		public virtual Statement Apply(Statement statement, FrameworkMethod frameworkMethod
			, object o)
		{
			return new _Statement_60(this, frameworkMethod, statement);
		}

		private sealed class _Statement_60 : Statement
		{
			public _Statement_60(TestJettyHelper _enclosing, FrameworkMethod frameworkMethod, 
				Statement statement)
			{
				this._enclosing = _enclosing;
				this.frameworkMethod = frameworkMethod;
				this.statement = statement;
			}

			/// <exception cref="System.Exception"/>
			public override void Evaluate()
			{
				TestJetty testJetty = frameworkMethod.GetAnnotation<TestJetty>();
				if (testJetty != null)
				{
					this._enclosing.server = this._enclosing.CreateJettyServer();
				}
				try
				{
					Org.Apache.Hadoop.Test.TestJettyHelper.TestJettyTl.Set(this._enclosing);
					statement.Evaluate();
				}
				finally
				{
					Org.Apache.Hadoop.Test.TestJettyHelper.TestJettyTl.Remove();
					if (this._enclosing.server != null && this._enclosing.server.IsRunning())
					{
						try
						{
							this._enclosing.server.Stop();
						}
						catch (Exception ex)
						{
							throw new RuntimeException("Could not stop embedded servlet container, " + ex.Message
								, ex);
						}
					}
				}
			}

			private readonly TestJettyHelper _enclosing;

			private readonly FrameworkMethod frameworkMethod;

			private readonly Statement statement;
		}

		private Server CreateJettyServer()
		{
			try
			{
				IPAddress localhost = Sharpen.Extensions.GetAddressByName("localhost");
				string host = "localhost";
				Socket ss = Sharpen.Extensions.CreateServerSocket(0, 50, localhost);
				int port = ss.GetLocalPort();
				ss.Close();
				Server server = new Server(0);
				if (!ssl)
				{
					server.GetConnectors()[0].SetHost(host);
					server.GetConnectors()[0].SetPort(port);
				}
				else
				{
					SslSocketConnector c = new SslSocketConnectorSecure();
					c.SetHost(host);
					c.SetPort(port);
					c.SetNeedClientAuth(false);
					c.SetKeystore(keyStore);
					c.SetKeystoreType(keyStoreType);
					c.SetKeyPassword(keyStorePassword);
					server.SetConnectors(new Connector[] { c });
				}
				return server;
			}
			catch (Exception ex)
			{
				throw new RuntimeException("Could not stop embedded servlet container, " + ex.Message
					, ex);
			}
		}

		/// <summary>Returns the authority (hostname & port) used by the JettyServer.</summary>
		/// <returns>an <code>InetSocketAddress</code> with the corresponding authority.</returns>
		public static IPEndPoint GetAuthority()
		{
			Server server = GetJettyServer();
			try
			{
				IPAddress add = Sharpen.Extensions.GetAddressByName(server.GetConnectors()[0].GetHost
					());
				int port = server.GetConnectors()[0].GetPort();
				return new IPEndPoint(add, port);
			}
			catch (UnknownHostException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <summary>Returns a Jetty server ready to be configured and the started.</summary>
		/// <remarks>
		/// Returns a Jetty server ready to be configured and the started. This server
		/// is only available when the test method has been annotated with
		/// <see cref="TestJetty"/>
		/// . Refer to
		/// <see cref="HTestCase"/>
		/// header for details.
		/// <p/>
		/// Once configured, the Jetty server should be started. The server will be
		/// automatically stopped when the test method ends.
		/// </remarks>
		/// <returns>a Jetty server ready to be configured and the started.</returns>
		public static Server GetJettyServer()
		{
			Org.Apache.Hadoop.Test.TestJettyHelper helper = TestJettyTl.Get();
			if (helper == null || helper.server == null)
			{
				throw new InvalidOperationException("This test does not use @TestJetty");
			}
			return helper.server;
		}

		/// <summary>
		/// Returns the base URL (SCHEMA://HOST:PORT) of the test Jetty server
		/// (see
		/// <see cref="GetJettyServer()"/>
		/// ) once started.
		/// </summary>
		/// <returns>the base URL (SCHEMA://HOST:PORT) of the test Jetty server.</returns>
		public static Uri GetJettyURL()
		{
			Org.Apache.Hadoop.Test.TestJettyHelper helper = TestJettyTl.Get();
			if (helper == null || helper.server == null)
			{
				throw new InvalidOperationException("This test does not use @TestJetty");
			}
			try
			{
				string scheme = (helper.ssl) ? "https" : "http";
				return new Uri(scheme + "://" + helper.server.GetConnectors()[0].GetHost() + ":" 
					+ helper.server.GetConnectors()[0].GetPort());
			}
			catch (UriFormatException ex)
			{
				throw new RuntimeException("It should never happen, " + ex.Message, ex);
			}
		}
	}
}
