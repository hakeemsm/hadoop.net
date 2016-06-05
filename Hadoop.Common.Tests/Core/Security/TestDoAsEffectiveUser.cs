using System;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authorize;


namespace Org.Apache.Hadoop.Security
{
	public class TestDoAsEffectiveUser
	{
		private const string RealUserName = "realUser1@HADOOP.APACHE.ORG";

		private const string RealUserShortName = "realUser1";

		private const string ProxyUserName = "proxyUser";

		private const string Group1Name = "group1";

		private const string Group2Name = "group2";

		private static readonly string[] GroupNames = new string[] { Group1Name, Group2Name
			 };

		private const string Address = "0.0.0.0";

		private TestDoAsEffectiveUser.TestProtocol proxy;

		private static readonly Configuration masterConf = new Configuration();

		public static readonly Log Log = LogFactory.GetLog(typeof(TestDoAsEffectiveUser));

		static TestDoAsEffectiveUser()
		{
			masterConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//"
				 + "RULE:[1:$1@$0](.*@HADOOP.APACHE.ORG)s/@.*//" + "DEFAULT");
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetMasterConf()
		{
			UserGroupInformation.SetConfiguration(masterConf);
			RefreshConf(masterConf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ConfigureSuperUserIPAddresses(Configuration conf, string superUserShortName
			)
		{
			AList<string> ipList = new AList<string>();
			Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.GetNetworkInterfaces
				();
			while (netInterfaceList.MoveNext())
			{
				NetworkInterface inf = netInterfaceList.Current;
				Enumeration<IPAddress> addrList = inf.GetInetAddresses();
				while (addrList.MoveNext())
				{
					IPAddress addr = addrList.Current;
					ipList.AddItem(addr.GetHostAddress());
				}
			}
			StringBuilder builder = new StringBuilder();
			foreach (string ip in ipList)
			{
				builder.Append(ip);
				builder.Append(',');
			}
			builder.Append("127.0.1.1,");
			builder.Append(Runtime.GetLocalHost().ToString());
			Log.Info("Local Ip addresses: " + builder.ToString());
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(superUserShortName), builder.ToString());
		}

		/// <summary>
		/// Test method for
		/// <see cref="UserGroupInformation.CreateProxyUser(string, UserGroupInformation)"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateProxyUser()
		{
			// ensure that doAs works correctly
			UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUser(ProxyUserName
				, realUserUgi);
			UserGroupInformation curUGI = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_122
				());
			Assert.Equal(ProxyUserName + " (auth:PROXY) via " + RealUserName
				 + " (auth:SIMPLE)", curUGI.ToString());
		}

		private sealed class _PrivilegedExceptionAction_122 : PrivilegedExceptionAction<UserGroupInformation
			>
		{
			public _PrivilegedExceptionAction_122()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public UserGroupInformation Run()
			{
				return UserGroupInformation.GetCurrentUser();
			}
		}

		public abstract class TestProtocol : VersionedProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string AMethod();

			/// <exception cref="System.IO.IOException"/>
			public abstract string GetServerRemoteUser();
		}

		public static class TestProtocolConstants
		{
		}

		public class TestImpl : TestDoAsEffectiveUser.TestProtocol
		{
			/// <exception cref="System.IO.IOException"/>
			public override string AMethod()
			{
				return UserGroupInformation.GetCurrentUser().ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public override string GetServerRemoteUser()
			{
				return Server.GetRemoteUser().ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestDoAsEffectiveUser.TestProtocol.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				return new ProtocolSignature(TestDoAsEffectiveUser.TestProtocol.versionID, null);
			}

			internal TestImpl(TestDoAsEffectiveUser _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDoAsEffectiveUser _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private void CheckRemoteUgi(Server server, UserGroupInformation ugi, Configuration
			 conf)
		{
			ugi.DoAs(new _PrivilegedExceptionAction_169(this, server, conf, ugi));
		}

		private sealed class _PrivilegedExceptionAction_169 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_169(TestDoAsEffectiveUser _enclosing, Server server
				, Configuration conf, UserGroupInformation ugi)
			{
				this._enclosing = _enclosing;
				this.server = server;
				this.conf = conf;
				this.ugi = ugi;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
					.versionID, NetUtils.GetConnectAddress(server), conf);
				Assert.Equal(ugi.ToString(), this._enclosing.proxy.AMethod());
				Assert.Equal(ugi.ToString(), this._enclosing.proxy.GetServerRemoteUser
					());
				return null;
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly Server server;

			private readonly Configuration conf;

			private readonly UserGroupInformation ugi;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRealUserSetup()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserShortName), "group1");
			ConfigureSuperUserIPAddresses(conf, RealUserShortName);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(5).SetVerbose(true).Build();
			RefreshConf(conf);
			try
			{
				server.Start();
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				CheckRemoteUgi(server, realUserUgi, conf);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				CheckRemoteUgi(server, proxyUserUgi, conf);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRealUserAuthorizationSuccess()
		{
			Configuration conf = new Configuration();
			ConfigureSuperUserIPAddresses(conf, RealUserShortName);
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserShortName), "group1");
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			RefreshConf(conf);
			try
			{
				server.Start();
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				CheckRemoteUgi(server, realUserUgi, conf);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				CheckRemoteUgi(server, proxyUserUgi, conf);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/*
		* Tests authorization of superuser's ip.
		*/
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRealUserIPAuthorizationFailure()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(RealUserShortName), "20.20.20.20");
			//Authorized IP address
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserShortName), "group1");
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			RefreshConf(conf);
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				string retVal = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_276(this, addr, 
					conf));
				NUnit.Framework.Assert.Fail("The RPC must have failed " + retVal);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_276 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_276(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public string Run()
			{
				this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
					.versionID, addr, conf);
				string ret = this._enclosing.proxy.AMethod();
				return ret;
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRealUserIPNotSpecified()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserShortName), "group1");
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			RefreshConf(conf);
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				string retVal = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_319(this, addr, 
					conf));
				NUnit.Framework.Assert.Fail("The RPC must have failed " + retVal);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_319 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_319(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public string Run()
			{
				this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
					.versionID, addr, conf);
				string ret = this._enclosing.proxy.AMethod();
				return ret;
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRealUserGroupNotSpecified()
		{
			Configuration conf = new Configuration();
			ConfigureSuperUserIPAddresses(conf, RealUserShortName);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				string retVal = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_359(this, addr, 
					conf));
				NUnit.Framework.Assert.Fail("The RPC must have failed " + retVal);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_359 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_359(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public string Run()
			{
				this._enclosing.proxy = (TestDoAsEffectiveUser.TestProtocol)RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol
					>(TestDoAsEffectiveUser.TestProtocol.versionID, addr, conf);
				string ret = this._enclosing.proxy.AMethod();
				return ret;
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestRealUserGroupAuthorizationFailure()
		{
			Configuration conf = new Configuration();
			ConfigureSuperUserIPAddresses(conf, RealUserShortName);
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUserShortName), "group3");
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			RefreshConf(conf);
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				UserGroupInformation realUserUgi = UserGroupInformation.CreateRemoteUser(RealUserName
					);
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(ProxyUserName, realUserUgi, GroupNames);
				string retVal = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_404(this, addr, 
					conf));
				NUnit.Framework.Assert.Fail("The RPC must have failed " + retVal);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_404 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_404(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public string Run()
			{
				this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
					.versionID, addr, conf);
				string ret = this._enclosing.proxy.AMethod();
				return ret;
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		/*
		*  Tests the scenario when token authorization is used.
		*  The server sees only the the owner of the token as the
		*  user.
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestProxyWithToken()
		{
			Configuration conf = new Configuration(masterConf);
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			UserGroupInformation.SetConfiguration(conf);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
			server.Start();
			UserGroupInformation current = UserGroupInformation.CreateRemoteUser(RealUserName
				);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier(new 
				Org.Apache.Hadoop.IO.Text(current.GetUserName()), new Org.Apache.Hadoop.IO.Text(
				"SomeSuperUser"));
			Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = new 
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>(tokenId, 
				sm);
			SecurityUtil.SetTokenService(token, addr);
			UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
				(ProxyUserName, current, GroupNames);
			proxyUserUgi.AddToken(token);
			RefreshConf(conf);
			string retVal = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_457(this, addr, 
				conf, server));
			//The user returned by server must be the one in the token.
			Assert.Equal(RealUserName + " (auth:TOKEN) via SomeSuperUser (auth:SIMPLE)"
				, retVal);
		}

		private sealed class _PrivilegedExceptionAction_457 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_457(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration conf, Server server)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
				this.server = server;
			}

			/// <exception cref="System.Exception"/>
			public string Run()
			{
				try
				{
					this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
						.versionID, addr, conf);
					string ret = this._enclosing.proxy.AMethod();
					return ret;
				}
				catch (Exception e)
				{
					Runtime.PrintStackTrace(e);
					throw;
				}
				finally
				{
					server.Stop();
					if (this._enclosing.proxy != null)
					{
						RPC.StopProxy(this._enclosing.proxy);
					}
				}
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;

			private readonly Server server;
		}

		/*
		* The user gets the token via a superuser. Server should authenticate
		* this user.
		*/
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTokenBySuperUser()
		{
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			Configuration newConf = new Configuration(masterConf);
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, newConf);
			UserGroupInformation.SetConfiguration(newConf);
			Server server = new RPC.Builder(newConf).SetProtocol(typeof(TestDoAsEffectiveUser.TestProtocol
				)).SetInstance(new TestDoAsEffectiveUser.TestImpl(this)).SetBindAddress(Address)
				.SetPort(0).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
			server.Start();
			UserGroupInformation current = UserGroupInformation.CreateUserForTesting(RealUserName
				, GroupNames);
			RefreshConf(newConf);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier(new 
				Org.Apache.Hadoop.IO.Text(current.GetUserName()), new Org.Apache.Hadoop.IO.Text(
				"SomeSuperUser"));
			Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = new 
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>(tokenId, 
				sm);
			SecurityUtil.SetTokenService(token, addr);
			current.AddToken(token);
			string retVal = current.DoAs(new _PrivilegedExceptionAction_509(this, addr, newConf
				, server));
			string expected = RealUserName + " (auth:TOKEN) via SomeSuperUser (auth:SIMPLE)";
			Assert.Equal(retVal + "!=" + expected, expected, retVal);
		}

		private sealed class _PrivilegedExceptionAction_509 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_509(TestDoAsEffectiveUser _enclosing, IPEndPoint
				 addr, Configuration newConf, Server server)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.newConf = newConf;
				this.server = server;
			}

			/// <exception cref="System.Exception"/>
			public string Run()
			{
				try
				{
					this._enclosing.proxy = RPC.GetProxy<TestDoAsEffectiveUser.TestProtocol>(TestDoAsEffectiveUser.TestProtocol
						.versionID, addr, newConf);
					string ret = this._enclosing.proxy.AMethod();
					return ret;
				}
				catch (Exception e)
				{
					Runtime.PrintStackTrace(e);
					throw;
				}
				finally
				{
					server.Stop();
					if (this._enclosing.proxy != null)
					{
						RPC.StopProxy(this._enclosing.proxy);
					}
				}
			}

			private readonly TestDoAsEffectiveUser _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration newConf;

			private readonly Server server;
		}

		//
		/// <exception cref="System.IO.IOException"/>
		private void RefreshConf(Configuration conf)
		{
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
		}
	}
}
