using System;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// MiniRPCBenchmark measures time to establish an RPC connection
	/// to a secure RPC server.
	/// </summary>
	/// <remarks>
	/// MiniRPCBenchmark measures time to establish an RPC connection
	/// to a secure RPC server.
	/// It sequentially establishes connections the specified number of times,
	/// and calculates the average time taken to connect.
	/// The time to connect includes the server side authentication time.
	/// The benchmark supports three authentication methods:
	/// <ol>
	/// <li>simple - no authentication. In order to enter this mode
	/// the configuration file <tt>core-site.xml</tt> should specify
	/// <tt>hadoop.security.authentication = simple</tt>.
	/// This is the default mode.</li>
	/// <li>kerberos - kerberos authentication. In order to enter this mode
	/// the configuration file <tt>core-site.xml</tt> should specify
	/// <tt>hadoop.security.authentication = kerberos</tt> and
	/// the argument string should provide qualifying
	/// <tt>keytabFile</tt> and <tt>userName</tt> parameters.
	/// <li>delegation token - authentication using delegation token.
	/// In order to enter this mode the benchmark should provide all the
	/// mentioned parameters for kerberos authentication plus the
	/// <tt>useToken</tt> argument option.
	/// </ol>
	/// Input arguments:
	/// <ul>
	/// <li>numIterations - number of connections to establish</li>
	/// <li>keytabFile - keytab file for kerberos authentication</li>
	/// <li>userName - principal name for kerberos authentication</li>
	/// <li>useToken - should be specified for delegation token authentication</li>
	/// <li>logLevel - logging level, see
	/// <see cref="Org.Apache.Log4j.Level"/>
	/// </li>
	/// </ul>
	/// </remarks>
	public class MiniRPCBenchmark
	{
		private const string KeytabFileKey = "test.keytab.file";

		private const string UserNameKey = "test.user.name";

		private const string MiniUser = "miniUser";

		private const string Renewer = "renewer";

		private const string GroupName1 = "MiniGroup1";

		private const string GroupName2 = "MiniGroup2";

		private static readonly string[] GroupNames = new string[] { GroupName1, GroupName2
			 };

		private UserGroupInformation currentUgi;

		private Level logLevel;

		internal MiniRPCBenchmark(Level l)
		{
			currentUgi = null;
			logLevel = l;
		}

		public class TestDelegationTokenSelector : AbstractDelegationTokenSelector<TestDelegationToken.TestDelegationTokenIdentifier
			>
		{
			protected internal TestDelegationTokenSelector()
				: base(new Text("MY KIND"))
			{
			}
		}

		public abstract class MiniProtocol : VersionedProtocol
		{
			public const long versionID = 1L;

			/// <summary>Get a Delegation Token.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> GetDelegationToken(Text renewer);
		}

		public static class MiniProtocolConstants
		{
		}

		/// <summary>
		/// Primitive RPC server, which
		/// allows clients to connect to it.
		/// </summary>
		internal class MiniServer : MiniRPCBenchmark.MiniProtocol
		{
			private const string DefaultServerAddress = "0.0.0.0";

			private TestDelegationToken.TestDelegationTokenSecretManager secretManager;

			private Server rpcServer;

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				// VersionedProtocol
				if (protocol.Equals(typeof(MiniRPCBenchmark.MiniProtocol).FullName))
				{
					return versionID;
				}
				throw new IOException("Unknown protocol: " + protocol);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHashCode)
			{
				// VersionedProtocol
				if (protocol.Equals(typeof(MiniRPCBenchmark.MiniProtocol).FullName))
				{
					return new ProtocolSignature(versionID, null);
				}
				throw new IOException("Unknown protocol: " + protocol);
			}

			/// <exception cref="System.IO.IOException"/>
			public override Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> GetDelegationToken(Text renewer)
			{
				// MiniProtocol
				string owner = UserGroupInformation.GetCurrentUser().GetUserName();
				string realUser = UserGroupInformation.GetCurrentUser().GetRealUser() == null ? string.Empty
					 : UserGroupInformation.GetCurrentUser().GetRealUser().GetUserName();
				TestDelegationToken.TestDelegationTokenIdentifier tokenId = new TestDelegationToken.TestDelegationTokenIdentifier
					(new Text(owner), renewer, new Text(realUser));
				return new Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					>(tokenId, secretManager);
			}

			/// <summary>Start RPC server</summary>
			/// <exception cref="System.IO.IOException"/>
			internal MiniServer(Configuration conf, string user, string keytabFile)
			{
				UserGroupInformation.SetConfiguration(conf);
				UserGroupInformation.LoginUserFromKeytab(user, keytabFile);
				secretManager = new TestDelegationToken.TestDelegationTokenSecretManager(24 * 60 
					* 60 * 1000, 7 * 24 * 60 * 60 * 1000, 24 * 60 * 60 * 1000, 3600000);
				secretManager.StartThreads();
				rpcServer = new RPC.Builder(conf).SetProtocol(typeof(MiniRPCBenchmark.MiniProtocol
					)).SetInstance(this).SetBindAddress(DefaultServerAddress).SetPort(0).SetNumHandlers
					(1).SetVerbose(false).SetSecretManager(secretManager).Build();
				rpcServer.Start();
			}

			/// <summary>Stop RPC server</summary>
			internal virtual void Stop()
			{
				if (rpcServer != null)
				{
					rpcServer.Stop();
				}
				rpcServer = null;
			}

			/// <summary>Get RPC server address</summary>
			internal virtual IPEndPoint GetAddress()
			{
				if (rpcServer == null)
				{
					return null;
				}
				return NetUtils.GetConnectAddress(rpcServer);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long ConnectToServer(Configuration conf, IPEndPoint addr)
		{
			MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				long start = Time.Now();
				client = RPC.GetProxy<MiniRPCBenchmark.MiniProtocol>(MiniRPCBenchmark.MiniProtocol
					.versionID, addr, conf);
				long end = Time.Now();
				return end - start;
			}
			finally
			{
				RPC.StopProxy(client);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ConnectToServerAndGetDelegationToken(Configuration conf, IPEndPoint
			 addr)
		{
			MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				UserGroupInformation current = UserGroupInformation.GetCurrentUser();
				UserGroupInformation proxyUserUgi = UserGroupInformation.CreateProxyUserForTesting
					(MiniUser, current, GroupNames);
				try
				{
					client = proxyUserUgi.DoAs(new _PrivilegedExceptionAction_212(this, addr, conf));
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail(Arrays.ToString(e.GetStackTrace()));
				}
			}
			finally
			{
				RPC.StopProxy(client);
			}
		}

		private sealed class _PrivilegedExceptionAction_212 : PrivilegedExceptionAction<MiniRPCBenchmark.MiniProtocol
			>
		{
			public _PrivilegedExceptionAction_212(MiniRPCBenchmark _enclosing, IPEndPoint addr
				, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public MiniRPCBenchmark.MiniProtocol Run()
			{
				MiniRPCBenchmark.MiniProtocol p = RPC.GetProxy<MiniRPCBenchmark.MiniProtocol>(MiniRPCBenchmark.MiniProtocol
					.versionID, addr, conf);
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token;
				token = p.GetDelegationToken(new Text(MiniRPCBenchmark.Renewer));
				this._enclosing.currentUgi = UserGroupInformation.CreateUserForTesting(MiniRPCBenchmark
					.MiniUser, MiniRPCBenchmark.GroupNames);
				SecurityUtil.SetTokenService(token, addr);
				this._enclosing.currentUgi.AddToken(token);
				return p;
			}

			private readonly MiniRPCBenchmark _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long ConnectToServerUsingDelegationToken(Configuration conf, IPEndPoint
			 addr)
		{
			MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				long start = Time.Now();
				try
				{
					client = currentUgi.DoAs(new _PrivilegedExceptionAction_240(addr, conf));
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				long end = Time.Now();
				return end - start;
			}
			finally
			{
				RPC.StopProxy(client);
			}
		}

		private sealed class _PrivilegedExceptionAction_240 : PrivilegedExceptionAction<MiniRPCBenchmark.MiniProtocol
			>
		{
			public _PrivilegedExceptionAction_240(IPEndPoint addr, Configuration conf)
			{
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public MiniRPCBenchmark.MiniProtocol Run()
			{
				return RPC.GetProxy<MiniRPCBenchmark.MiniProtocol>(MiniRPCBenchmark.MiniProtocol.
					versionID, addr, conf);
			}

			private readonly IPEndPoint addr;

			private readonly Configuration conf;
		}

		internal static void SetLoggingLevel(Level level)
		{
			LogManager.GetLogger(typeof(Server).FullName).SetLevel(level);
			((Log4JLogger)Server.Auditlog).GetLogger().SetLevel(level);
			LogManager.GetLogger(typeof(Client).FullName).SetLevel(level);
		}

		/// <summary>Run MiniBenchmark with MiniServer as the RPC server.</summary>
		/// <param name="conf">- configuration</param>
		/// <param name="count">- connect this many times</param>
		/// <param name="keytabKey">- key for keytab file in the configuration</param>
		/// <param name="userNameKey">- key for user name in the configuration</param>
		/// <returns>average time to connect</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long RunMiniBenchmark(Configuration conf, int count, string keytabKey
			, string userNameKey)
		{
			// get login information
			string user = Runtime.GetProperty("user.name");
			if (userNameKey != null)
			{
				user = conf.Get(userNameKey, user);
			}
			string keytabFile = null;
			if (keytabKey != null)
			{
				keytabFile = conf.Get(keytabKey, keytabFile);
			}
			MiniRPCBenchmark.MiniServer miniServer = null;
			try
			{
				// start the server
				miniServer = new MiniRPCBenchmark.MiniServer(conf, user, keytabFile);
				IPEndPoint addr = miniServer.GetAddress();
				ConnectToServer(conf, addr);
				// connect to the server count times
				SetLoggingLevel(logLevel);
				long elapsed = 0L;
				for (int idx = 0; idx < count; idx++)
				{
					elapsed += ConnectToServer(conf, addr);
				}
				return elapsed;
			}
			finally
			{
				if (miniServer != null)
				{
					miniServer.Stop();
				}
			}
		}

		/// <summary>Run MiniBenchmark using delegation token authentication.</summary>
		/// <param name="conf">- configuration</param>
		/// <param name="count">- connect this many times</param>
		/// <param name="keytabKey">- key for keytab file in the configuration</param>
		/// <param name="userNameKey">- key for user name in the configuration</param>
		/// <returns>average time to connect</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long RunMiniBenchmarkWithDelegationToken(Configuration conf, int
			 count, string keytabKey, string userNameKey)
		{
			// get login information
			string user = Runtime.GetProperty("user.name");
			if (userNameKey != null)
			{
				user = conf.Get(userNameKey, user);
			}
			string keytabFile = null;
			if (keytabKey != null)
			{
				keytabFile = conf.Get(keytabKey, keytabFile);
			}
			MiniRPCBenchmark.MiniServer miniServer = null;
			UserGroupInformation.SetConfiguration(conf);
			string shortUserName = UserGroupInformation.CreateRemoteUser(user).GetShortUserName
				();
			try
			{
				conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
					(shortUserName), GroupName1);
				ConfigureSuperUserIPAddresses(conf, shortUserName);
				// start the server
				miniServer = new MiniRPCBenchmark.MiniServer(conf, user, keytabFile);
				IPEndPoint addr = miniServer.GetAddress();
				ConnectToServerAndGetDelegationToken(conf, addr);
				// connect to the server count times
				SetLoggingLevel(logLevel);
				long elapsed = 0L;
				for (int idx = 0; idx < count; idx++)
				{
					elapsed += ConnectToServerUsingDelegationToken(conf, addr);
				}
				return elapsed;
			}
			finally
			{
				if (miniServer != null)
				{
					miniServer.Stop();
				}
			}
		}

		internal static void PrintUsage()
		{
			System.Console.Error.WriteLine("Usage: MiniRPCBenchmark <numIterations> [<keytabFile> [<userName> "
				 + "[useToken|useKerberos [<logLevel>]]]]");
			System.Environment.Exit(-1);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Console.Out.WriteLine("Benchmark: RPC session establishment.");
			if (args.Length < 1)
			{
				PrintUsage();
			}
			Configuration conf = new Configuration();
			int count = System.Convert.ToInt32(args[0]);
			if (args.Length > 1)
			{
				conf.Set(KeytabFileKey, args[1]);
			}
			if (args.Length > 2)
			{
				conf.Set(UserNameKey, args[2]);
			}
			bool useDelegationToken = false;
			if (args.Length > 3)
			{
				useDelegationToken = Sharpen.Runtime.EqualsIgnoreCase(args[3], "useToken");
			}
			Level l = Level.Error;
			if (args.Length > 4)
			{
				l = Level.ToLevel(args[4]);
			}
			MiniRPCBenchmark mb = new MiniRPCBenchmark(l);
			long elapsedTime = 0;
			if (useDelegationToken)
			{
				System.Console.Out.WriteLine("Running MiniRPCBenchmark with delegation token authentication."
					);
				elapsedTime = mb.RunMiniBenchmarkWithDelegationToken(conf, count, KeytabFileKey, 
					UserNameKey);
			}
			else
			{
				string auth = SecurityUtil.GetAuthenticationMethod(conf).ToString();
				System.Console.Out.WriteLine("Running MiniRPCBenchmark with " + auth + " authentication."
					);
				elapsedTime = mb.RunMiniBenchmark(conf, count, KeytabFileKey, UserNameKey);
			}
			System.Console.Out.WriteLine(VersionInfo.GetVersion());
			System.Console.Out.WriteLine("Number  of  connects: " + count);
			System.Console.Out.WriteLine("Average connect time: " + ((double)elapsedTime / count
				));
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
			builder.Append(Sharpen.Runtime.GetLocalHost().ToString());
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(superUserShortName), builder.ToString());
		}
	}
}
