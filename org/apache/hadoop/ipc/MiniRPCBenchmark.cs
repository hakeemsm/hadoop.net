using Sharpen;

namespace org.apache.hadoop.ipc
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
	/// <see cref="org.apache.log4j.Level"/>
	/// </li>
	/// </ul>
	/// </remarks>
	public class MiniRPCBenchmark
	{
		private const string KEYTAB_FILE_KEY = "test.keytab.file";

		private const string USER_NAME_KEY = "test.user.name";

		private const string MINI_USER = "miniUser";

		private const string RENEWER = "renewer";

		private const string GROUP_NAME_1 = "MiniGroup1";

		private const string GROUP_NAME_2 = "MiniGroup2";

		private static readonly string[] GROUP_NAMES = new string[] { GROUP_NAME_1, GROUP_NAME_2
			 };

		private org.apache.hadoop.security.UserGroupInformation currentUgi;

		private org.apache.log4j.Level logLevel;

		internal MiniRPCBenchmark(org.apache.log4j.Level l)
		{
			currentUgi = null;
			logLevel = l;
		}

		public class TestDelegationTokenSelector : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector
			<org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
			>
		{
			protected internal TestDelegationTokenSelector()
				: base(new org.apache.hadoop.io.Text("MY KIND"))
			{
			}
		}

		public abstract class MiniProtocol : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 1L;

			/// <summary>Get a Delegation Token.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
				> getDelegationToken(org.apache.hadoop.io.Text renewer);
		}

		public static class MiniProtocolConstants
		{
		}

		/// <summary>
		/// Primitive RPC server, which
		/// allows clients to connect to it.
		/// </summary>
		internal class MiniServer : org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
		{
			private const string DEFAULT_SERVER_ADDRESS = "0.0.0.0";

			private org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenSecretManager
				 secretManager;

			private org.apache.hadoop.ipc.Server rpcServer;

			/// <exception cref="System.IO.IOException"/>
			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				// VersionedProtocol
				if (protocol.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
					)).getName()))
				{
					return versionID;
				}
				throw new System.IO.IOException("Unknown protocol: " + protocol);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int clientMethodsHashCode)
			{
				// VersionedProtocol
				if (protocol.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
					)).getName()))
				{
					return new org.apache.hadoop.ipc.ProtocolSignature(versionID, null);
				}
				throw new System.IO.IOException("Unknown protocol: " + protocol);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
				> getDelegationToken(org.apache.hadoop.io.Text renewer)
			{
				// MiniProtocol
				string owner = org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getUserName
					();
				string realUser = org.apache.hadoop.security.UserGroupInformation.getCurrentUser(
					).getRealUser() == null ? string.Empty : org.apache.hadoop.security.UserGroupInformation
					.getCurrentUser().getRealUser().getUserName();
				org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
					 tokenId = new org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
					(new org.apache.hadoop.io.Text(owner), renewer, new org.apache.hadoop.io.Text(realUser
					));
				return new org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
					>(tokenId, secretManager);
			}

			/// <summary>Start RPC server</summary>
			/// <exception cref="System.IO.IOException"/>
			internal MiniServer(org.apache.hadoop.conf.Configuration conf, string user, string
				 keytabFile)
			{
				org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
				org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(user, keytabFile
					);
				secretManager = new org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenSecretManager
					(24 * 60 * 60 * 1000, 7 * 24 * 60 * 60 * 1000, 24 * 60 * 60 * 1000, 3600000);
				secretManager.startThreads();
				rpcServer = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol))).setInstance(this)
					.setBindAddress(DEFAULT_SERVER_ADDRESS).setPort(0).setNumHandlers(1).setVerbose(
					false).setSecretManager(secretManager).build();
				rpcServer.start();
			}

			/// <summary>Stop RPC server</summary>
			internal virtual void stop()
			{
				if (rpcServer != null)
				{
					rpcServer.stop();
				}
				rpcServer = null;
			}

			/// <summary>Get RPC server address</summary>
			internal virtual java.net.InetSocketAddress getAddress()
			{
				if (rpcServer == null)
				{
					return null;
				}
				return org.apache.hadoop.net.NetUtils.getConnectAddress(rpcServer);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long connectToServer(org.apache.hadoop.conf.Configuration conf, 
			java.net.InetSocketAddress addr)
		{
			org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				long start = org.apache.hadoop.util.Time.now();
				client = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
					>(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol.versionID, addr, conf);
				long end = org.apache.hadoop.util.Time.now();
				return end - start;
			}
			finally
			{
				org.apache.hadoop.ipc.RPC.stopProxy(client);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void connectToServerAndGetDelegationToken(org.apache.hadoop.conf.Configuration
			 conf, java.net.InetSocketAddress addr)
		{
			org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				org.apache.hadoop.security.UserGroupInformation current = org.apache.hadoop.security.UserGroupInformation
					.getCurrentUser();
				org.apache.hadoop.security.UserGroupInformation proxyUserUgi = org.apache.hadoop.security.UserGroupInformation
					.createProxyUserForTesting(MINI_USER, current, GROUP_NAMES);
				try
				{
					client = proxyUserUgi.doAs(new _PrivilegedExceptionAction_212(this, addr, conf));
				}
				catch (System.Exception e)
				{
					NUnit.Framework.Assert.Fail(java.util.Arrays.toString(e.getStackTrace()));
				}
			}
			finally
			{
				org.apache.hadoop.ipc.RPC.stopProxy(client);
			}
		}

		private sealed class _PrivilegedExceptionAction_212 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol>
		{
			public _PrivilegedExceptionAction_212(MiniRPCBenchmark _enclosing, java.net.InetSocketAddress
				 addr, org.apache.hadoop.conf.Configuration conf)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol run()
			{
				org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol p = org.apache.hadoop.ipc.RPC
					.getProxy<org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol>(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
					.versionID, addr, conf);
				org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.TestDelegationToken.TestDelegationTokenIdentifier
					> token;
				token = p.getDelegationToken(new org.apache.hadoop.io.Text(org.apache.hadoop.ipc.MiniRPCBenchmark
					.RENEWER));
				this._enclosing.currentUgi = org.apache.hadoop.security.UserGroupInformation.createUserForTesting
					(org.apache.hadoop.ipc.MiniRPCBenchmark.MINI_USER, org.apache.hadoop.ipc.MiniRPCBenchmark
					.GROUP_NAMES);
				org.apache.hadoop.security.SecurityUtil.setTokenService(token, addr);
				this._enclosing.currentUgi.addToken(token);
				return p;
			}

			private readonly MiniRPCBenchmark _enclosing;

			private readonly java.net.InetSocketAddress addr;

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long connectToServerUsingDelegationToken(org.apache.hadoop.conf.Configuration
			 conf, java.net.InetSocketAddress addr)
		{
			org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol client = null;
			try
			{
				long start = org.apache.hadoop.util.Time.now();
				try
				{
					client = currentUgi.doAs(new _PrivilegedExceptionAction_240(addr, conf));
				}
				catch (System.Exception e)
				{
					Sharpen.Runtime.printStackTrace(e);
				}
				long end = org.apache.hadoop.util.Time.now();
				return end - start;
			}
			finally
			{
				org.apache.hadoop.ipc.RPC.stopProxy(client);
			}
		}

		private sealed class _PrivilegedExceptionAction_240 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol>
		{
			public _PrivilegedExceptionAction_240(java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol run()
			{
				return org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol
					>(org.apache.hadoop.ipc.MiniRPCBenchmark.MiniProtocol.versionID, addr, conf);
			}

			private readonly java.net.InetSocketAddress addr;

			private readonly org.apache.hadoop.conf.Configuration conf;
		}

		internal static void setLoggingLevel(org.apache.log4j.Level level)
		{
			org.apache.log4j.LogManager.getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.Server
				)).getName()).setLevel(level);
			((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.ipc.Server.AUDITLOG
				).getLogger().setLevel(level);
			org.apache.log4j.LogManager.getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.Client
				)).getName()).setLevel(level);
		}

		/// <summary>Run MiniBenchmark with MiniServer as the RPC server.</summary>
		/// <param name="conf">- configuration</param>
		/// <param name="count">- connect this many times</param>
		/// <param name="keytabKey">- key for keytab file in the configuration</param>
		/// <param name="userNameKey">- key for user name in the configuration</param>
		/// <returns>average time to connect</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long runMiniBenchmark(org.apache.hadoop.conf.Configuration conf, 
			int count, string keytabKey, string userNameKey)
		{
			// get login information
			string user = Sharpen.Runtime.getProperty("user.name");
			if (userNameKey != null)
			{
				user = conf.get(userNameKey, user);
			}
			string keytabFile = null;
			if (keytabKey != null)
			{
				keytabFile = conf.get(keytabKey, keytabFile);
			}
			org.apache.hadoop.ipc.MiniRPCBenchmark.MiniServer miniServer = null;
			try
			{
				// start the server
				miniServer = new org.apache.hadoop.ipc.MiniRPCBenchmark.MiniServer(conf, user, keytabFile
					);
				java.net.InetSocketAddress addr = miniServer.getAddress();
				connectToServer(conf, addr);
				// connect to the server count times
				setLoggingLevel(logLevel);
				long elapsed = 0L;
				for (int idx = 0; idx < count; idx++)
				{
					elapsed += connectToServer(conf, addr);
				}
				return elapsed;
			}
			finally
			{
				if (miniServer != null)
				{
					miniServer.stop();
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
		internal virtual long runMiniBenchmarkWithDelegationToken(org.apache.hadoop.conf.Configuration
			 conf, int count, string keytabKey, string userNameKey)
		{
			// get login information
			string user = Sharpen.Runtime.getProperty("user.name");
			if (userNameKey != null)
			{
				user = conf.get(userNameKey, user);
			}
			string keytabFile = null;
			if (keytabKey != null)
			{
				keytabFile = conf.get(keytabKey, keytabFile);
			}
			org.apache.hadoop.ipc.MiniRPCBenchmark.MiniServer miniServer = null;
			org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
			string shortUserName = org.apache.hadoop.security.UserGroupInformation.createRemoteUser
				(user).getShortUserName();
			try
			{
				conf.setStrings(org.apache.hadoop.security.authorize.DefaultImpersonationProvider
					.getTestProvider().getProxySuperuserGroupConfKey(shortUserName), GROUP_NAME_1);
				configureSuperUserIPAddresses(conf, shortUserName);
				// start the server
				miniServer = new org.apache.hadoop.ipc.MiniRPCBenchmark.MiniServer(conf, user, keytabFile
					);
				java.net.InetSocketAddress addr = miniServer.getAddress();
				connectToServerAndGetDelegationToken(conf, addr);
				// connect to the server count times
				setLoggingLevel(logLevel);
				long elapsed = 0L;
				for (int idx = 0; idx < count; idx++)
				{
					elapsed += connectToServerUsingDelegationToken(conf, addr);
				}
				return elapsed;
			}
			finally
			{
				if (miniServer != null)
				{
					miniServer.stop();
				}
			}
		}

		internal static void printUsage()
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
				printUsage();
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			int count = System.Convert.ToInt32(args[0]);
			if (args.Length > 1)
			{
				conf.set(KEYTAB_FILE_KEY, args[1]);
			}
			if (args.Length > 2)
			{
				conf.set(USER_NAME_KEY, args[2]);
			}
			bool useDelegationToken = false;
			if (args.Length > 3)
			{
				useDelegationToken = Sharpen.Runtime.equalsIgnoreCase(args[3], "useToken");
			}
			org.apache.log4j.Level l = org.apache.log4j.Level.ERROR;
			if (args.Length > 4)
			{
				l = org.apache.log4j.Level.toLevel(args[4]);
			}
			org.apache.hadoop.ipc.MiniRPCBenchmark mb = new org.apache.hadoop.ipc.MiniRPCBenchmark
				(l);
			long elapsedTime = 0;
			if (useDelegationToken)
			{
				System.Console.Out.WriteLine("Running MiniRPCBenchmark with delegation token authentication."
					);
				elapsedTime = mb.runMiniBenchmarkWithDelegationToken(conf, count, KEYTAB_FILE_KEY
					, USER_NAME_KEY);
			}
			else
			{
				string auth = org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod(conf
					).ToString();
				System.Console.Out.WriteLine("Running MiniRPCBenchmark with " + auth + " authentication."
					);
				elapsedTime = mb.runMiniBenchmark(conf, count, KEYTAB_FILE_KEY, USER_NAME_KEY);
			}
			System.Console.Out.WriteLine(org.apache.hadoop.util.VersionInfo.getVersion());
			System.Console.Out.WriteLine("Number  of  connects: " + count);
			System.Console.Out.WriteLine("Average connect time: " + ((double)elapsedTime / count
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private void configureSuperUserIPAddresses(org.apache.hadoop.conf.Configuration conf
			, string superUserShortName)
		{
			System.Collections.Generic.List<string> ipList = new System.Collections.Generic.List
				<string>();
			java.util.Enumeration<java.net.NetworkInterface> netInterfaceList = java.net.NetworkInterface
				.getNetworkInterfaces();
			while (netInterfaceList.MoveNext())
			{
				java.net.NetworkInterface inf = netInterfaceList.Current;
				java.util.Enumeration<java.net.InetAddress> addrList = inf.getInetAddresses();
				while (addrList.MoveNext())
				{
					java.net.InetAddress addr = addrList.Current;
					ipList.add(addr.getHostAddress());
				}
			}
			java.lang.StringBuilder builder = new java.lang.StringBuilder();
			foreach (string ip in ipList)
			{
				builder.Append(ip);
				builder.Append(',');
			}
			builder.Append("127.0.1.1,");
			builder.Append(java.net.InetAddress.getLocalHost().getCanonicalHostName());
			conf.setStrings(org.apache.hadoop.security.authorize.DefaultImpersonationProvider
				.getTestProvider().getProxySuperuserIpConfKey(superUserShortName), builder.ToString
				());
		}
	}
}
