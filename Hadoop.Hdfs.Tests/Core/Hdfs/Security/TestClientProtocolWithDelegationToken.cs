using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security
{
	/// <summary>Unit tests for using Delegation Token over RPC.</summary>
	public class TestClientProtocolWithDelegationToken
	{
		private const string Address = "0.0.0.0";

		public static readonly Log Log = LogFactory.GetLog(typeof(TestClientProtocolWithDelegationToken
			));

		private static readonly Configuration conf;

		static TestClientProtocolWithDelegationToken()
		{
			conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
		}

		static TestClientProtocolWithDelegationToken()
		{
			((Log4JLogger)Client.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)Server.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcClient.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcServer.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslInputStream.Log).GetLogger().SetLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenRpc()
		{
			ClientProtocol mockNN = Org.Mockito.Mockito.Mock<ClientProtocol>();
			FSNamesystem mockNameSys = Org.Mockito.Mockito.Mock<FSNamesystem>();
			DelegationTokenSecretManager sm = new DelegationTokenSecretManager(DFSConfigKeys.
				DfsNamenodeDelegationKeyUpdateIntervalDefault, DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalDefault
				, DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeDefault, 3600000, mockNameSys
				);
			sm.StartThreads();
			Org.Apache.Hadoop.Ipc.Server server = new RPC.Builder(conf).SetProtocol(typeof(ClientProtocol
				)).SetInstance(mockNN).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).SetSecretManager(sm).Build();
			server.Start();
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			string user = current.GetUserName();
			Text owner = new Text(user);
			DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner, owner, null
				);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>(dtId, sm);
			SecurityUtil.SetTokenService(token, addr);
			Log.Info("Service for token is " + token.GetService());
			current.AddToken(token);
			current.DoAs(new _PrivilegedExceptionAction_100(addr, server));
		}

		private sealed class _PrivilegedExceptionAction_100 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_100(IPEndPoint addr, Org.Apache.Hadoop.Ipc.Server
				 server)
			{
				this.addr = addr;
				this.server = server;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				ClientProtocol proxy = null;
				try
				{
					proxy = RPC.GetProxy<ClientProtocol>(ClientProtocol.versionID, addr, TestClientProtocolWithDelegationToken
						.conf);
					proxy.GetServerDefaults();
				}
				finally
				{
					server.Stop();
					if (proxy != null)
					{
						RPC.StopProxy(proxy);
					}
				}
				return null;
			}

			private readonly IPEndPoint addr;

			private readonly Org.Apache.Hadoop.Ipc.Server server;
		}
	}
}
