using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	/// <summary>Unit tests for using Job Token over RPC.</summary>
	/// <remarks>
	/// Unit tests for using Job Token over RPC.
	/// System properties required:
	/// -Djava.security.krb5.conf=.../hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/target/test-classes/krb5.conf
	/// -Djava.net.preferIPv4Stack=true
	/// </remarks>
	public class TestUmbilicalProtocolWithJobToken
	{
		private const string Address = "0.0.0.0";

		public static readonly Log Log = LogFactory.GetLog(typeof(TestUmbilicalProtocolWithJobToken
			));

		private static Configuration conf;

		static TestUmbilicalProtocolWithJobToken()
		{
			conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
		}

		static TestUmbilicalProtocolWithJobToken()
		{
			((Log4JLogger)Client.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)Server.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcClient.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcServer.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslInputStream.Log).GetLogger().SetLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobTokenRpc()
		{
			TaskUmbilicalProtocol mockTT = Org.Mockito.Mockito.Mock<TaskUmbilicalProtocol>();
			Org.Mockito.Mockito.DoReturn(TaskUmbilicalProtocol.versionID).When(mockTT).GetProtocolVersion
				(Matchers.AnyString(), Matchers.AnyLong());
			Org.Mockito.Mockito.DoReturn(ProtocolSignature.GetProtocolSignature(mockTT, typeof(
				TaskUmbilicalProtocol).FullName, TaskUmbilicalProtocol.versionID, 0)).When(mockTT
				).GetProtocolSignature(Matchers.AnyString(), Matchers.AnyLong(), Matchers.AnyInt
				());
			JobTokenSecretManager sm = new JobTokenSecretManager();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TaskUmbilicalProtocol)).
				SetInstance(mockTT).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).SetSecretManager(sm).Build();
			server.Start();
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			string jobId = current.GetUserName();
			JobTokenIdentifier tokenId = new JobTokenIdentifier(new Text(jobId));
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>(tokenId, sm);
			sm.AddTokenForJob(jobId, token);
			SecurityUtil.SetTokenService(token, addr);
			Log.Info("Service address for token is " + token.GetService());
			current.AddToken(token);
			current.DoAs(new _PrivilegedExceptionAction_110(addr, server));
		}

		private sealed class _PrivilegedExceptionAction_110 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_110(IPEndPoint addr, Server server)
			{
				this.addr = addr;
				this.server = server;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				TaskUmbilicalProtocol proxy = null;
				try
				{
					proxy = (TaskUmbilicalProtocol)RPC.GetProxy<TaskUmbilicalProtocol>(TaskUmbilicalProtocol
						.versionID, addr, TestUmbilicalProtocolWithJobToken.conf);
					proxy.Ping(null);
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

			private readonly Server server;
		}
	}
}
