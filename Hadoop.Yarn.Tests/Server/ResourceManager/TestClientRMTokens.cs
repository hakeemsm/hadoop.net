using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestClientRMTokens
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClientRMTokens));

		[SetUp]
		public virtual void ResetSecretManager()
		{
			RMDelegationTokenIdentifier.Renewer.SetSecretManager(null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationToken()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmPrincipal, "testuser/localhost@apache.org");
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			ResourceScheduler scheduler = CreateMockScheduler(conf);
			long initialInterval = 10000l;
			long maxLifetime = 20000l;
			long renewInterval = 10000l;
			RMDelegationTokenSecretManager rmDtSecretManager = CreateRMDelegationTokenSecretManager
				(initialInterval, maxLifetime, renewInterval);
			rmDtSecretManager.StartThreads();
			Log.Info("Creating DelegationTokenSecretManager with initialInterval: " + initialInterval
				 + ", maxLifetime: " + maxLifetime + ", renewInterval: " + renewInterval);
			ClientRMService clientRMService = new TestClientRMTokens.ClientRMServiceForTest(this
				, conf, scheduler, rmDtSecretManager);
			clientRMService.Init(conf);
			clientRMService.Start();
			ApplicationClientProtocol clientRMWithDT = null;
			try
			{
				// Create a user for the renewr and fake the authentication-method
				UserGroupInformation loggedInUser = UserGroupInformation.CreateRemoteUser("testrenewer@APACHE.ORG"
					);
				NUnit.Framework.Assert.AreEqual("testrenewer", loggedInUser.GetShortUserName());
				// Default realm is APACHE.ORG
				loggedInUser.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
					);
				Token token = GetDelegationToken(loggedInUser, clientRMService, loggedInUser.GetShortUserName
					());
				long tokenFetchTime = Runtime.CurrentTimeMillis();
				Log.Info("Got delegation token at: " + tokenFetchTime);
				// Now try talking to RMService using the delegation token
				clientRMWithDT = GetClientRMProtocolWithDT(token, clientRMService.GetBindAddress(
					), "loginuser1", conf);
				GetNewApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
					GetNewApplicationRequest>();
				try
				{
					clientRMWithDT.GetNewApplication(request);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				// Renew after 50% of token age.
				while (Runtime.CurrentTimeMillis() < tokenFetchTime + initialInterval / 2)
				{
					Sharpen.Thread.Sleep(500l);
				}
				long nextExpTime = RenewDelegationToken(loggedInUser, clientRMService, token);
				long renewalTime = Runtime.CurrentTimeMillis();
				Log.Info("Renewed token at: " + renewalTime + ", NextExpiryTime: " + nextExpTime);
				// Wait for first expiry, but before renewed expiry.
				while (Runtime.CurrentTimeMillis() > tokenFetchTime + initialInterval && Runtime.
					CurrentTimeMillis() < nextExpTime)
				{
					Sharpen.Thread.Sleep(500l);
				}
				Sharpen.Thread.Sleep(50l);
				// Valid token because of renewal.
				try
				{
					clientRMWithDT.GetNewApplication(request);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				// Wait for expiry.
				while (Runtime.CurrentTimeMillis() < renewalTime + renewInterval)
				{
					Sharpen.Thread.Sleep(500l);
				}
				Sharpen.Thread.Sleep(50l);
				Log.Info("At time: " + Runtime.CurrentTimeMillis() + ", token should be invalid");
				// Token should have expired.      
				try
				{
					clientRMWithDT.GetNewApplication(request);
					NUnit.Framework.Assert.Fail("Should not have succeeded with an expired token");
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.AreEqual(typeof(SecretManager.InvalidToken).FullName, e.GetType
						().FullName);
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("is expired"));
				}
				// Test cancellation
				// Stop the existing proxy, start another.
				if (clientRMWithDT != null)
				{
					RPC.StopProxy(clientRMWithDT);
					clientRMWithDT = null;
				}
				token = GetDelegationToken(loggedInUser, clientRMService, loggedInUser.GetShortUserName
					());
				tokenFetchTime = Runtime.CurrentTimeMillis();
				Log.Info("Got delegation token at: " + tokenFetchTime);
				// Now try talking to RMService using the delegation token
				clientRMWithDT = GetClientRMProtocolWithDT(token, clientRMService.GetBindAddress(
					), "loginuser2", conf);
				request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetNewApplicationRequest>
					();
				try
				{
					clientRMWithDT.GetNewApplication(request);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				CancelDelegationToken(loggedInUser, clientRMService, token);
				if (clientRMWithDT != null)
				{
					RPC.StopProxy(clientRMWithDT);
					clientRMWithDT = null;
				}
				// Creating a new connection.
				clientRMWithDT = GetClientRMProtocolWithDT(token, clientRMService.GetBindAddress(
					), "loginuser2", conf);
				Log.Info("Cancelled delegation token at: " + Runtime.CurrentTimeMillis());
				// Verify cancellation worked.
				try
				{
					clientRMWithDT.GetNewApplication(request);
					NUnit.Framework.Assert.Fail("Should not have succeeded with a cancelled delegation token"
						);
				}
				catch (IOException)
				{
				}
				catch (YarnException)
				{
				}
				// Test new version token
				// Stop the existing proxy, start another.
				if (clientRMWithDT != null)
				{
					RPC.StopProxy(clientRMWithDT);
					clientRMWithDT = null;
				}
				token = GetDelegationToken(loggedInUser, clientRMService, loggedInUser.GetShortUserName
					());
				byte[] tokenIdentifierContent = ((byte[])token.GetIdentifier().Array());
				RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier();
				DataInputBuffer dib = new DataInputBuffer();
				dib.Reset(tokenIdentifierContent, tokenIdentifierContent.Length);
				tokenIdentifier.ReadFields(dib);
				// Construct new version RMDelegationTokenIdentifier with additional field
				RMDelegationTokenIdentifierForTest newVersionTokenIdentifier = new RMDelegationTokenIdentifierForTest
					(tokenIdentifier, "message");
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> newRMDTtoken = 
					new Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>(newVersionTokenIdentifier
					, rmDtSecretManager);
				Org.Apache.Hadoop.Yarn.Api.Records.Token newToken = BuilderUtils.NewDelegationToken
					(newRMDTtoken.GetIdentifier(), newRMDTtoken.GetKind().ToString(), newRMDTtoken.GetPassword
					(), newRMDTtoken.GetService().ToString());
				// Now try talking to RMService using the new version delegation token
				clientRMWithDT = GetClientRMProtocolWithDT(newToken, clientRMService.GetBindAddress
					(), "loginuser3", conf);
				request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetNewApplicationRequest>
					();
				try
				{
					clientRMWithDT.GetNewApplication(request);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.Fail("Unexpected exception" + e);
				}
			}
			finally
			{
				rmDtSecretManager.StopThreads();
				// TODO PRECOMMIT Close proxies.
				if (clientRMWithDT != null)
				{
					RPC.StopProxy(clientRMWithDT);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitRenewCancel()
		{
			IPEndPoint addr = NetUtils.CreateSocketAddr(Sharpen.Runtime.GetLocalHost().GetHostName
				(), 123, null);
			CheckShortCircuitRenewCancel(addr, addr, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitRenewCancelWildcardAddress()
		{
			IPEndPoint rmAddr = new IPEndPoint(123);
			IPEndPoint serviceAddr = NetUtils.CreateSocketAddr(Sharpen.Runtime.GetLocalHost()
				.GetHostName(), rmAddr.Port, null);
			CheckShortCircuitRenewCancel(rmAddr, serviceAddr, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitRenewCancelSameHostDifferentPort()
		{
			IPEndPoint rmAddr = NetUtils.CreateSocketAddr(Sharpen.Runtime.GetLocalHost().GetHostName
				(), 123, null);
			CheckShortCircuitRenewCancel(rmAddr, new IPEndPoint(rmAddr.Address, rmAddr.Port +
				 1), false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitRenewCancelDifferentHostSamePort()
		{
			IPEndPoint rmAddr = NetUtils.CreateSocketAddr(Sharpen.Runtime.GetLocalHost().GetHostName
				(), 123, null);
			CheckShortCircuitRenewCancel(rmAddr, new IPEndPoint("1.1.1.1", rmAddr.Port), false
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitRenewCancelDifferentHostDifferentPort()
		{
			IPEndPoint rmAddr = NetUtils.CreateSocketAddr(Sharpen.Runtime.GetLocalHost().GetHostName
				(), 123, null);
			CheckShortCircuitRenewCancel(rmAddr, new IPEndPoint("1.1.1.1", rmAddr.Port + 1), 
				false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CheckShortCircuitRenewCancel(IPEndPoint rmAddr, IPEndPoint serviceAddr
			, bool shouldShortCircuit)
		{
			Configuration conf = new Configuration();
			conf.SetClass(YarnConfiguration.IpcRpcImpl, typeof(TestClientRMTokens.YarnBadRPC)
				, typeof(YarnRPC));
			RMDelegationTokenSecretManager secretManager = Org.Mockito.Mockito.Mock<RMDelegationTokenSecretManager
				>();
			RMDelegationTokenIdentifier.Renewer.SetSecretManager(secretManager, rmAddr);
			RMDelegationTokenIdentifier ident = new RMDelegationTokenIdentifier(new Text("owner"
				), new Text("renewer"), null);
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier>(ident, secretManager);
			SecurityUtil.SetTokenService(token, serviceAddr);
			if (shouldShortCircuit)
			{
				token.Renew(conf);
				Org.Mockito.Mockito.Verify(secretManager).RenewToken(Matchers.Eq(token), Matchers.Eq
					("renewer"));
				Org.Mockito.Mockito.Reset(secretManager);
				token.Cancel(conf);
				Org.Mockito.Mockito.Verify(secretManager).CancelToken(Matchers.Eq(token), Matchers.Eq
					("renewer"));
			}
			else
			{
				try
				{
					token.Renew(conf);
					NUnit.Framework.Assert.Fail();
				}
				catch (RuntimeException e)
				{
					NUnit.Framework.Assert.AreEqual("getProxy", e.Message);
				}
				Org.Mockito.Mockito.Verify(secretManager, Org.Mockito.Mockito.Never()).RenewToken
					(Matchers.Any<Org.Apache.Hadoop.Security.Token.Token>(), Matchers.AnyString());
				try
				{
					token.Cancel(conf);
					NUnit.Framework.Assert.Fail();
				}
				catch (RuntimeException e)
				{
					NUnit.Framework.Assert.AreEqual("getProxy", e.Message);
				}
				Org.Mockito.Mockito.Verify(secretManager, Org.Mockito.Mockito.Never()).CancelToken
					(Matchers.Any<Org.Apache.Hadoop.Security.Token.Token>(), Matchers.AnyString());
			}
		}

		public class YarnBadRPC : YarnRPC
		{
			public override object GetProxy(Type protocol, IPEndPoint addr, Configuration conf
				)
			{
				throw new RuntimeException("getProxy");
			}

			public override void StopProxy(object proxy, Configuration conf)
			{
				throw new RuntimeException("stopProxy");
			}

			public override Org.Apache.Hadoop.Ipc.Server GetServer<_T0>(Type protocol, object
				 instance, IPEndPoint addr, Configuration conf, SecretManager<_T0> secretManager
				, int numHandlers, string portRangeConfig)
			{
				throw new RuntimeException("getServer");
			}
		}

		// Get the delegation token directly as it is a little difficult to setup
		// the kerberos based rpc.
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Org.Apache.Hadoop.Yarn.Api.Records.Token GetDelegationToken(UserGroupInformation
			 loggedInUser, ApplicationClientProtocol clientRMService, string renewerString)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Token token = loggedInUser.DoAs(new _PrivilegedExceptionAction_405
				(renewerString, clientRMService));
			return token;
		}

		private sealed class _PrivilegedExceptionAction_405 : PrivilegedExceptionAction<Org.Apache.Hadoop.Yarn.Api.Records.Token
			>
		{
			public _PrivilegedExceptionAction_405(string renewerString, ApplicationClientProtocol
				 clientRMService)
			{
				this.renewerString = renewerString;
				this.clientRMService = clientRMService;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Yarn.Api.Records.Token Run()
			{
				GetDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetDelegationTokenRequest>();
				request.SetRenewer(renewerString);
				return clientRMService.GetDelegationToken(request).GetRMDelegationToken();
			}

			private readonly string renewerString;

			private readonly ApplicationClientProtocol clientRMService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private long RenewDelegationToken(UserGroupInformation loggedInUser, ApplicationClientProtocol
			 clientRMService, Org.Apache.Hadoop.Yarn.Api.Records.Token dToken)
		{
			long nextExpTime = loggedInUser.DoAs(new _PrivilegedExceptionAction_423(dToken, clientRMService
				));
			return nextExpTime;
		}

		private sealed class _PrivilegedExceptionAction_423 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_423(Org.Apache.Hadoop.Yarn.Api.Records.Token dToken
				, ApplicationClientProtocol clientRMService)
			{
				this.dToken = dToken;
				this.clientRMService = clientRMService;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public long Run()
			{
				RenewDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<RenewDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				return clientRMService.RenewDelegationToken(request).GetNextExpirationTime();
			}

			private readonly Org.Apache.Hadoop.Yarn.Api.Records.Token dToken;

			private readonly ApplicationClientProtocol clientRMService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CancelDelegationToken(UserGroupInformation loggedInUser, ApplicationClientProtocol
			 clientRMService, Org.Apache.Hadoop.Yarn.Api.Records.Token dToken)
		{
			loggedInUser.DoAs(new _PrivilegedExceptionAction_440(dToken, clientRMService));
		}

		private sealed class _PrivilegedExceptionAction_440 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_440(Org.Apache.Hadoop.Yarn.Api.Records.Token dToken
				, ApplicationClientProtocol clientRMService)
			{
				this.dToken = dToken;
				this.clientRMService = clientRMService;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				CancelDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<CancelDelegationTokenRequest>();
				request.SetDelegationToken(dToken);
				clientRMService.CancelDelegationToken(request);
				return null;
			}

			private readonly Org.Apache.Hadoop.Yarn.Api.Records.Token dToken;

			private readonly ApplicationClientProtocol clientRMService;
		}

		private ApplicationClientProtocol GetClientRMProtocolWithDT(Org.Apache.Hadoop.Yarn.Api.Records.Token
			 token, IPEndPoint rmAddress, string user, Configuration conf)
		{
			// Maybe consider converting to Hadoop token, serialize de-serialize etc
			// before trying to renew the token.
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			ugi.AddToken(ConverterUtils.ConvertFromYarn(token, rmAddress));
			YarnRPC rpc = YarnRPC.Create(conf);
			ApplicationClientProtocol clientRMWithDT = ugi.DoAs(new _PrivilegedAction_464(rpc
				, rmAddress, conf));
			return clientRMWithDT;
		}

		private sealed class _PrivilegedAction_464 : PrivilegedAction<ApplicationClientProtocol
			>
		{
			public _PrivilegedAction_464(YarnRPC rpc, IPEndPoint rmAddress, Configuration conf
				)
			{
				this.rpc = rpc;
				this.rmAddress = rmAddress;
				this.conf = conf;
			}

			public ApplicationClientProtocol Run()
			{
				return (ApplicationClientProtocol)rpc.GetProxy(typeof(ApplicationClientProtocol), 
					rmAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint rmAddress;

			private readonly Configuration conf;
		}

		internal class ClientRMServiceForTest : ClientRMService
		{
			public ClientRMServiceForTest(TestClientRMTokens _enclosing, Configuration conf, 
				ResourceScheduler scheduler, RMDelegationTokenSecretManager rmDTSecretManager)
				: base(Org.Mockito.Mockito.Mock<RMContext>(), scheduler, Org.Mockito.Mockito.Mock
					<RMAppManager>(), new ApplicationACLsManager(conf), new QueueACLsManager(scheduler
					, conf), rmDTSecretManager)
			{
				this._enclosing = _enclosing;
			}

			// Use a random port unless explicitly specified.
			internal override IPEndPoint GetBindAddress(Configuration conf)
			{
				return conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
					, 0);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				if (this.rmDTSecretManager != null)
				{
					this.rmDTSecretManager.StopThreads();
				}
				base.ServiceStop();
			}

			private readonly TestClientRMTokens _enclosing;
		}

		private static ResourceScheduler CreateMockScheduler(Configuration conf)
		{
			ResourceScheduler mockSched = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(512, 0)).When(mockSched).GetMinimumResourceCapability
				();
			Org.Mockito.Mockito.DoReturn(BuilderUtils.NewResource(5120, 0)).When(mockSched).GetMaximumResourceCapability
				();
			return mockSched;
		}

		private static RMDelegationTokenSecretManager CreateRMDelegationTokenSecretManager
			(long secretKeyInterval, long tokenMaxLifetime, long tokenRenewInterval)
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(new NullRMStateStore
				());
			RMDelegationTokenSecretManager rmDtSecretManager = new RMDelegationTokenSecretManager
				(secretKeyInterval, tokenMaxLifetime, tokenRenewInterval, 3600000, rmContext);
			return rmDtSecretManager;
		}
	}
}
