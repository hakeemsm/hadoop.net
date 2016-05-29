using System;
using System.Net;
using Javax.Security.Sasl;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class TestClientToAMTokens : ParameterizedSchedulerTestBase
	{
		private YarnConfiguration conf;

		public TestClientToAMTokens(ParameterizedSchedulerTestBase.SchedulerType type)
			: base(type)
		{
		}

		[SetUp]
		public virtual void Setup()
		{
			conf = GetConf();
		}

		private abstract class CustomProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public abstract void Ping();
		}

		private static class CustomProtocolConstants
		{
		}

		private class CustomSecurityInfo : SecurityInfo
		{
			public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
			{
				return new _TokenInfo_107();
			}

			private sealed class _TokenInfo_107 : TokenInfo
			{
				public _TokenInfo_107()
				{
				}

				public Type AnnotationType()
				{
					return null;
				}

				public Type Value()
				{
					return typeof(ClientToAMTokenSelector);
				}
			}

			public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
			{
				return null;
			}
		}

		private class CustomAM : AbstractService, TestClientToAMTokens.CustomProtocol
		{
			private readonly ApplicationAttemptId appAttemptId;

			private readonly byte[] secretKey;

			private IPEndPoint address;

			private bool pinged = false;

			private ClientToAMTokenSecretManager secretMgr;

			public CustomAM(ApplicationAttemptId appId, byte[] secretKey)
				: base("CustomAM")
			{
				this.appAttemptId = appId;
				this.secretKey = secretKey;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void Ping()
			{
				this.pinged = true;
			}

			public virtual ClientToAMTokenSecretManager GetClientToAMTokenSecretManager()
			{
				return secretMgr;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				Configuration conf = GetConfig();
				Org.Apache.Hadoop.Ipc.Server server;
				try
				{
					secretMgr = new ClientToAMTokenSecretManager(this.appAttemptId, secretKey);
					server = new RPC.Builder(conf).SetProtocol(typeof(TestClientToAMTokens.CustomProtocol
						)).SetNumHandlers(1).SetSecretManager(secretMgr).SetInstance(this).Build();
				}
				catch (Exception e)
				{
					throw new YarnRuntimeException(e);
				}
				server.Start();
				this.address = NetUtils.GetConnectAddress(server);
				base.ServiceStart();
			}

			public virtual void SetClientSecretKey(byte[] key)
			{
				secretMgr.SetMasterKey(key);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClientToAMTokens()
		{
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			ContainerManagementProtocol containerManager = Org.Mockito.Mockito.Mock<ContainerManagementProtocol
				>();
			StartContainersResponse mockResponse = Org.Mockito.Mockito.Mock<StartContainersResponse
				>();
			Org.Mockito.Mockito.When(containerManager.StartContainers((StartContainersRequest
				)Matchers.Any())).ThenReturn(mockResponse);
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm = new _MockRMWithCustomAMLauncher_192(dispatcher, conf, containerManager
				);
			rm.Start();
			// Submit an app
			RMApp app = rm.SubmitApp(1024);
			// Set up a node.
			MockNM nm1 = rm.RegisterNode("localhost:1234", 3072);
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttempt = app.GetCurrentAppAttempt().GetAppAttemptId();
			MockAM mockAM = new MockAM(rm.GetRMContext(), rm.GetApplicationMasterService(), app
				.GetCurrentAppAttempt().GetAppAttemptId());
			UserGroupInformation appUgi = UserGroupInformation.CreateRemoteUser(appAttempt.ToString
				());
			RegisterApplicationMasterResponse response = appUgi.DoAs(new _PrivilegedAction_229
				(mockAM));
			// Get the app-report.
			GetApplicationReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationReportRequest>();
			request.SetApplicationId(app.GetApplicationId());
			GetApplicationReportResponse reportResponse = rm.GetClientRMService().GetApplicationReport
				(request);
			ApplicationReport appReport = reportResponse.GetApplicationReport();
			Org.Apache.Hadoop.Yarn.Api.Records.Token originalClientToAMToken = appReport.GetClientToAMToken
				();
			// ClientToAMToken master key should have been received on register
			// application master response.
			NUnit.Framework.Assert.IsNotNull(response.GetClientToAMTokenMasterKey());
			NUnit.Framework.Assert.IsTrue(((byte[])response.GetClientToAMTokenMasterKey().Array
				()).Length > 0);
			// Start the AM with the correct shared-secret.
			ApplicationAttemptId appAttemptId = app.GetAppAttempts().Keys.GetEnumerator().Next
				();
			NUnit.Framework.Assert.IsNotNull(appAttemptId);
			TestClientToAMTokens.CustomAM am = new TestClientToAMTokens.CustomAM(appAttemptId
				, ((byte[])response.GetClientToAMTokenMasterKey().Array()));
			am.Init(conf);
			am.Start();
			// Now the real test!
			// Set up clients to be able to pick up correct tokens.
			SecurityUtil.SetSecurityInfoProviders(new TestClientToAMTokens.CustomSecurityInfo
				());
			// Verify denial for unauthenticated user
			try
			{
				TestClientToAMTokens.CustomProtocol client = (TestClientToAMTokens.CustomProtocol
					)RPC.GetProxy<TestClientToAMTokens.CustomProtocol>(1L, am.address, conf);
				client.Ping();
				NUnit.Framework.Assert.Fail("Access by unauthenticated user should fail!!");
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsFalse(am.pinged);
			}
			Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token = ConverterUtils
				.ConvertFromYarn(originalClientToAMToken, am.address);
			// Verify denial for a malicious user with tampered ID
			VerifyTokenWithTamperedID(conf, am, token);
			// Verify denial for a malicious user with tampered user-name
			VerifyTokenWithTamperedUserName(conf, am, token);
			// Now for an authenticated user
			VerifyValidToken(conf, am, token);
			// Verify for a new version token
			VerifyNewVersionToken(conf, am, token, rm);
			am.Stop();
			rm.Stop();
		}

		private sealed class _MockRMWithCustomAMLauncher_192 : MockRMWithCustomAMLauncher
		{
			public _MockRMWithCustomAMLauncher_192(DrainDispatcher dispatcher, Configuration 
				baseArg1, ContainerManagementProtocol baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.rmContext, this.scheduler, this.rmAppManager, this
					.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}

			private readonly DrainDispatcher dispatcher;
		}

		private sealed class _PrivilegedAction_229 : PrivilegedAction<RegisterApplicationMasterResponse
			>
		{
			public _PrivilegedAction_229(MockAM mockAM)
			{
				this.mockAM = mockAM;
			}

			public RegisterApplicationMasterResponse Run()
			{
				RegisterApplicationMasterResponse response = null;
				try
				{
					response = mockAM.RegisterAppAttempt();
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Exception was not expected");
				}
				return response;
			}

			private readonly MockAM mockAM;
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyTokenWithTamperedID(Configuration conf, TestClientToAMTokens.CustomAM
			 am, Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token)
		{
			// Malicious user, messes with appId
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("me");
			ClientToAMTokenIdentifier maliciousID = new ClientToAMTokenIdentifier(BuilderUtils
				.NewApplicationAttemptId(BuilderUtils.NewApplicationId(am.appAttemptId.GetApplicationId
				().GetClusterTimestamp(), 42), 43), UserGroupInformation.GetCurrentUser().GetShortUserName
				());
			VerifyTamperedToken(conf, am, token, ugi, maliciousID);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyTokenWithTamperedUserName(Configuration conf, TestClientToAMTokens.CustomAM
			 am, Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token)
		{
			// Malicious user, messes with appId
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("me");
			ClientToAMTokenIdentifier maliciousID = new ClientToAMTokenIdentifier(am.appAttemptId
				, "evilOrc");
			VerifyTamperedToken(conf, am, token, ugi, maliciousID);
		}

		private void VerifyTamperedToken(Configuration conf, TestClientToAMTokens.CustomAM
			 am, Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token, UserGroupInformation
			 ugi, ClientToAMTokenIdentifier maliciousID)
		{
			Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> maliciousToken = 
				new Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier>(maliciousID
				.GetBytes(), token.GetPassword(), token.GetKind(), token.GetService());
			ugi.AddToken(maliciousToken);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_338(am, conf));
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.AreEqual(typeof(RemoteException).FullName, e.GetType().FullName
					);
				e = ((RemoteException)e).UnwrapRemoteException();
				NUnit.Framework.Assert.AreEqual(typeof(SaslException).GetCanonicalName(), e.GetType
					().GetCanonicalName());
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("DIGEST-MD5: digest response format violation. "
					 + "Mismatched response."));
				NUnit.Framework.Assert.IsFalse(am.pinged);
			}
		}

		private sealed class _PrivilegedExceptionAction_338 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_338(TestClientToAMTokens.CustomAM am, Configuration
				 conf)
			{
				this.am = am;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					TestClientToAMTokens.CustomProtocol client = (TestClientToAMTokens.CustomProtocol
						)RPC.GetProxy<TestClientToAMTokens.CustomProtocol>(1L, am.address, conf);
					client.Ping();
					NUnit.Framework.Assert.Fail("Connection initiation with illegally modified " + "tokens is expected to fail."
						);
					return null;
				}
				catch (YarnException ex)
				{
					NUnit.Framework.Assert.Fail("Cannot get a YARN remote exception as " + "it will indicate RPC success"
						);
					throw;
				}
			}

			private readonly TestClientToAMTokens.CustomAM am;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyNewVersionToken(Configuration conf, TestClientToAMTokens.CustomAM
			 am, Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token, MockRM
			 rm)
		{
			UserGroupInformation ugi;
			ugi = UserGroupInformation.CreateRemoteUser("me");
			Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> newToken = new 
				Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier>(new ClientToAMTokenIdentifierForTest
				(token.DecodeIdentifier(), "message"), am.GetClientToAMTokenSecretManager());
			newToken.SetService(token.GetService());
			ugi.AddToken(newToken);
			ugi.DoAs(new _PrivilegedExceptionAction_386(am, conf));
		}

		private sealed class _PrivilegedExceptionAction_386 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_386(TestClientToAMTokens.CustomAM am, Configuration
				 conf)
			{
				this.am = am;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				TestClientToAMTokens.CustomProtocol client = (TestClientToAMTokens.CustomProtocol
					)RPC.GetProxy<TestClientToAMTokens.CustomProtocol>(1L, am.address, conf);
				client.Ping();
				NUnit.Framework.Assert.IsTrue(am.pinged);
				return null;
			}

			private readonly TestClientToAMTokens.CustomAM am;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyValidToken(Configuration conf, TestClientToAMTokens.CustomAM am
			, Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token)
		{
			UserGroupInformation ugi;
			ugi = UserGroupInformation.CreateRemoteUser("me");
			ugi.AddToken(token);
			ugi.DoAs(new _PrivilegedExceptionAction_406(am, conf));
		}

		private sealed class _PrivilegedExceptionAction_406 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_406(TestClientToAMTokens.CustomAM am, Configuration
				 conf)
			{
				this.am = am;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				TestClientToAMTokens.CustomProtocol client = (TestClientToAMTokens.CustomProtocol
					)RPC.GetProxy<TestClientToAMTokens.CustomProtocol>(1L, am.address, conf);
				client.Ping();
				NUnit.Framework.Assert.IsTrue(am.pinged);
				return null;
			}

			private readonly TestClientToAMTokens.CustomAM am;

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientTokenRace()
		{
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			ContainerManagementProtocol containerManager = Org.Mockito.Mockito.Mock<ContainerManagementProtocol
				>();
			StartContainersResponse mockResponse = Org.Mockito.Mockito.Mock<StartContainersResponse
				>();
			Org.Mockito.Mockito.When(containerManager.StartContainers((StartContainersRequest
				)Matchers.Any())).ThenReturn(mockResponse);
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm = new _MockRMWithCustomAMLauncher_433(dispatcher, conf, containerManager
				);
			rm.Start();
			// Submit an app
			RMApp app = rm.SubmitApp(1024);
			// Set up a node.
			MockNM nm1 = rm.RegisterNode("localhost:1234", 3072);
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			ApplicationAttemptId appAttempt = app.GetCurrentAppAttempt().GetAppAttemptId();
			MockAM mockAM = new MockAM(rm.GetRMContext(), rm.GetApplicationMasterService(), app
				.GetCurrentAppAttempt().GetAppAttemptId());
			UserGroupInformation appUgi = UserGroupInformation.CreateRemoteUser(appAttempt.ToString
				());
			RegisterApplicationMasterResponse response = appUgi.DoAs(new _PrivilegedAction_469
				(mockAM));
			// Get the app-report.
			GetApplicationReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationReportRequest>();
			request.SetApplicationId(app.GetApplicationId());
			GetApplicationReportResponse reportResponse = rm.GetClientRMService().GetApplicationReport
				(request);
			ApplicationReport appReport = reportResponse.GetApplicationReport();
			Org.Apache.Hadoop.Yarn.Api.Records.Token originalClientToAMToken = appReport.GetClientToAMToken
				();
			// ClientToAMToken master key should have been received on register
			// application master response.
			ByteBuffer clientMasterKey = response.GetClientToAMTokenMasterKey();
			NUnit.Framework.Assert.IsNotNull(clientMasterKey);
			NUnit.Framework.Assert.IsTrue(((byte[])clientMasterKey.Array()).Length > 0);
			// Start the AM with the correct shared-secret.
			ApplicationAttemptId appAttemptId = app.GetAppAttempts().Keys.GetEnumerator().Next
				();
			NUnit.Framework.Assert.IsNotNull(appAttemptId);
			TestClientToAMTokens.CustomAM am = new TestClientToAMTokens.CustomAM(appAttemptId
				, null);
			am.Init(conf);
			am.Start();
			// Now the real test!
			// Set up clients to be able to pick up correct tokens.
			SecurityUtil.SetSecurityInfoProviders(new TestClientToAMTokens.CustomSecurityInfo
				());
			Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token = ConverterUtils
				.ConvertFromYarn(originalClientToAMToken, am.address);
			// Schedule the key to be set after a significant delay
			Timer timer = new Timer();
			TimerTask timerTask = new _TimerTask_516(am, clientMasterKey);
			timer.Schedule(timerTask, 250);
			// connect should pause waiting for the master key to arrive
			VerifyValidToken(conf, am, token);
			am.Stop();
			rm.Stop();
		}

		private sealed class _MockRMWithCustomAMLauncher_433 : MockRMWithCustomAMLauncher
		{
			public _MockRMWithCustomAMLauncher_433(DrainDispatcher dispatcher, Configuration 
				baseArg1, ContainerManagementProtocol baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.rmContext, this.scheduler, this.rmAppManager, this
					.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}

			private readonly DrainDispatcher dispatcher;
		}

		private sealed class _PrivilegedAction_469 : PrivilegedAction<RegisterApplicationMasterResponse
			>
		{
			public _PrivilegedAction_469(MockAM mockAM)
			{
				this.mockAM = mockAM;
			}

			public RegisterApplicationMasterResponse Run()
			{
				RegisterApplicationMasterResponse response = null;
				try
				{
					response = mockAM.RegisterAppAttempt();
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Exception was not expected");
				}
				return response;
			}

			private readonly MockAM mockAM;
		}

		private sealed class _TimerTask_516 : TimerTask
		{
			public _TimerTask_516(TestClientToAMTokens.CustomAM am, ByteBuffer clientMasterKey
				)
			{
				this.am = am;
				this.clientMasterKey = clientMasterKey;
			}

			public override void Run()
			{
				am.SetClientSecretKey(((byte[])clientMasterKey.Array()));
			}

			private readonly TestClientToAMTokens.CustomAM am;

			private readonly ByteBuffer clientMasterKey;
		}
	}
}
