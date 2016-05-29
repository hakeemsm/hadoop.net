using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class TestAMRMTokens
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.TestAMRMTokens
			));

		private readonly Configuration conf;

		private const int maxWaitAttempts = 50;

		private const int rolling_interval_sec = 13;

		private const long am_expire_ms = 4000;

		[Parameterized.Parameters]
		public static ICollection<object[]> Configs()
		{
			Configuration conf = new Configuration();
			Configuration confWithSecurity = new Configuration();
			confWithSecurity.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, 
				"kerberos");
			return Arrays.AsList(new object[][] { new object[] { conf }, new object[] { confWithSecurity
				 } });
		}

		public TestAMRMTokens(Configuration conf)
		{
			this.conf = conf;
			UserGroupInformation.SetConfiguration(conf);
		}

		/// <summary>
		/// Validate that application tokens are unusable after the
		/// application-finishes.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenExpiry()
		{
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			TestAMAuthorization.MockRMWithAMS rm = new TestAMAuthorization.MockRMWithAMS(conf
				, containerManager);
			rm.Start();
			Configuration conf = rm.GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			ApplicationMasterProtocol rmClient = null;
			try
			{
				MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
				RMApp app = rm.SubmitApp(1024);
				nm1.NodeHeartbeat(true);
				int waitCount = 0;
				while (containerManager.containerTokens == null && waitCount++ < 20)
				{
					Log.Info("Waiting for AM Launch to happen..");
					Sharpen.Thread.Sleep(1000);
				}
				NUnit.Framework.Assert.IsNotNull(containerManager.containerTokens);
				RMAppAttempt attempt = app.GetCurrentAppAttempt();
				ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
				// Create a client to the RM.
				UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
					.ToString());
				Credentials credentials = containerManager.GetContainerCredentials();
				IPEndPoint rmBindAddress = rm.GetApplicationMasterService().GetBindAddress();
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> amRMToken = TestAMAuthorization.MockRMWithAMS
					.SetupAndReturnAMRMToken(rmBindAddress, credentials.GetAllTokens());
				currentUser.AddToken(amRMToken);
				rmClient = CreateRMClient(rm, conf, rpc, currentUser);
				RegisterApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<RegisterApplicationMasterRequest>();
				rmClient.RegisterApplicationMaster(request);
				FinishApplicationMasterRequest finishAMRequest = Org.Apache.Hadoop.Yarn.Util.Records
					.NewRecord<FinishApplicationMasterRequest>();
				finishAMRequest.SetFinalApplicationStatus(FinalApplicationStatus.Succeeded);
				finishAMRequest.SetDiagnostics("diagnostics");
				finishAMRequest.SetTrackingUrl("url");
				rmClient.FinishApplicationMaster(finishAMRequest);
				// Send RMAppAttemptEventType.CONTAINER_FINISHED to transit RMAppAttempt
				// from Finishing state to Finished State. Both AMRMToken and
				// ClientToAMToken will be removed.
				ContainerStatus containerStatus = BuilderUtils.NewContainerStatus(attempt.GetMasterContainer
					().GetId(), ContainerState.Complete, "AM Container Finished", 0);
				rm.GetRMContext().GetDispatcher().GetEventHandler().Handle(new RMAppAttemptContainerFinishedEvent
					(applicationAttemptId, containerStatus, nm1.GetNodeId()));
				// Make sure the RMAppAttempt is at Finished State.
				// Both AMRMToken and ClientToAMToken have been removed.
				int count = 0;
				while (attempt.GetState() != RMAppAttemptState.Finished && count < maxWaitAttempts
					)
				{
					Sharpen.Thread.Sleep(100);
					count++;
				}
				NUnit.Framework.Assert.IsTrue(attempt.GetState() == RMAppAttemptState.Finished);
				// Now simulate trying to allocate. RPC call itself should throw auth
				// exception.
				rpc.StopProxy(rmClient, conf);
				// To avoid using cached client
				rmClient = CreateRMClient(rm, conf, rpc, currentUser);
				AllocateRequest allocateRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
					>();
				try
				{
					rmClient.Allocate(allocateRequest);
					NUnit.Framework.Assert.Fail("You got to be kidding me! " + "Using App tokens after app-finish should fail!"
						);
				}
				catch (Exception t)
				{
					Log.Info("Exception found is ", t);
					// The exception will still have the earlier appAttemptId as it picks it
					// up from the token.
					NUnit.Framework.Assert.IsTrue(t.InnerException.Message.Contains(applicationAttemptId
						.ToString() + " not found in AMRMTokenSecretManager."));
				}
			}
			finally
			{
				rm.Stop();
				if (rmClient != null)
				{
					rpc.StopProxy(rmClient, conf);
				}
			}
		}

		// To avoid using cached client
		/// <summary>
		/// Validate master-key-roll-over and that tokens are usable even after
		/// master-key-roll-over.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMasterKeyRollOver()
		{
			conf.SetLong(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs, rolling_interval_sec
				);
			conf.SetLong(YarnConfiguration.RmAmExpiryIntervalMs, am_expire_ms);
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			TestAMAuthorization.MockRMWithAMS rm = new TestAMAuthorization.MockRMWithAMS(conf
				, containerManager);
			rm.Start();
			long startTime = Runtime.CurrentTimeMillis();
			Configuration conf = rm.GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			ApplicationMasterProtocol rmClient = null;
			AMRMTokenSecretManager appTokenSecretManager = rm.GetRMContext().GetAMRMTokenSecretManager
				();
			MasterKeyData oldKey = appTokenSecretManager.GetMasterKey();
			NUnit.Framework.Assert.IsNotNull(oldKey);
			try
			{
				MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
				RMApp app = rm.SubmitApp(1024);
				nm1.NodeHeartbeat(true);
				int waitCount = 0;
				while (containerManager.containerTokens == null && waitCount++ < maxWaitAttempts)
				{
					Log.Info("Waiting for AM Launch to happen..");
					Sharpen.Thread.Sleep(1000);
				}
				NUnit.Framework.Assert.IsNotNull(containerManager.containerTokens);
				RMAppAttempt attempt = app.GetCurrentAppAttempt();
				ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
				// Create a client to the RM.
				UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
					.ToString());
				Credentials credentials = containerManager.GetContainerCredentials();
				IPEndPoint rmBindAddress = rm.GetApplicationMasterService().GetBindAddress();
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> amRMToken = TestAMAuthorization.MockRMWithAMS
					.SetupAndReturnAMRMToken(rmBindAddress, credentials.GetAllTokens());
				currentUser.AddToken(amRMToken);
				rmClient = CreateRMClient(rm, conf, rpc, currentUser);
				RegisterApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<RegisterApplicationMasterRequest>();
				rmClient.RegisterApplicationMaster(request);
				// One allocate call.
				AllocateRequest allocateRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
					>();
				NUnit.Framework.Assert.IsTrue(rmClient.Allocate(allocateRequest).GetAMCommand() ==
					 null);
				// Wait for enough time and make sure the roll_over happens
				// At mean time, the old AMRMToken should continue to work
				while (Runtime.CurrentTimeMillis() - startTime < rolling_interval_sec * 1000)
				{
					rmClient.Allocate(allocateRequest);
					Sharpen.Thread.Sleep(500);
				}
				MasterKeyData newKey = appTokenSecretManager.GetMasterKey();
				NUnit.Framework.Assert.IsNotNull(newKey);
				NUnit.Framework.Assert.IsFalse("Master key should have changed!", oldKey.Equals(newKey
					));
				// Another allocate call with old AMRMToken. Should continue to work.
				rpc.StopProxy(rmClient, conf);
				// To avoid using cached client
				rmClient = CreateRMClient(rm, conf, rpc, currentUser);
				NUnit.Framework.Assert.IsTrue(rmClient.Allocate(allocateRequest).GetAMCommand() ==
					 null);
				waitCount = 0;
				while (waitCount++ <= maxWaitAttempts)
				{
					if (appTokenSecretManager.GetCurrnetMasterKeyData() != oldKey)
					{
						break;
					}
					try
					{
						rmClient.Allocate(allocateRequest);
					}
					catch (Exception)
					{
						break;
					}
					Sharpen.Thread.Sleep(200);
				}
				// active the nextMasterKey, and replace the currentMasterKey
				NUnit.Framework.Assert.IsTrue(appTokenSecretManager.GetCurrnetMasterKeyData().Equals
					(newKey));
				NUnit.Framework.Assert.IsTrue(appTokenSecretManager.GetMasterKey().Equals(newKey)
					);
				NUnit.Framework.Assert.IsTrue(appTokenSecretManager.GetNextMasterKeyData() == null
					);
				// Create a new Token
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> newToken = appTokenSecretManager
					.CreateAndGetAMRMToken(applicationAttemptId);
				SecurityUtil.SetTokenService(newToken, rmBindAddress);
				currentUser.AddToken(newToken);
				// Another allocate call. Should continue to work.
				rpc.StopProxy(rmClient, conf);
				// To avoid using cached client
				rmClient = CreateRMClient(rm, conf, rpc, currentUser);
				allocateRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest>(
					);
				NUnit.Framework.Assert.IsTrue(rmClient.Allocate(allocateRequest).GetAMCommand() ==
					 null);
				// Should not work by using the old AMRMToken.
				rpc.StopProxy(rmClient, conf);
				// To avoid using cached client
				try
				{
					currentUser.AddToken(amRMToken);
					rmClient = CreateRMClient(rm, conf, rpc, currentUser);
					allocateRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest>(
						);
					NUnit.Framework.Assert.IsTrue(rmClient.Allocate(allocateRequest).GetAMCommand() ==
						 null);
					NUnit.Framework.Assert.Fail("The old Token should not work");
				}
				catch (Exception)
				{
				}
			}
			finally
			{
				// expect exception
				rm.Stop();
				if (rmClient != null)
				{
					rpc.StopProxy(rmClient, conf);
				}
			}
		}

		// To avoid using cached client
		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMMasterKeysUpdate()
		{
			AtomicReference<AMRMTokenSecretManager> spySecretMgrRef = new AtomicReference<AMRMTokenSecretManager
				>();
			MockRM rm = new _MockRM_349(this, spySecretMgrRef, conf);
			// Skip the login.
			rm.Start();
			MockNM nm = rm.RegisterNode("127.0.0.1:1234", 8000);
			RMApp app = rm.SubmitApp(200);
			MockAM am = MockRM.LaunchAndRegisterAM(app, rm, nm);
			AMRMTokenSecretManager spySecretMgr = spySecretMgrRef.Get();
			// Do allocate. Should not update AMRMToken
			AllocateResponse response = am.Allocate(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<AllocateRequest>());
			NUnit.Framework.Assert.IsNull(response.GetAMRMToken());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> oldToken = rm.GetRMContext
				().GetRMApps()[app.GetApplicationId()].GetRMAppAttempt(am.GetApplicationAttemptId
				()).GetAMRMToken();
			// roll over the master key
			// Do allocate again. the AM should get the latest AMRMToken
			rm.GetRMContext().GetAMRMTokenSecretManager().RollMasterKey();
			response = am.Allocate(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
				>());
			NUnit.Framework.Assert.IsNotNull(response.GetAMRMToken());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = ConverterUtils
				.ConvertFromYarn(response.GetAMRMToken(), new Text(response.GetAMRMToken().GetService
				()));
			NUnit.Framework.Assert.AreEqual(amrmToken.DecodeIdentifier().GetKeyId(), rm.GetRMContext
				().GetAMRMTokenSecretManager().GetMasterKey().GetMasterKey().GetKeyId());
			// Do allocate again with the same old token and verify the RM sends
			// back the last generated token instead of generating it again.
			Org.Mockito.Mockito.Reset(spySecretMgr);
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(am.GetApplicationAttemptId
				().ToString(), new string[0]);
			ugi.AddTokenIdentifier(oldToken.DecodeIdentifier());
			response = am.DoAllocateAs(ugi, Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
				>());
			NUnit.Framework.Assert.IsNotNull(response.GetAMRMToken());
			Org.Mockito.Mockito.Verify(spySecretMgr, Org.Mockito.Mockito.Never()).CreateAndGetAMRMToken
				(Matchers.IsA<ApplicationAttemptId>());
			// Do allocate again with the updated token and verify we do not
			// receive a new token to use.
			response = am.Allocate(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
				>());
			NUnit.Framework.Assert.IsNull(response.GetAMRMToken());
			// Activate the next master key. Since there is new master key generated
			// in AMRMTokenSecretManager. The AMRMToken will not get updated for AM
			rm.GetRMContext().GetAMRMTokenSecretManager().ActivateNextMasterKey();
			response = am.Allocate(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
				>());
			NUnit.Framework.Assert.IsNull(response.GetAMRMToken());
			rm.Stop();
		}

		private sealed class _MockRM_349 : MockRM
		{
			public _MockRM_349(TestAMRMTokens _enclosing, AtomicReference<AMRMTokenSecretManager
				> spySecretMgrRef, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.spySecretMgrRef = spySecretMgrRef;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}

			protected internal override RMSecretManagerService CreateRMSecretManagerService()
			{
				return new _RMSecretManagerService_357(spySecretMgrRef, this._enclosing.conf, this
					.rmContext);
			}

			private sealed class _RMSecretManagerService_357 : RMSecretManagerService
			{
				public _RMSecretManagerService_357(AtomicReference<AMRMTokenSecretManager> spySecretMgrRef
					, Configuration baseArg1, RMContextImpl baseArg2)
					: base(baseArg1, baseArg2)
				{
					this.spySecretMgrRef = spySecretMgrRef;
				}

				protected internal override AMRMTokenSecretManager CreateAMRMTokenSecretManager(Configuration
					 conf, RMContext rmContext)
				{
					AMRMTokenSecretManager spySecretMgr = Org.Mockito.Mockito.Spy(base.CreateAMRMTokenSecretManager
						(conf, rmContext));
					spySecretMgrRef.Set(spySecretMgr);
					return spySecretMgr;
				}

				private readonly AtomicReference<AMRMTokenSecretManager> spySecretMgrRef;
			}

			private readonly TestAMRMTokens _enclosing;

			private readonly AtomicReference<AMRMTokenSecretManager> spySecretMgrRef;
		}

		private ApplicationMasterProtocol CreateRMClient(MockRM rm, Configuration conf, YarnRPC
			 rpc, UserGroupInformation currentUser)
		{
			return currentUser.DoAs(new _PrivilegedAction_422(rpc, rm, conf));
		}

		private sealed class _PrivilegedAction_422 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_422(YarnRPC rpc, MockRM rm, Configuration conf)
			{
				this.rpc = rpc;
				this.rm = rm;
				this.conf = conf;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)rpc.GetProxy(typeof(ApplicationMasterProtocol), 
					rm.GetApplicationMasterService().GetBindAddress(), conf);
			}

			private readonly YarnRPC rpc;

			private readonly MockRM rm;

			private readonly Configuration conf;
		}
	}
}
