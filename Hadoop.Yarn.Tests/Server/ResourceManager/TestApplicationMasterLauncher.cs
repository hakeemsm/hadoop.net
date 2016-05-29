using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestApplicationMasterLauncher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestApplicationMasterLauncher
			));

		private sealed class MyContainerManagerImpl : ContainerManagementProtocol
		{
			internal bool launched = false;

			internal bool cleanedup = false;

			internal string attemptIdAtContainerManager = null;

			internal string containerIdAtContainerManager = null;

			internal string nmHostAtContainerManager = null;

			internal long submitTimeAtContainerManager;

			internal int maxAppAttempts;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public StartContainersResponse StartContainers(StartContainersRequest requests)
			{
				StartContainerRequest request = requests.GetStartContainerRequests()[0];
				Log.Info("Container started by MyContainerManager: " + request);
				launched = true;
				IDictionary<string, string> env = request.GetContainerLaunchContext().GetEnvironment
					();
				Token containerToken = request.GetContainerToken();
				ContainerTokenIdentifier tokenId = null;
				try
				{
					tokenId = BuilderUtils.NewContainerTokenIdentifier(containerToken);
				}
				catch (IOException e)
				{
					throw RPCUtil.GetRemoteException(e);
				}
				ContainerId containerId = tokenId.GetContainerID();
				containerIdAtContainerManager = containerId.ToString();
				attemptIdAtContainerManager = containerId.GetApplicationAttemptId().ToString();
				nmHostAtContainerManager = tokenId.GetNmHostAddress();
				submitTimeAtContainerManager = long.Parse(env[ApplicationConstants.AppSubmitTimeEnv
					]);
				maxAppAttempts = System.Convert.ToInt32(env[ApplicationConstants.MaxAppAttemptsEnv
					]);
				return StartContainersResponse.NewInstance(new Dictionary<string, ByteBuffer>(), 
					new AList<ContainerId>(), new Dictionary<ContainerId, SerializedException>());
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public StopContainersResponse StopContainers(StopContainersRequest request)
			{
				Log.Info("Container cleaned up by MyContainerManager");
				cleanedup = true;
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMLaunchAndCleanup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			TestApplicationMasterLauncher.MyContainerManagerImpl containerManager = new TestApplicationMasterLauncher.MyContainerManagerImpl
				();
			MockRMWithCustomAMLauncher rm = new MockRMWithCustomAMLauncher(containerManager);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 5120);
			RMApp app = rm.SubmitApp(2000);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			int waitCount = 0;
			while (containerManager.launched == false && waitCount++ < 20)
			{
				Log.Info("Waiting for AM Launch to happen..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsTrue(containerManager.launched);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			ApplicationAttemptId appAttemptId = attempt.GetAppAttemptId();
			NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), containerManager.attemptIdAtContainerManager
				);
			NUnit.Framework.Assert.AreEqual(app.GetSubmitTime(), containerManager.submitTimeAtContainerManager
				);
			NUnit.Framework.Assert.AreEqual(app.GetRMAppAttempt(appAttemptId).GetMasterContainer
				().GetId().ToString(), containerManager.containerIdAtContainerManager);
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId().ToString(), containerManager.nmHostAtContainerManager
				);
			NUnit.Framework.Assert.AreEqual(YarnConfiguration.DefaultRmAmMaxAttempts, containerManager
				.maxAppAttempts);
			MockAM am = new MockAM(rm.GetRMContext(), rm.GetApplicationMasterService(), appAttemptId
				);
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			//complete the AM container to finish the app normally
			nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			waitCount = 0;
			while (containerManager.cleanedup == false && waitCount++ < 20)
			{
				Log.Info("Waiting for AM Cleanup to happen..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsTrue(containerManager.cleanedup);
			am.WaitForState(RMAppAttemptState.Finished);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetriesOnFailures()
		{
			ContainerManagementProtocol mockProxy = Org.Mockito.Mockito.Mock<ContainerManagementProtocol
				>();
			StartContainersResponse mockResponse = Org.Mockito.Mockito.Mock<StartContainersResponse
				>();
			Org.Mockito.Mockito.When(mockProxy.StartContainers(Matchers.Any<StartContainersRequest
				>())).ThenThrow(new NMNotYetReadyException("foo")).ThenReturn(mockResponse);
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			conf.SetInt(YarnConfiguration.ClientNmConnectRetryIntervalMs, 1);
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm = new _MockRMWithCustomAMLauncher_206(dispatcher, mockProxy, conf, null
				);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 5120);
			RMApp app = rm.SubmitApp(2000);
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			rm.WaitForState(appAttemptId, RMAppAttemptState.Launched, 500);
		}

		private sealed class _MockRMWithCustomAMLauncher_206 : MockRMWithCustomAMLauncher
		{
			public _MockRMWithCustomAMLauncher_206(DrainDispatcher dispatcher, ContainerManagementProtocol
				 mockProxy, Configuration baseArg1, ContainerManagementProtocol baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dispatcher = dispatcher;
				this.mockProxy = mockProxy;
			}

			protected internal override ApplicationMasterLauncher CreateAMLauncher()
			{
				return new _ApplicationMasterLauncher_209(mockProxy, this.GetRMContext());
			}

			private sealed class _ApplicationMasterLauncher_209 : ApplicationMasterLauncher
			{
				public _ApplicationMasterLauncher_209(ContainerManagementProtocol mockProxy, RMContext
					 baseArg1)
					: base(baseArg1)
				{
					this.mockProxy = mockProxy;
				}

				protected internal override Runnable CreateRunnableLauncher(RMAppAttempt application
					, AMLauncherEventType @event)
				{
					return new _AMLauncher_213(mockProxy, this.context, application, @event, this.GetConfig
						());
				}

				private sealed class _AMLauncher_213 : AMLauncher
				{
					public _AMLauncher_213(ContainerManagementProtocol mockProxy, RMContext baseArg1, 
						RMAppAttempt baseArg2, AMLauncherEventType baseArg3, Configuration baseArg4)
						: base(baseArg1, baseArg2, baseArg3, baseArg4)
					{
						this.mockProxy = mockProxy;
					}

					protected internal override YarnRPC GetYarnRPC()
					{
						YarnRPC mockRpc = Org.Mockito.Mockito.Mock<YarnRPC>();
						Org.Mockito.Mockito.When(mockRpc.GetProxy(Matchers.Any<Type>(), Matchers.Any<IPEndPoint
							>(), Matchers.Any<Configuration>())).ThenReturn(mockProxy);
						return mockRpc;
					}

					private readonly ContainerManagementProtocol mockProxy;
				}

				private readonly ContainerManagementProtocol mockProxy;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly DrainDispatcher dispatcher;

			private readonly ContainerManagementProtocol mockProxy;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestallocateBeforeAMRegistration()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			bool thrown = false;
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM();
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5000);
			RMApp app = rm.SubmitApp(2000);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			// request for containers
			int request = 2;
			AllocateResponse ar = null;
			try
			{
				ar = am.Allocate("h1", 1000, request, new AList<ContainerId>());
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationMasterNotRegisteredException)
			{
			}
			// kick the scheduler
			nm1.NodeHeartbeat(true);
			AllocateResponse amrs = null;
			try
			{
				amrs = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>());
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationMasterNotRegisteredException)
			{
			}
			am.RegisterAppAttempt();
			try
			{
				am.RegisterAppAttempt(false);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.AreEqual("Application Master is already registered : " + attempt
					.GetAppAttemptId().GetApplicationId(), e.Message);
			}
			// Simulate an AM that was disconnected and app attempt was removed
			// (responseMap does not contain attemptid)
			am.UnregisterAppAttempt();
			nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			try
			{
				amrs = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>());
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationAttemptNotFoundException)
			{
			}
		}
	}
}
