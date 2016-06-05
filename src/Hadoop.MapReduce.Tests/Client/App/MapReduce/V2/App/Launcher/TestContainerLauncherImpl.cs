using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	public class TestContainerLauncherImpl
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestContainerLauncherImpl
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private IDictionary<string, ByteBuffer> serviceResponse = new Dictionary<string, 
			ByteBuffer>();

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			serviceResponse.Clear();
			serviceResponse[ShuffleHandler.MapreduceShuffleServiceid] = ShuffleHandler.SerializeMetaData
				(80);
		}

		private interface ContainerManagementProtocolClient : ContainerManagementProtocol
			, IDisposable
		{
			// tests here mock ContainerManagementProtocol which does not have close
			// method. creating an interface that implements ContainerManagementProtocol
			// and Closeable so the tests does not fail with NoSuchMethodException
		}

		private class ContainerLauncherImplUnderTest : ContainerLauncherImpl
		{
			private ContainerManagementProtocol containerManager;

			public ContainerLauncherImplUnderTest(AppContext context, ContainerManagementProtocol
				 containerManager)
				: base(context)
			{
				this.containerManager = containerManager;
			}

			/// <exception cref="System.IO.IOException"/>
			public override ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
				 GetCMProxy(string containerMgrBindAddr, ContainerId containerId)
			{
				ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData protocolProxy
					 = Org.Mockito.Mockito.Mock<ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
					>();
				Org.Mockito.Mockito.When(protocolProxy.GetContainerManagementProtocol()).ThenReturn
					(containerManager);
				return protocolProxy;
			}

			/// <exception cref="System.Exception"/>
			public virtual void WaitForPoolToIdle()
			{
				//I wish that we did not need the sleep, but it is here so that we are sure
				// That the other thread had time to insert the event into the queue and
				// start processing it.  For some reason we were getting interrupted
				// exceptions within eventQueue without this sleep.
				Sharpen.Thread.Sleep(100l);
				Log.Debug("POOL SIZE 1: " + this.eventQueue.Count + " POOL SIZE 2: " + this.launcherPool
					.GetQueue().Count + " ACTIVE COUNT: " + this.launcherPool.GetActiveCount());
				while (!this.eventQueue.IsEmpty() || !this.launcherPool.GetQueue().IsEmpty() || this
					.launcherPool.GetActiveCount() > 0)
				{
					Sharpen.Thread.Sleep(100l);
					Log.Debug("POOL SIZE 1: " + this.eventQueue.Count + " POOL SIZE 2: " + this.launcherPool
						.GetQueue().Count + " ACTIVE COUNT: " + this.launcherPool.GetActiveCount());
				}
				Log.Debug("POOL SIZE 1: " + this.eventQueue.Count + " POOL SIZE 2: " + this.launcherPool
					.GetQueue().Count + " ACTIVE COUNT: " + this.launcherPool.GetActiveCount());
			}
		}

		public static ContainerId MakeContainerId(long ts, int appId, int attemptId, int 
			id)
		{
			return ContainerId.NewContainerId(ApplicationAttemptId.NewInstance(ApplicationId.
				NewInstance(ts, appId), attemptId), id);
		}

		public static TaskAttemptId MakeTaskAttemptId(long ts, int appId, int taskId, TaskType
			 taskType, int id)
		{
			ApplicationId aID = ApplicationId.NewInstance(ts, appId);
			JobId jID = MRBuilderUtils.NewJobId(aID, id);
			TaskId tID = MRBuilderUtils.NewTaskId(jID, taskId, taskType);
			return MRBuilderUtils.NewTaskAttemptId(tID, id);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHandle()
		{
			Log.Info("STARTING testHandle");
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(mockEventHandler
				);
			string cmAddress = "127.0.0.1:8000";
			TestContainerLauncherImpl.ContainerManagementProtocolClient mockCM = Org.Mockito.Mockito.Mock
				<TestContainerLauncherImpl.ContainerManagementProtocolClient>();
			TestContainerLauncherImpl.ContainerLauncherImplUnderTest ut = new TestContainerLauncherImpl.ContainerLauncherImplUnderTest
				(mockContext, mockCM);
			Configuration conf = new Configuration();
			ut.Init(conf);
			ut.Start();
			try
			{
				ContainerId contId = MakeContainerId(0l, 0, 0, 1);
				TaskAttemptId taskAttemptId = MakeTaskAttemptId(0l, 0, 0, TaskType.Map, 0);
				StartContainersResponse startResp = recordFactory.NewRecordInstance<StartContainersResponse
					>();
				startResp.SetAllServicesMetaData(serviceResponse);
				Log.Info("inserting launch event");
				ContainerRemoteLaunchEvent mockLaunchEvent = Org.Mockito.Mockito.Mock<ContainerRemoteLaunchEvent
					>();
				Org.Mockito.Mockito.When(mockLaunchEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteLaunch);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				Org.Mockito.Mockito.When(mockCM.StartContainers(Matchers.Any<StartContainersRequest
					>())).ThenReturn(startResp);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerToken()).ThenReturn(CreateNewContainerToken
					(contId, cmAddress));
				ut.Handle(mockLaunchEvent);
				ut.WaitForPoolToIdle();
				Org.Mockito.Mockito.Verify(mockCM).StartContainers(Matchers.Any<StartContainersRequest
					>());
				Log.Info("inserting cleanup event");
				ContainerLauncherEvent mockCleanupEvent = Org.Mockito.Mockito.Mock<ContainerLauncherEvent
					>();
				Org.Mockito.Mockito.When(mockCleanupEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteCleanup);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				ut.Handle(mockCleanupEvent);
				ut.WaitForPoolToIdle();
				Org.Mockito.Mockito.Verify(mockCM).StopContainers(Matchers.Any<StopContainersRequest
					>());
			}
			finally
			{
				ut.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOutOfOrder()
		{
			Log.Info("STARTING testOutOfOrder");
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(mockEventHandler
				);
			TestContainerLauncherImpl.ContainerManagementProtocolClient mockCM = Org.Mockito.Mockito.Mock
				<TestContainerLauncherImpl.ContainerManagementProtocolClient>();
			TestContainerLauncherImpl.ContainerLauncherImplUnderTest ut = new TestContainerLauncherImpl.ContainerLauncherImplUnderTest
				(mockContext, mockCM);
			Configuration conf = new Configuration();
			ut.Init(conf);
			ut.Start();
			try
			{
				ContainerId contId = MakeContainerId(0l, 0, 0, 1);
				TaskAttemptId taskAttemptId = MakeTaskAttemptId(0l, 0, 0, TaskType.Map, 0);
				string cmAddress = "127.0.0.1:8000";
				StartContainersResponse startResp = recordFactory.NewRecordInstance<StartContainersResponse
					>();
				startResp.SetAllServicesMetaData(serviceResponse);
				Log.Info("inserting cleanup event");
				ContainerLauncherEvent mockCleanupEvent = Org.Mockito.Mockito.Mock<ContainerLauncherEvent
					>();
				Org.Mockito.Mockito.When(mockCleanupEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteCleanup);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				ut.Handle(mockCleanupEvent);
				ut.WaitForPoolToIdle();
				Org.Mockito.Mockito.Verify(mockCM, Org.Mockito.Mockito.Never()).StopContainers(Matchers.Any
					<StopContainersRequest>());
				Log.Info("inserting launch event");
				ContainerRemoteLaunchEvent mockLaunchEvent = Org.Mockito.Mockito.Mock<ContainerRemoteLaunchEvent
					>();
				Org.Mockito.Mockito.When(mockLaunchEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteLaunch);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				Org.Mockito.Mockito.When(mockCM.StartContainers(Matchers.Any<StartContainersRequest
					>())).ThenReturn(startResp);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerToken()).ThenReturn(CreateNewContainerToken
					(contId, cmAddress));
				ut.Handle(mockLaunchEvent);
				ut.WaitForPoolToIdle();
				Org.Mockito.Mockito.Verify(mockCM, Org.Mockito.Mockito.Never()).StartContainers(Matchers.Any
					<StartContainersRequest>());
			}
			finally
			{
				ut.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMyShutdown()
		{
			Log.Info("in test Shutdown");
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(mockEventHandler
				);
			TestContainerLauncherImpl.ContainerManagementProtocolClient mockCM = Org.Mockito.Mockito.Mock
				<TestContainerLauncherImpl.ContainerManagementProtocolClient>();
			TestContainerLauncherImpl.ContainerLauncherImplUnderTest ut = new TestContainerLauncherImpl.ContainerLauncherImplUnderTest
				(mockContext, mockCM);
			Configuration conf = new Configuration();
			ut.Init(conf);
			ut.Start();
			try
			{
				ContainerId contId = MakeContainerId(0l, 0, 0, 1);
				TaskAttemptId taskAttemptId = MakeTaskAttemptId(0l, 0, 0, TaskType.Map, 0);
				string cmAddress = "127.0.0.1:8000";
				StartContainersResponse startResp = recordFactory.NewRecordInstance<StartContainersResponse
					>();
				startResp.SetAllServicesMetaData(serviceResponse);
				Log.Info("inserting launch event");
				ContainerRemoteLaunchEvent mockLaunchEvent = Org.Mockito.Mockito.Mock<ContainerRemoteLaunchEvent
					>();
				Org.Mockito.Mockito.When(mockLaunchEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteLaunch);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				Org.Mockito.Mockito.When(mockCM.StartContainers(Matchers.Any<StartContainersRequest
					>())).ThenReturn(startResp);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerToken()).ThenReturn(CreateNewContainerToken
					(contId, cmAddress));
				ut.Handle(mockLaunchEvent);
				ut.WaitForPoolToIdle();
				Org.Mockito.Mockito.Verify(mockCM).StartContainers(Matchers.Any<StartContainersRequest
					>());
			}
			finally
			{
				// skip cleanup and make sure stop kills the container
				ut.Stop();
				Org.Mockito.Mockito.Verify(mockCM).StopContainers(Matchers.Any<StopContainersRequest
					>());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerCleaned()
		{
			Log.Info("STARTING testContainerCleaned");
			CyclicBarrier startLaunchBarrier = new CyclicBarrier(2);
			CyclicBarrier completeLaunchBarrier = new CyclicBarrier(2);
			AppContext mockContext = Org.Mockito.Mockito.Mock<AppContext>();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Org.Mockito.Mockito.When(mockContext.GetEventHandler()).ThenReturn(mockEventHandler
				);
			TestContainerLauncherImpl.ContainerManagementProtocolClient mockCM = new TestContainerLauncherImpl.ContainerManagerForTest
				(startLaunchBarrier, completeLaunchBarrier);
			TestContainerLauncherImpl.ContainerLauncherImplUnderTest ut = new TestContainerLauncherImpl.ContainerLauncherImplUnderTest
				(mockContext, mockCM);
			Configuration conf = new Configuration();
			ut.Init(conf);
			ut.Start();
			try
			{
				ContainerId contId = MakeContainerId(0l, 0, 0, 1);
				TaskAttemptId taskAttemptId = MakeTaskAttemptId(0l, 0, 0, TaskType.Map, 0);
				string cmAddress = "127.0.0.1:8000";
				StartContainersResponse startResp = recordFactory.NewRecordInstance<StartContainersResponse
					>();
				startResp.SetAllServicesMetaData(serviceResponse);
				Log.Info("inserting launch event");
				ContainerRemoteLaunchEvent mockLaunchEvent = Org.Mockito.Mockito.Mock<ContainerRemoteLaunchEvent
					>();
				Org.Mockito.Mockito.When(mockLaunchEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteLaunch);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				Org.Mockito.Mockito.When(mockLaunchEvent.GetContainerToken()).ThenReturn(CreateNewContainerToken
					(contId, cmAddress));
				ut.Handle(mockLaunchEvent);
				startLaunchBarrier.Await();
				Log.Info("inserting cleanup event");
				ContainerLauncherEvent mockCleanupEvent = Org.Mockito.Mockito.Mock<ContainerLauncherEvent
					>();
				Org.Mockito.Mockito.When(mockCleanupEvent.GetType()).ThenReturn(ContainerLauncher.EventType
					.ContainerRemoteCleanup);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerID()).ThenReturn(contId);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetTaskAttemptID()).ThenReturn(taskAttemptId
					);
				Org.Mockito.Mockito.When(mockCleanupEvent.GetContainerMgrAddress()).ThenReturn(cmAddress
					);
				ut.Handle(mockCleanupEvent);
				completeLaunchBarrier.Await();
				ut.WaitForPoolToIdle();
				ArgumentCaptor<Org.Apache.Hadoop.Yarn.Event.Event> arg = ArgumentCaptor.ForClass<
					Org.Apache.Hadoop.Yarn.Event.Event>();
				Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(2)).Handle
					(arg.Capture());
				bool containerCleaned = false;
				for (int i = 0; i < arg.GetAllValues().Count; i++)
				{
					Log.Info(arg.GetAllValues()[i].ToString());
					Org.Apache.Hadoop.Yarn.Event.Event currentEvent = arg.GetAllValues()[i];
					if (currentEvent.GetType() == TaskAttemptEventType.TaContainerCleaned)
					{
						containerCleaned = true;
					}
				}
				System.Diagnostics.Debug.Assert((containerCleaned));
			}
			finally
			{
				ut.Stop();
			}
		}

		private Token CreateNewContainerToken(ContainerId contId, string containerManagerAddr
			)
		{
			long currentTime = Runtime.CurrentTimeMillis();
			return MRApp.NewContainerToken(NodeId.NewInstance("127.0.0.1", 1234), Sharpen.Runtime.GetBytesForString
				("password"), new ContainerTokenIdentifier(contId, containerManagerAddr, "user", 
				Resource.NewInstance(1024, 1), currentTime + 10000L, 123, currentTime, Priority.
				NewInstance(0), 0));
		}

		private class ContainerManagerForTest : TestContainerLauncherImpl.ContainerManagementProtocolClient
		{
			private CyclicBarrier startLaunchBarrier;

			private CyclicBarrier completeLaunchBarrier;

			internal ContainerManagerForTest(CyclicBarrier startLaunchBarrier, CyclicBarrier 
				completeLaunchBarrier)
			{
				this.startLaunchBarrier = startLaunchBarrier;
				this.completeLaunchBarrier = completeLaunchBarrier;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual StartContainersResponse StartContainers(StartContainersRequest request
				)
			{
				try
				{
					startLaunchBarrier.Await();
					completeLaunchBarrier.Await();
					//To ensure the kill is started before the launch
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (BrokenBarrierException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				throw new IOException(new TestContainerLauncherImpl.ContainerException("Force fail CM"
					));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual StopContainersResponse StopContainers(StopContainersRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		[System.Serializable]
		private class ContainerException : YarnException
		{
			public ContainerException(string message)
				: base(message)
			{
			}

			public override Exception InnerException
			{
				get
				{
					return null;
				}
			}
		}
	}
}
