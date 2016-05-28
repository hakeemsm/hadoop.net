using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	public class TestContainerLauncher
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal Configuration conf;

		internal Server server;

		internal static readonly Log Log = LogFactory.GetLog(typeof(TestContainerLauncher
			));

		/// <exception cref="System.Exception"/>
		public virtual void TestPoolSize()
		{
			ApplicationId appId = ApplicationId.NewInstance(12345, 67);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 3);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 8);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 9, TaskType.Map);
			AppContext context = Org.Mockito.Mockito.Mock<AppContext>();
			TestContainerLauncher.CustomContainerLauncher containerLauncher = new TestContainerLauncher.CustomContainerLauncher
				(this, context);
			containerLauncher.Init(new Configuration());
			containerLauncher.Start();
			ThreadPoolExecutor threadPool = containerLauncher.GetThreadPool();
			// No events yet
			NUnit.Framework.Assert.AreEqual(containerLauncher.initialPoolSize, MRJobConfig.DefaultMrAmContainerlauncherThreadpoolInitialSize
				);
			NUnit.Framework.Assert.AreEqual(0, threadPool.GetPoolSize());
			NUnit.Framework.Assert.AreEqual(containerLauncher.initialPoolSize, threadPool.GetCorePoolSize
				());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			containerLauncher.expectedCorePoolSize = containerLauncher.initialPoolSize;
			for (int i = 0; i < 10; i++)
			{
				ContainerId containerId = ContainerId.NewContainerId(appAttemptId, i);
				TaskAttemptId taskAttemptId = MRBuilderUtils.NewTaskAttemptId(taskId, i);
				containerLauncher.Handle(new ContainerLauncherEvent(taskAttemptId, containerId, "host"
					 + i + ":1234", null, ContainerLauncher.EventType.ContainerRemoteLaunch));
			}
			WaitForEvents(containerLauncher, 10);
			NUnit.Framework.Assert.AreEqual(10, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			// Same set of hosts, so no change
			containerLauncher.finishEventHandling = true;
			int timeOut = 0;
			while (containerLauncher.numEventsProcessed.Get() < 10 && timeOut++ < 200)
			{
				Log.Info("Waiting for number of events processed to become " + 10 + ". It is now "
					 + containerLauncher.numEventsProcessed.Get() + ". Timeout is " + timeOut);
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(10, containerLauncher.numEventsProcessed.Get());
			containerLauncher.finishEventHandling = false;
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				ContainerId containerId = ContainerId.NewContainerId(appAttemptId, i_1 + 10);
				TaskAttemptId taskAttemptId = MRBuilderUtils.NewTaskAttemptId(taskId, i_1 + 10);
				containerLauncher.Handle(new ContainerLauncherEvent(taskAttemptId, containerId, "host"
					 + i_1 + ":1234", null, ContainerLauncher.EventType.ContainerRemoteLaunch));
			}
			WaitForEvents(containerLauncher, 20);
			NUnit.Framework.Assert.AreEqual(10, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			// Different hosts, there should be an increase in core-thread-pool size to
			// 21(11hosts+10buffer)
			// Core pool size should be 21 but the live pool size should be only 11.
			containerLauncher.expectedCorePoolSize = 11 + containerLauncher.initialPoolSize;
			containerLauncher.finishEventHandling = false;
			ContainerId containerId_1 = ContainerId.NewContainerId(appAttemptId, 21);
			TaskAttemptId taskAttemptId_1 = MRBuilderUtils.NewTaskAttemptId(taskId, 21);
			containerLauncher.Handle(new ContainerLauncherEvent(taskAttemptId_1, containerId_1
				, "host11:1234", null, ContainerLauncher.EventType.ContainerRemoteLaunch));
			WaitForEvents(containerLauncher, 21);
			NUnit.Framework.Assert.AreEqual(11, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			containerLauncher.Stop();
			// change configuration MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE
			// and verify initialPoolSize value.
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MrAmContainerlauncherThreadpoolInitialSize, 20);
			containerLauncher = new TestContainerLauncher.CustomContainerLauncher(this, context
				);
			containerLauncher.Init(conf);
			NUnit.Framework.Assert.AreEqual(containerLauncher.initialPoolSize, 20);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPoolLimits()
		{
			ApplicationId appId = ApplicationId.NewInstance(12345, 67);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 3);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 8);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 9, TaskType.Map);
			TaskAttemptId taskAttemptId = MRBuilderUtils.NewTaskAttemptId(taskId, 0);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 10);
			AppContext context = Org.Mockito.Mockito.Mock<AppContext>();
			TestContainerLauncher.CustomContainerLauncher containerLauncher = new TestContainerLauncher.CustomContainerLauncher
				(this, context);
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.MrAmContainerlauncherThreadCountLimit, 12);
			containerLauncher.Init(conf);
			containerLauncher.Start();
			ThreadPoolExecutor threadPool = containerLauncher.GetThreadPool();
			// 10 different hosts
			containerLauncher.expectedCorePoolSize = containerLauncher.initialPoolSize;
			for (int i = 0; i < 10; i++)
			{
				containerLauncher.Handle(new ContainerLauncherEvent(taskAttemptId, containerId, "host"
					 + i + ":1234", null, ContainerLauncher.EventType.ContainerRemoteLaunch));
			}
			WaitForEvents(containerLauncher, 10);
			NUnit.Framework.Assert.AreEqual(10, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			// 4 more different hosts, but thread pool size should be capped at 12
			containerLauncher.expectedCorePoolSize = 12;
			for (int i_1 = 1; i_1 <= 4; i_1++)
			{
				containerLauncher.Handle(new ContainerLauncherEvent(taskAttemptId, containerId, "host1"
					 + i_1 + ":1234", null, ContainerLauncher.EventType.ContainerRemoteLaunch));
			}
			WaitForEvents(containerLauncher, 12);
			NUnit.Framework.Assert.AreEqual(12, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			// Make some threads ideal so that remaining events are also done.
			containerLauncher.finishEventHandling = true;
			WaitForEvents(containerLauncher, 14);
			NUnit.Framework.Assert.AreEqual(12, threadPool.GetPoolSize());
			NUnit.Framework.Assert.IsNull(containerLauncher.foundErrors);
			containerLauncher.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void WaitForEvents(TestContainerLauncher.CustomContainerLauncher containerLauncher
			, int expectedNumEvents)
		{
			int timeOut = 0;
			while (containerLauncher.numEventsProcessing.Get() < expectedNumEvents && timeOut
				++ < 20)
			{
				Log.Info("Waiting for number of events to become " + expectedNumEvents + ". It is now "
					 + containerLauncher.numEventsProcessing.Get());
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(expectedNumEvents, containerLauncher.numEventsProcessing
				.Get());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSlowNM()
		{
			conf = new Configuration();
			int maxAttempts = 1;
			conf.SetInt(MRJobConfig.MapMaxAttempts, maxAttempts);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			// set timeout low for the test
			conf.SetInt("yarn.rpc.nm-command-timeout", 3000);
			conf.Set(YarnConfiguration.IpcRpcImpl, typeof(HadoopYarnProtoRPC).FullName);
			YarnRPC rpc = YarnRPC.Create(conf);
			string bindAddr = "localhost:0";
			IPEndPoint addr = NetUtils.CreateSocketAddr(bindAddr);
			NMTokenSecretManagerInNM tokenSecretManager = new NMTokenSecretManagerInNM();
			MasterKey masterKey = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<MasterKey>();
			masterKey.SetBytes(ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("key")));
			tokenSecretManager.SetMasterKey(masterKey);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "token");
			server = rpc.GetServer(typeof(ContainerManagementProtocol), new TestContainerLauncher.DummyContainerManager
				(this), addr, conf, tokenSecretManager, 1);
			server.Start();
			MRApp app = new TestContainerLauncher.MRAppWithSlowNM(this, tokenSecretManager);
			try
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
				app.WaitForState(job, JobState.Running);
				IDictionary<TaskId, Task> tasks = job.GetTasks();
				NUnit.Framework.Assert.AreEqual("Num tasks is not correct", 1, tasks.Count);
				Task task = tasks.Values.GetEnumerator().Next();
				app.WaitForState(task, TaskState.Scheduled);
				IDictionary<TaskAttemptId, TaskAttempt> attempts = tasks.Values.GetEnumerator().Next
					().GetAttempts();
				NUnit.Framework.Assert.AreEqual("Num attempts is not correct", maxAttempts, attempts
					.Count);
				TaskAttempt attempt = attempts.Values.GetEnumerator().Next();
				app.WaitForInternalState((TaskAttemptImpl)attempt, TaskAttemptStateInternal.Assigned
					);
				app.WaitForState(job, JobState.Failed);
				string diagnostics = attempt.GetDiagnostics().ToString();
				Log.Info("attempt.getDiagnostics: " + diagnostics);
				NUnit.Framework.Assert.IsTrue(diagnostics.Contains("Container launch failed for "
					 + "container_0_0000_01_000000 : "));
				NUnit.Framework.Assert.IsTrue(diagnostics.Contains("java.net.SocketTimeoutException: 3000 millis timeout while waiting for channel"
					));
			}
			finally
			{
				server.Stop();
				app.Stop();
			}
		}

		private sealed class CustomContainerLauncher : ContainerLauncherImpl
		{
			private volatile int expectedCorePoolSize = 0;

			private AtomicInteger numEventsProcessing = new AtomicInteger(0);

			private AtomicInteger numEventsProcessed = new AtomicInteger(0);

			private volatile string foundErrors = null;

			private volatile bool finishEventHandling;

			private CustomContainerLauncher(TestContainerLauncher _enclosing, AppContext context
				)
				: base(context)
			{
				this._enclosing = _enclosing;
			}

			public ThreadPoolExecutor GetThreadPool()
			{
				return base.launcherPool;
			}

			private sealed class CustomEventProcessor : ContainerLauncherImpl.EventProcessor
			{
				private readonly ContainerLauncherEvent @event;

				private CustomEventProcessor(CustomContainerLauncher _enclosing, ContainerLauncherEvent
					 @event)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
					this.@event = @event;
				}

				public override void Run()
				{
					// do nothing substantial
					ContainerLauncherImpl.Log.Info("Processing the event " + this.@event.ToString());
					this._enclosing.numEventsProcessing.IncrementAndGet();
					// Stall
					while (!this._enclosing.finishEventHandling)
					{
						lock (this)
						{
							try
							{
								Sharpen.Runtime.Wait(this, 1000);
							}
							catch (Exception)
							{
							}
						}
					}
					this._enclosing.numEventsProcessed.IncrementAndGet();
				}

				private readonly CustomContainerLauncher _enclosing;
			}

			protected internal override ContainerLauncherImpl.EventProcessor CreateEventProcessor
				(ContainerLauncherEvent @event)
			{
				// At this point of time, the EventProcessor is being created and so no
				// additional threads would have been created.
				// Core-pool-size should have increased by now.
				if (this.expectedCorePoolSize != this.launcherPool.GetCorePoolSize())
				{
					this.foundErrors = "Expected " + this.expectedCorePoolSize + " but found " + this
						.launcherPool.GetCorePoolSize();
				}
				return new TestContainerLauncher.CustomContainerLauncher.CustomEventProcessor(this
					, @event);
			}

			private readonly TestContainerLauncher _enclosing;
		}

		private class MRAppWithSlowNM : MRApp
		{
			private NMTokenSecretManagerInNM tokenSecretManager;

			public MRAppWithSlowNM(TestContainerLauncher _enclosing, NMTokenSecretManagerInNM
				 tokenSecretManager)
				: base(1, 0, false, "TestContainerLauncher", true)
			{
				this._enclosing = _enclosing;
				this.tokenSecretManager = tokenSecretManager;
			}

			protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
				context)
			{
				return new _ContainerLauncherImpl_379(this, context);
			}

			private sealed class _ContainerLauncherImpl_379 : ContainerLauncherImpl
			{
				public _ContainerLauncherImpl_379(MRAppWithSlowNM _enclosing, AppContext baseArg1
					)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				public override ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
					 GetCMProxy(string containerMgrBindAddr, ContainerId containerId)
				{
					IPEndPoint addr = NetUtils.GetConnectAddress(this._enclosing._enclosing.server);
					string containerManagerBindAddr = addr.GetHostName() + ":" + addr.Port;
					Token token = this._enclosing.tokenSecretManager.CreateNMToken(containerId.GetApplicationAttemptId
						(), NodeId.NewInstance(addr.GetHostName(), addr.Port), "user");
					ContainerManagementProtocolProxy cmProxy = new ContainerManagementProtocolProxy(this
						._enclosing._enclosing.conf);
					ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = new 
						ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData(this, YarnRPC
						.Create(this._enclosing._enclosing.conf), containerManagerBindAddr, containerId, 
						token);
					return proxy;
				}

				private readonly MRAppWithSlowNM _enclosing;
			}

			private readonly TestContainerLauncher _enclosing;
		}

		public class DummyContainerManager : ContainerManagementProtocol
		{
			private ContainerStatus status = null;

			/// <exception cref="System.IO.IOException"/>
			public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				IList<ContainerStatus> statuses = new AList<ContainerStatus>();
				statuses.AddItem(this.status);
				return GetContainerStatusesResponse.NewInstance(statuses, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual StartContainersResponse StartContainers(StartContainersRequest requests
				)
			{
				StartContainerRequest request = requests.GetStartContainerRequests()[0];
				ContainerTokenIdentifier containerTokenIdentifier = MRApp.NewContainerTokenIdentifier
					(request.GetContainerToken());
				// Validate that the container is what RM is giving.
				NUnit.Framework.Assert.AreEqual(MRApp.NmHost + ":" + MRApp.NmPort, containerTokenIdentifier
					.GetNmHostAddress());
				StartContainersResponse response = TestContainerLauncher.recordFactory.NewRecordInstance
					<StartContainersResponse>();
				this.status = TestContainerLauncher.recordFactory.NewRecordInstance<ContainerStatus
					>();
				try
				{
					// make the thread sleep to look like its not going to respond
					Sharpen.Thread.Sleep(15000);
				}
				catch (Exception e)
				{
					TestContainerLauncher.Log.Error(e);
					throw new UndeclaredThrowableException(e);
				}
				this.status.SetState(ContainerState.Running);
				this.status.SetContainerId(containerTokenIdentifier.GetContainerID());
				this.status.SetExitStatus(0);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual StopContainersResponse StopContainers(StopContainersRequest request
				)
			{
				Exception e = new Exception("Dummy function", new Exception("Dummy function cause"
					));
				throw new IOException(e);
			}

			internal DummyContainerManager(TestContainerLauncher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestContainerLauncher _enclosing;
		}
	}
}
