using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeManagerResync
	{
		internal static readonly FilePath basedir = new FilePath("target", typeof(TestNodeManagerResync
			).FullName);

		internal static readonly FilePath tmpDir = new FilePath(basedir, "tmpDir");

		internal static readonly FilePath logsDir = new FilePath(basedir, "logs");

		internal static readonly FilePath remoteLogsDir = new FilePath(basedir, "remotelogs"
			);

		internal static readonly FilePath nmLocalDir = new FilePath(basedir, "nm0");

		internal static readonly FilePath processStartFile = new FilePath(tmpDir, "start_file.txt"
			).GetAbsoluteFile();

		internal static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal const string user = "nobody";

		private FileContext localFS;

		private CyclicBarrier syncBarrier;

		private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

		private AtomicBoolean isNMShutdownCalled = new AtomicBoolean(false);

		private readonly NodeManagerEvent resyncEvent = new NodeManagerEvent(NodeManagerEventType
			.Resync);

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		[SetUp]
		public virtual void Setup()
		{
			localFS = FileContext.GetLocalFSFileContext();
			tmpDir.Mkdirs();
			logsDir.Mkdirs();
			remoteLogsDir.Mkdirs();
			nmLocalDir.Mkdirs();
			syncBarrier = new CyclicBarrier(2);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			localFS.Delete(new Path(basedir.GetPath()), true);
			assertionFailedInThread.Set(false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestKillContainersOnResync()
		{
			TestNodeManagerResync.TestNodeManager1 nm = new TestNodeManagerResync.TestNodeManager1
				(this, false);
			TestContainerPreservationOnResyncImpl(nm, false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestPreserveContainersOnResyncKeepingContainers()
		{
			TestNodeManagerResync.TestNodeManager1 nm = new TestNodeManagerResync.TestNodeManager1
				(this, true);
			TestContainerPreservationOnResyncImpl(nm, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void TestContainerPreservationOnResyncImpl(TestNodeManagerResync.TestNodeManager1
			 nm, bool isWorkPreservingRestartEnabled)
		{
			YarnConfiguration conf = CreateNMConfig();
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, isWorkPreservingRestartEnabled
				);
			try
			{
				nm.Init(conf);
				nm.Start();
				ContainerId cId = TestNodeManagerShutdown.CreateContainerId();
				TestNodeManagerShutdown.StartContainer(nm, cId, localFS, tmpDir, processStartFile
					);
				nm.SetExistingContainerId(cId);
				NUnit.Framework.Assert.AreEqual(1, ((TestNodeManagerResync.TestNodeManager1)nm).GetNMRegistrationCount
					());
				nm.GetNMDispatcher().GetEventHandler().Handle(resyncEvent);
				try
				{
					syncBarrier.Await();
				}
				catch (BrokenBarrierException)
				{
				}
				NUnit.Framework.Assert.AreEqual(2, ((TestNodeManagerResync.TestNodeManager1)nm).GetNMRegistrationCount
					());
				// Only containers should be killed on resync, apps should lie around.
				// That way local resources for apps can be used beyond resync without
				// relocalization
				NUnit.Framework.Assert.IsTrue(nm.GetNMContext().GetApplications().Contains(cId.GetApplicationAttemptId
					().GetApplicationId()));
				NUnit.Framework.Assert.IsFalse(assertionFailedInThread.Get());
			}
			finally
			{
				nm.Stop();
			}
		}

		// This test tests new container requests are blocked when NM starts from
		// scratch until it register with RM AND while NM is resyncing with RM
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestBlockNewContainerRequestsOnStartAndResync()
		{
			NodeManager nm = new TestNodeManagerResync.TestNodeManager2(this);
			YarnConfiguration conf = CreateNMConfig();
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			nm.Init(conf);
			nm.Start();
			// Start the container in running state
			ContainerId cId = TestNodeManagerShutdown.CreateContainerId();
			TestNodeManagerShutdown.StartContainer(nm, cId, localFS, tmpDir, processStartFile
				);
			nm.GetNMDispatcher().GetEventHandler().Handle(new NodeManagerEvent(NodeManagerEventType
				.Resync));
			try
			{
				syncBarrier.Await();
			}
			catch (BrokenBarrierException)
			{
			}
			NUnit.Framework.Assert.IsFalse(assertionFailedInThread.Get());
			nm.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestNMshutdownWhenResyncThrowException()
		{
			NodeManager nm = new TestNodeManagerResync.TestNodeManager3(this);
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			nm.Start();
			NUnit.Framework.Assert.AreEqual(1, ((TestNodeManagerResync.TestNodeManager3)nm).GetNMRegistrationCount
				());
			nm.GetNMDispatcher().GetEventHandler().Handle(new NodeManagerEvent(NodeManagerEventType
				.Resync));
			lock (isNMShutdownCalled)
			{
				while (isNMShutdownCalled.Get() == false)
				{
					try
					{
						Sharpen.Runtime.Wait(isNMShutdownCalled);
					}
					catch (Exception)
					{
					}
				}
			}
			NUnit.Framework.Assert.IsTrue("NM shutdown not called.", isNMShutdownCalled.Get()
				);
			nm.Stop();
		}

		// This is to test when NM gets the resync response from last heart beat, it
		// should be able to send the already-sent-via-last-heart-beat container
		// statuses again when it re-register with RM.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMSentContainerStatusOnResync()
		{
			ContainerStatus testCompleteContainer = TestNodeStatusUpdater.CreateContainerStatus
				(2, ContainerState.Complete);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = TestNodeStatusUpdater.GetMockContainer(testCompleteContainer);
			NMContainerStatus report = CreateNMContainerStatus(2, ContainerState.Complete);
			Org.Mockito.Mockito.When(container.GetNMContainerStatus()).ThenReturn(report);
			NodeManager nm = new _NodeManager_225(this, testCompleteContainer, container);
			// first register, no containers info.
			// put the completed container into the context
			// second register contains the completed container info.
			// first heartBeat contains the completed container info
			// notify RESYNC on first heartbeat.
			YarnConfiguration conf = CreateNMConfig();
			nm.Init(conf);
			nm.Start();
			try
			{
				syncBarrier.Await();
			}
			catch (BrokenBarrierException)
			{
			}
			NUnit.Framework.Assert.IsFalse(assertionFailedInThread.Get());
			nm.Stop();
		}

		private sealed class _NodeManager_225 : NodeManager
		{
			public _NodeManager_225(TestNodeManagerResync _enclosing, ContainerStatus testCompleteContainer
				, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container)
			{
				this._enclosing = _enclosing;
				this.testCompleteContainer = testCompleteContainer;
				this.container = container;
				this.registerCount = 0;
			}

			internal int registerCount;

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new _TestNodeStatusUpdaterResync_232(this, testCompleteContainer, container
					, context, dispatcher, healthChecker, this.metrics);
			}

			private sealed class _TestNodeStatusUpdaterResync_232 : TestNodeManagerResync.TestNodeStatusUpdaterResync
			{
				public _TestNodeStatusUpdaterResync_232(_NodeManager_225 _enclosing, ContainerStatus
					 testCompleteContainer, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container, Context baseArg1, Dispatcher baseArg2, NodeHealthCheckerService baseArg3
					, NodeManagerMetrics baseArg4)
					: base(_enclosing, baseArg1, baseArg2, baseArg3, baseArg4)
				{
					this._enclosing = _enclosing;
					this.testCompleteContainer = testCompleteContainer;
					this.container = container;
				}

				protected internal override ResourceTracker CreateResourceTracker()
				{
					return new _MockResourceTracker_235(this, testCompleteContainer, container);
				}

				private sealed class _MockResourceTracker_235 : MockNodeStatusUpdater.MockResourceTracker
				{
					public _MockResourceTracker_235(_TestNodeStatusUpdaterResync_232 _enclosing, ContainerStatus
						 testCompleteContainer, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
						 container)
					{
						this._enclosing = _enclosing;
						this.testCompleteContainer = testCompleteContainer;
						this.container = container;
					}

					/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
					/// <exception cref="System.IO.IOException"/>
					public override RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
						 request)
					{
						if (this._enclosing._enclosing.registerCount == 0)
						{
							try
							{
								NUnit.Framework.Assert.AreEqual(0, request.GetNMContainerStatuses().Count);
							}
							catch (Exception error)
							{
								Sharpen.Runtime.PrintStackTrace(error);
								this._enclosing._enclosing._enclosing.assertionFailedInThread.Set(true);
							}
							this._enclosing._enclosing.GetNMContext().GetContainers()[testCompleteContainer.GetContainerId
								()] = container;
							this._enclosing._enclosing.GetNMContext().GetApplications()[testCompleteContainer
								.GetContainerId().GetApplicationAttemptId().GetApplicationId()] = Org.Mockito.Mockito
								.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
								>();
						}
						else
						{
							IList<NMContainerStatus> statuses = request.GetNMContainerStatuses();
							try
							{
								NUnit.Framework.Assert.AreEqual(1, statuses.Count);
								NUnit.Framework.Assert.AreEqual(testCompleteContainer.GetContainerId(), statuses[
									0].GetContainerId());
							}
							catch (Exception error)
							{
								Sharpen.Runtime.PrintStackTrace(error);
								this._enclosing._enclosing._enclosing.assertionFailedInThread.Set(true);
							}
						}
						this._enclosing._enclosing.registerCount++;
						return base.RegisterNodeManager(request);
					}

					public override NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
					{
						IList<ContainerStatus> statuses = request.GetNodeStatus().GetContainersStatuses();
						try
						{
							NUnit.Framework.Assert.AreEqual(1, statuses.Count);
							NUnit.Framework.Assert.AreEqual(testCompleteContainer.GetContainerId(), statuses[
								0].GetContainerId());
						}
						catch (Exception error)
						{
							Sharpen.Runtime.PrintStackTrace(error);
							this._enclosing._enclosing._enclosing.assertionFailedInThread.Set(true);
						}
						return YarnServerBuilderUtils.NewNodeHeartbeatResponse(1, NodeAction.Resync, null
							, null, null, null, 1000L);
					}

					private readonly _TestNodeStatusUpdaterResync_232 _enclosing;

					private readonly ContainerStatus testCompleteContainer;

					private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
						 container;
				}

				private readonly _NodeManager_225 _enclosing;

				private readonly ContainerStatus testCompleteContainer;

				private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container;
			}

			private readonly TestNodeManagerResync _enclosing;

			private readonly ContainerStatus testCompleteContainer;

			private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container;
		}

		internal class TestNodeStatusUpdaterResync : MockNodeStatusUpdater
		{
			public TestNodeStatusUpdaterResync(TestNodeManagerResync _enclosing, Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics
				 metrics)
				: base(context, dispatcher, healthChecker, metrics)
			{
				this._enclosing = _enclosing;
			}

			// This can be used as a common base class for testing NM resync behavior.
			protected internal override void RebootNodeStatusUpdaterAndRegisterWithRM()
			{
				try
				{
					// Wait here so as to sync with the main test thread.
					base.RebootNodeStatusUpdaterAndRegisterWithRM();
					this._enclosing.syncBarrier.Await();
				}
				catch (Exception)
				{
				}
				catch (BrokenBarrierException)
				{
				}
				catch (Exception ae)
				{
					Sharpen.Runtime.PrintStackTrace(ae);
					this._enclosing.assertionFailedInThread.Set(true);
				}
			}

			private readonly TestNodeManagerResync _enclosing;
		}

		private YarnConfiguration CreateNMConfig()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.NmPmemMb, 5 * 1024);
			// 5GB
			conf.Set(YarnConfiguration.NmAddress, "127.0.0.1:12345");
			conf.Set(YarnConfiguration.NmLocalizerAddress, "127.0.0.1:12346");
			conf.Set(YarnConfiguration.NmLogDirs, logsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogsDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLocalDirs, nmLocalDir.GetAbsolutePath());
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			return conf;
		}

		internal class TestNodeManager1 : NodeManager
		{
			private int registrationCount = 0;

			private bool containersShouldBePreserved;

			private ContainerId existingCid;

			public TestNodeManager1(TestNodeManagerResync _enclosing, bool containersShouldBePreserved
				)
			{
				this._enclosing = _enclosing;
				this.containersShouldBePreserved = containersShouldBePreserved;
			}

			public virtual void SetExistingContainerId(ContainerId cId)
			{
				this.existingCid = cId;
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new TestNodeManagerResync.TestNodeManager1.TestNodeStatusUpdaterImpl1(this
					, context, dispatcher, healthChecker, this.metrics);
			}

			public virtual int GetNMRegistrationCount()
			{
				return this.registrationCount;
			}

			internal class TestNodeStatusUpdaterImpl1 : MockNodeStatusUpdater
			{
				public TestNodeStatusUpdaterImpl1(TestNodeManager1 _enclosing, Context context, Dispatcher
					 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
					: base(context, dispatcher, healthChecker, metrics)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				protected internal override void RegisterWithRM()
				{
					base.RegisterWithRM();
					this._enclosing.registrationCount++;
				}

				protected internal override void RebootNodeStatusUpdaterAndRegisterWithRM()
				{
					ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
						> containers = this._enclosing.GetNMContext().GetContainers();
					try
					{
						try
						{
							if (this._enclosing.containersShouldBePreserved)
							{
								NUnit.Framework.Assert.IsFalse(containers.IsEmpty());
								NUnit.Framework.Assert.IsTrue(containers.Contains(this._enclosing.existingCid));
								NUnit.Framework.Assert.AreEqual(ContainerState.Running, containers[this._enclosing
									.existingCid].CloneAndGetContainerStatus().GetState());
							}
							else
							{
								// ensure that containers are empty or are completed before
								// restart nodeStatusUpdater
								if (!containers.IsEmpty())
								{
									NUnit.Framework.Assert.AreEqual(ContainerState.Complete, containers[this._enclosing
										.existingCid].CloneAndGetContainerStatus().GetState());
								}
							}
							base.RebootNodeStatusUpdaterAndRegisterWithRM();
						}
						catch (Exception ae)
						{
							Sharpen.Runtime.PrintStackTrace(ae);
							this._enclosing._enclosing.assertionFailedInThread.Set(true);
						}
						finally
						{
							this._enclosing._enclosing.syncBarrier.Await();
						}
					}
					catch (Exception)
					{
					}
					catch (BrokenBarrierException)
					{
					}
					catch (Exception ae)
					{
						Sharpen.Runtime.PrintStackTrace(ae);
						this._enclosing._enclosing.assertionFailedInThread.Set(true);
					}
				}

				private readonly TestNodeManager1 _enclosing;
			}

			private readonly TestNodeManagerResync _enclosing;
		}

		internal class TestNodeManager2 : NodeManager
		{
			internal Sharpen.Thread launchContainersThread = null;

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new TestNodeManagerResync.TestNodeManager2.TestNodeStatusUpdaterImpl2(this
					, context, dispatcher, healthChecker, this.metrics);
			}

			protected internal override ContainerManagerImpl CreateContainerManager(Context context
				, ContainerExecutor exec, DeletionService del, NodeStatusUpdater nodeStatusUpdater
				, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler)
			{
				return new _ContainerManagerImpl_438(this, context, exec, del, nodeStatusUpdater, 
					this.metrics, aclsManager, dirsHandler);
			}

			private sealed class _ContainerManagerImpl_438 : ContainerManagerImpl
			{
				public _ContainerManagerImpl_438(TestNodeManager2 _enclosing, Context baseArg1, ContainerExecutor
					 baseArg2, DeletionService baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics
					 baseArg5, ApplicationACLsManager baseArg6, LocalDirsHandlerService baseArg7)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
				{
					this._enclosing = _enclosing;
				}

				public override void SetBlockNewContainerRequests(bool blockNewContainerRequests)
				{
					if (blockNewContainerRequests)
					{
						// start test thread right after blockNewContainerRequests is set
						// true
						base.SetBlockNewContainerRequests(blockNewContainerRequests);
						this._enclosing.launchContainersThread = new TestNodeManagerResync.TestNodeManager2.RejectedContainersLauncherThread
							(this);
						this._enclosing.launchContainersThread.Start();
					}
					else
					{
						// join the test thread right before blockNewContainerRequests is
						// reset
						try
						{
							// stop the test thread
							((TestNodeManagerResync.TestNodeManager2.RejectedContainersLauncherThread)this._enclosing
								.launchContainersThread).SetStopThreadFlag(true);
							this._enclosing.launchContainersThread.Join();
							((TestNodeManagerResync.TestNodeManager2.RejectedContainersLauncherThread)this._enclosing
								.launchContainersThread).SetStopThreadFlag(false);
							base.SetBlockNewContainerRequests(blockNewContainerRequests);
						}
						catch (Exception e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
						}
					}
				}

				private readonly TestNodeManager2 _enclosing;
			}

			internal class TestNodeStatusUpdaterImpl2 : MockNodeStatusUpdater
			{
				public TestNodeStatusUpdaterImpl2(TestNodeManager2 _enclosing, Context context, Dispatcher
					 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
					: base(context, dispatcher, healthChecker, metrics)
				{
					this._enclosing = _enclosing;
				}

				protected internal override void RebootNodeStatusUpdaterAndRegisterWithRM()
				{
					ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
						> containers = this._enclosing.GetNMContext().GetContainers();
					try
					{
						// ensure that containers are empty before restart nodeStatusUpdater
						if (!containers.IsEmpty())
						{
							foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
								 container in containers.Values)
							{
								NUnit.Framework.Assert.AreEqual(ContainerState.Complete, container.CloneAndGetContainerStatus
									().GetState());
							}
						}
						base.RebootNodeStatusUpdaterAndRegisterWithRM();
						// After this point new containers are free to be launched, except
						// containers from previous RM
						// Wait here so as to sync with the main test thread.
						this._enclosing._enclosing.syncBarrier.Await();
					}
					catch (Exception)
					{
					}
					catch (BrokenBarrierException)
					{
					}
					catch (Exception ae)
					{
						Sharpen.Runtime.PrintStackTrace(ae);
						this._enclosing._enclosing.assertionFailedInThread.Set(true);
					}
				}

				private readonly TestNodeManager2 _enclosing;
			}

			internal class RejectedContainersLauncherThread : Sharpen.Thread
			{
				internal bool isStopped = false;

				public virtual void SetStopThreadFlag(bool isStopped)
				{
					this.isStopped = isStopped;
				}

				public override void Run()
				{
					int numContainers = 0;
					int numContainersRejected = 0;
					ContainerLaunchContext containerLaunchContext = TestNodeManagerResync.recordFactory
						.NewRecordInstance<ContainerLaunchContext>();
					try
					{
						while (!this.isStopped && numContainers < 10)
						{
							StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
								, null);
							IList<StartContainerRequest> list = new AList<StartContainerRequest>();
							list.AddItem(scRequest);
							StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
							System.Console.Out.WriteLine("no. of containers to be launched: " + numContainers
								);
							numContainers++;
							try
							{
								this._enclosing.GetContainerManager().StartContainers(allRequests);
							}
							catch (YarnException e)
							{
								numContainersRejected++;
								NUnit.Framework.Assert.IsTrue(e.Message.Contains("Rejecting new containers as NodeManager has not"
									 + " yet connected with ResourceManager"));
								NUnit.Framework.Assert.AreEqual(typeof(NMNotYetReadyException).FullName, e.GetType
									().FullName);
							}
							catch (IOException e)
							{
								Sharpen.Runtime.PrintStackTrace(e);
								this._enclosing._enclosing.assertionFailedInThread.Set(true);
							}
						}
						// no. of containers to be launched should equal to no. of
						// containers rejected
						NUnit.Framework.Assert.AreEqual(numContainers, numContainersRejected);
					}
					catch (Exception)
					{
						this._enclosing._enclosing.assertionFailedInThread.Set(true);
					}
				}

				internal RejectedContainersLauncherThread(TestNodeManager2 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly TestNodeManager2 _enclosing;
			}

			internal TestNodeManager2(TestNodeManagerResync _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeManagerResync _enclosing;
		}

		internal class TestNodeManager3 : NodeManager
		{
			private int registrationCount = 0;

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new TestNodeManagerResync.TestNodeManager3.TestNodeStatusUpdaterImpl3(this
					, context, dispatcher, healthChecker, this.metrics);
			}

			public virtual int GetNMRegistrationCount()
			{
				return this.registrationCount;
			}

			protected internal override void ShutDown()
			{
				lock (this._enclosing.isNMShutdownCalled)
				{
					this._enclosing.isNMShutdownCalled.Set(true);
					Sharpen.Runtime.Notify(this._enclosing.isNMShutdownCalled);
				}
			}

			internal class TestNodeStatusUpdaterImpl3 : MockNodeStatusUpdater
			{
				public TestNodeStatusUpdaterImpl3(TestNodeManager3 _enclosing, Context context, Dispatcher
					 dispatcher, NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics)
					: base(context, dispatcher, healthChecker, metrics)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				protected internal override void RegisterWithRM()
				{
					base.RegisterWithRM();
					this._enclosing.registrationCount++;
					if (this._enclosing.registrationCount > 1)
					{
						throw new YarnRuntimeException("Registration with RM failed.");
					}
				}

				private readonly TestNodeManager3 _enclosing;
			}

			internal TestNodeManager3(TestNodeManagerResync _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNodeManagerResync _enclosing;
		}

		public static NMContainerStatus CreateNMContainerStatus(int id, ContainerState containerState
			)
		{
			ApplicationId applicationId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId containerId = ContainerId.NewContainerId(applicationAttemptId, id);
			NMContainerStatus containerReport = NMContainerStatus.NewInstance(containerId, containerState
				, Resource.NewInstance(1024, 1), "recover container", 0, Priority.NewInstance(10
				), 0);
			return containerReport;
		}
	}
}
