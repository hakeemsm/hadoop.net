using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestEventFlow
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private static FilePath localDir = new FilePath("target", typeof(TestEventFlow).FullName
			 + "-localDir").GetAbsoluteFile();

		private static FilePath localLogDir = new FilePath("target", typeof(TestEventFlow
			).FullName + "-localLogDir").GetAbsoluteFile();

		private static FilePath remoteLogDir = new FilePath("target", typeof(TestEventFlow
			).FullName + "-remoteLogDir").GetAbsoluteFile();

		private const long SimulatedRmIdentifier = 1234;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccessfulContainerLaunch()
		{
			FileContext localFS = FileContext.GetLocalFSFileContext();
			localFS.Delete(new Path(localDir.GetAbsolutePath()), true);
			localFS.Delete(new Path(localLogDir.GetAbsolutePath()), true);
			localFS.Delete(new Path(remoteLogDir.GetAbsolutePath()), true);
			localDir.Mkdir();
			localLogDir.Mkdir();
			remoteLogDir.Mkdir();
			YarnConfiguration conf = new YarnConfiguration();
			Context context = new _NMContext_84(new NMContainerTokenSecretManager(conf), new 
				NMTokenSecretManagerInNM(), null, null, new NMNullStateStoreService());
			conf.Set(YarnConfiguration.NmLocalDirs, localDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogDir.GetAbsolutePath());
			ContainerExecutor exec = new DefaultContainerExecutor();
			exec.SetConf(conf);
			DeletionService del = new DeletionService(exec);
			Dispatcher dispatcher = new AsyncDispatcher();
			NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
			healthChecker.Init(conf);
			LocalDirsHandlerService dirsHandler = healthChecker.GetDiskHandler();
			NodeManagerMetrics metrics = NodeManagerMetrics.Create();
			NodeStatusUpdater nodeStatusUpdater = new _NodeStatusUpdaterImpl_106(context, dispatcher
				, healthChecker, metrics);
			// Don't start any updating thread.
			DummyContainerManager containerManager = new DummyContainerManager(context, exec, 
				del, nodeStatusUpdater, metrics, new ApplicationACLsManager(conf), dirsHandler);
			nodeStatusUpdater.Init(conf);
			((NodeManager.NMContext)context).SetContainerManager(containerManager);
			nodeStatusUpdater.Start();
			containerManager.Init(conf);
			containerManager.Start();
			ContainerLaunchContext launchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			ApplicationId applicationId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 0);
			ContainerId cID = ContainerId.NewContainerId(applicationAttemptId, 0);
			string user = "testing";
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(launchContext
				, TestContainerManager.CreateContainerToken(cID, SimulatedRmIdentifier, context.
				GetNodeId(), user, context.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cID, ContainerState
				.Running);
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cID);
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			containerManager.StopContainers(stopRequest);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cID, ContainerState
				.Complete);
			containerManager.Stop();
		}

		private sealed class _NMContext_84 : NodeManager.NMContext
		{
			public _NMContext_84(NMContainerTokenSecretManager baseArg1, NMTokenSecretManagerInNM
				 baseArg2, LocalDirsHandlerService baseArg3, ApplicationACLsManager baseArg4, NMStateStoreService
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			public override int GetHttpPort()
			{
				return 1234;
			}
		}

		private sealed class _NodeStatusUpdaterImpl_106 : NodeStatusUpdaterImpl
		{
			public _NodeStatusUpdaterImpl_106(Context baseArg1, Dispatcher baseArg2, NodeHealthCheckerService
				 baseArg3, NodeManagerMetrics baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
			}

			protected internal override ResourceTracker GetRMClient()
			{
				return new LocalRMInterface();
			}

			protected internal override void StopRMProxy()
			{
				return;
			}

			protected internal override void StartStatusUpdater()
			{
				return;
			}

			public override long GetRMIdentifier()
			{
				return TestEventFlow.SimulatedRmIdentifier;
			}
		}
	}
}
