using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public abstract class BaseContainerManagerTest
	{
		protected internal static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal static FileContext localFS;

		protected internal static FilePath localDir;

		protected internal static FilePath localLogDir;

		protected internal static FilePath remoteLogDir;

		protected internal static FilePath tmpDir;

		protected internal readonly NodeManagerMetrics metrics = NodeManagerMetrics.Create
			();

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public BaseContainerManagerTest()
		{
			context = new _NMContext_108(new NMContainerTokenSecretManager(conf), new NMTokenSecretManagerInNM
				(), null, new ApplicationACLsManager(conf), new NMNullStateStoreService());
			nodeStatusUpdater = new _NodeStatusUpdaterImpl_121(this, context, new AsyncDispatcher
				(), null, metrics);
			localFS = FileContext.GetLocalFSFileContext();
			localDir = new FilePath("target", this.GetType().Name + "-localDir").GetAbsoluteFile
				();
			localLogDir = new FilePath("target", this.GetType().Name + "-localLogDir").GetAbsoluteFile
				();
			remoteLogDir = new FilePath("target", this.GetType().Name + "-remoteLogDir").GetAbsoluteFile
				();
			tmpDir = new FilePath("target", this.GetType().Name + "-tmpDir");
		}

		protected internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.BaseContainerManagerTest
			));

		protected internal const int HttpPort = 5412;

		protected internal Configuration conf = new YarnConfiguration();

		private sealed class _NMContext_108 : NodeManager.NMContext
		{
			public _NMContext_108(NMContainerTokenSecretManager baseArg1, NMTokenSecretManagerInNM
				 baseArg2, LocalDirsHandlerService baseArg3, ApplicationACLsManager baseArg4, NMStateStoreService
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			public override int GetHttpPort()
			{
				return Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.BaseContainerManagerTest
					.HttpPort;
			}
		}

		protected internal Context context;

		protected internal ContainerExecutor exec;

		protected internal DeletionService delSrvc;

		protected internal string user = "nobody";

		protected internal NodeHealthCheckerService nodeHealthChecker;

		protected internal LocalDirsHandlerService dirsHandler;

		protected internal readonly long DummyRmIdentifier = 1234;

		private sealed class _NodeStatusUpdaterImpl_121 : NodeStatusUpdaterImpl
		{
			public _NodeStatusUpdaterImpl_121(BaseContainerManagerTest _enclosing, Context baseArg1
				, Dispatcher baseArg2, NodeHealthCheckerService baseArg3, NodeManagerMetrics baseArg4
				)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this._enclosing = _enclosing;
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

			// Don't start any updating thread.
			public override long GetRMIdentifier()
			{
				// There is no real RM registration, simulate and set RMIdentifier
				return this._enclosing.DummyRmIdentifier;
			}

			private readonly BaseContainerManagerTest _enclosing;
		}

		protected internal NodeStatusUpdater nodeStatusUpdater;

		protected internal ContainerManagerImpl containerManager = null;

		protected internal virtual ContainerExecutor CreateContainerExecutor()
		{
			DefaultContainerExecutor exec = new DefaultContainerExecutor();
			exec.SetConf(conf);
			return exec;
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			localFS.Delete(new Path(localDir.GetAbsolutePath()), true);
			localFS.Delete(new Path(tmpDir.GetAbsolutePath()), true);
			localFS.Delete(new Path(localLogDir.GetAbsolutePath()), true);
			localFS.Delete(new Path(remoteLogDir.GetAbsolutePath()), true);
			localDir.Mkdir();
			tmpDir.Mkdir();
			localLogDir.Mkdir();
			remoteLogDir.Mkdir();
			Log.Info("Created localDir in " + localDir.GetAbsolutePath());
			Log.Info("Created tmpDir in " + tmpDir.GetAbsolutePath());
			string bindAddress = "0.0.0.0:12345";
			conf.Set(YarnConfiguration.NmAddress, bindAddress);
			conf.Set(YarnConfiguration.NmLocalDirs, localDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogDir.GetAbsolutePath());
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			// Default delSrvc
			delSrvc = CreateDeletionService();
			delSrvc.Init(conf);
			exec = CreateContainerExecutor();
			nodeHealthChecker = new NodeHealthCheckerService();
			nodeHealthChecker.Init(conf);
			dirsHandler = nodeHealthChecker.GetDiskHandler();
			containerManager = CreateContainerManager(delSrvc);
			((NodeManager.NMContext)context).SetContainerManager(containerManager);
			nodeStatusUpdater.Init(conf);
			containerManager.Init(conf);
			nodeStatusUpdater.Start();
		}

		protected internal virtual ContainerManagerImpl CreateContainerManager(DeletionService
			 delSrvc)
		{
			return new _ContainerManagerImpl_191(context, exec, delSrvc, nodeStatusUpdater, metrics
				, new ApplicationACLsManager(conf), dirsHandler);
		}

		private sealed class _ContainerManagerImpl_191 : ContainerManagerImpl
		{
			public _ContainerManagerImpl_191(Context baseArg1, ContainerExecutor baseArg2, DeletionService
				 baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics baseArg5, ApplicationACLsManager
				 baseArg6, LocalDirsHandlerService baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override void SetBlockNewContainerRequests(bool blockNewContainerRequests)
			{
			}

			// do nothing
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override void AuthorizeGetAndStopContainerRequest(ContainerId 
				containerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container, bool stopRequest, NMTokenIdentifier identifier)
			{
			}

			// do nothing
			protected internal override void AuthorizeUser(UserGroupInformation remoteUgi, NMTokenIdentifier
				 nmTokenIdentifier)
			{
			}

			// do nothing
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override void AuthorizeStartRequest(NMTokenIdentifier nmTokenIdentifier
				, ContainerTokenIdentifier containerTokenIdentifier)
			{
			}

			// do nothing
			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			protected internal override void UpdateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier
				)
			{
			}

			// Do nothing
			public override IDictionary<string, ByteBuffer> GetAuxServiceMetaData()
			{
				IDictionary<string, ByteBuffer> serviceData = new Dictionary<string, ByteBuffer>(
					);
				serviceData["AuxService1"] = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("AuxServiceMetaData1"
					));
				serviceData["AuxService2"] = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("AuxServiceMetaData2"
					));
				return serviceData;
			}
		}

		protected internal virtual DeletionService CreateDeletionService()
		{
			return new _DeletionService_234(exec);
		}

		private sealed class _DeletionService_234 : DeletionService
		{
			public _DeletionService_234(ContainerExecutor baseArg1)
				: base(baseArg1)
			{
			}

			public override void Delete(string user, Path subDir, params Path[] baseDirs)
			{
				// Don't do any deletions.
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.BaseContainerManagerTest
					.Log.Info("Psuedo delete: user - " + user + ", subDir - " + subDir + ", baseDirs - "
					 + baseDirs);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (containerManager != null)
			{
				containerManager.Stop();
			}
			CreateContainerExecutor().DeleteAsUser(user, new Path(localDir.GetAbsolutePath())
				, new Path[] {  });
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void WaitForContainerState(ContainerManagementProtocol containerManager
			, ContainerId containerID, ContainerState finalState)
		{
			WaitForContainerState(containerManager, containerID, finalState, 20);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void WaitForContainerState(ContainerManagementProtocol containerManager
			, ContainerId containerID, ContainerState finalState, int timeOutMax)
		{
			IList<ContainerId> list = new AList<ContainerId>();
			list.AddItem(containerID);
			GetContainerStatusesRequest request = GetContainerStatusesRequest.NewInstance(list
				);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(request).
				GetContainerStatuses()[0];
			int timeoutSecs = 0;
			while (!containerStatus.GetState().Equals(finalState) && timeoutSecs++ < timeOutMax
				)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for container to get into state " + finalState + ". Current state is "
					 + containerStatus.GetState());
				containerStatus = containerManager.GetContainerStatuses(request).GetContainerStatuses
					()[0];
			}
			Log.Info("Container state is " + containerStatus.GetState());
			NUnit.Framework.Assert.AreEqual("ContainerState is not correct (timedout)", finalState
				, containerStatus.GetState());
		}

		/// <exception cref="System.Exception"/>
		internal static void WaitForApplicationState(ContainerManagerImpl containerManager
			, ApplicationId appID, ApplicationState finalState)
		{
			// Wait for app-finish
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = containerManager.GetContext().GetApplications()[appID];
			int timeout = 0;
			while (!(app.GetApplicationState().Equals(finalState)) && timeout++ < 15)
			{
				Log.Info("Waiting for app to reach " + finalState + ".. Current state is " + app.
					GetApplicationState());
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsTrue("App is not in " + finalState + " yet!! Timedout!!"
				, app.GetApplicationState().Equals(finalState));
		}
	}
}
