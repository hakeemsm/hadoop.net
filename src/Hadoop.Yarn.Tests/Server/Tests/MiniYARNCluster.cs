using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	/// <summary>Embedded Yarn minicluster for testcases that need to interact with a cluster.
	/// 	</summary>
	/// <remarks>
	/// Embedded Yarn minicluster for testcases that need to interact with a cluster.
	/// <p/>
	/// In a real cluster, resource request matching is done using the hostname, and
	/// by default Yarn minicluster works in the exact same way as a real cluster.
	/// <p/>
	/// If a testcase needs to use multiple nodes and exercise resource request
	/// matching to a specific node, then the property
	/// <YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME/>
	/// should be set
	/// <code>true</code> in the configuration used to initialize the minicluster.
	/// <p/>
	/// With this property set to <code>true</code>, the matching will be done using
	/// the <code>hostname:port</code> of the namenodes. In such case, the AM must
	/// do resource request using <code>hostname:port</code> as the location.
	/// </remarks>
	public class MiniYARNCluster : CompositeService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.MiniYARNCluster
			));

		static MiniYARNCluster()
		{
			// temp fix until metrics system can auto-detect itself running in unit test:
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		private NodeManager[] nodeManagers;

		private ResourceManager[] resourceManagers;

		private string[] rmIds;

		private ApplicationHistoryServer appHistoryServer;

		private bool useFixedPorts;

		private bool useRpc = false;

		private int failoverTimeout;

		private ConcurrentMap<ApplicationAttemptId, long> appMasters = new ConcurrentHashMap
			<ApplicationAttemptId, long>(16, 0.75f, 2);

		private FilePath testWorkDir;

		private int numLocalDirs;

		private int numLogDirs;

		private bool enableAHS;

		/// <param name="testName">name of the test</param>
		/// <param name="numResourceManagers">the number of resource managers in the cluster</param>
		/// <param name="numNodeManagers">the number of node managers in the cluster</param>
		/// <param name="numLocalDirs">the number of nm-local-dirs per nodemanager</param>
		/// <param name="numLogDirs">the number of nm-log-dirs per nodemanager</param>
		/// <param name="enableAHS">enable ApplicationHistoryServer or not</param>
		public MiniYARNCluster(string testName, int numResourceManagers, int numNodeManagers
			, int numLocalDirs, int numLogDirs, bool enableAHS)
			: base(testName.Replace("$", string.Empty))
		{
			// Number of nm-local-dirs per nodemanager
			// Number of nm-log-dirs per nodemanager
			this.numLocalDirs = numLocalDirs;
			this.numLogDirs = numLogDirs;
			this.enableAHS = enableAHS;
			string testSubDir = testName.Replace("$", string.Empty);
			FilePath targetWorkDir = new FilePath("target", testSubDir);
			try
			{
				FileContext.GetLocalFSFileContext().Delete(new Path(targetWorkDir.GetAbsolutePath
					()), true);
			}
			catch (Exception e)
			{
				Log.Warn("COULD NOT CLEANUP", e);
				throw new YarnRuntimeException("could not cleanup test dir: " + e, e);
			}
			if (Shell.Windows)
			{
				// The test working directory can exceed the maximum path length supported
				// by some Windows APIs and cmd.exe (260 characters).  To work around this,
				// create a symlink in temporary storage with a much shorter path,
				// targeting the full path to the test working directory.  Then, use the
				// symlink as the test working directory.
				string targetPath = targetWorkDir.GetAbsolutePath();
				FilePath link = new FilePath(Runtime.GetProperty("java.io.tmpdir"), Runtime.CurrentTimeMillis
					().ToString());
				string linkPath = link.GetAbsolutePath();
				try
				{
					FileContext.GetLocalFSFileContext().Delete(new Path(linkPath), true);
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException("could not cleanup symlink: " + linkPath, e);
				}
				// Guarantee target exists before creating symlink.
				targetWorkDir.Mkdirs();
				Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(Shell.GetSymlinkCommand
					(targetPath, linkPath));
				try
				{
					shexec.Execute();
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException(string.Format("failed to create symlink from %s to %s, shell output: %s"
						, linkPath, targetPath, shexec.GetOutput()), e);
				}
				this.testWorkDir = link;
			}
			else
			{
				this.testWorkDir = targetWorkDir;
			}
			resourceManagers = new ResourceManager[numResourceManagers];
			nodeManagers = new NodeManager[numNodeManagers];
		}

		/// <param name="testName">name of the test</param>
		/// <param name="numResourceManagers">the number of resource managers in the cluster</param>
		/// <param name="numNodeManagers">the number of node managers in the cluster</param>
		/// <param name="numLocalDirs">the number of nm-local-dirs per nodemanager</param>
		/// <param name="numLogDirs">the number of nm-log-dirs per nodemanager</param>
		public MiniYARNCluster(string testName, int numResourceManagers, int numNodeManagers
			, int numLocalDirs, int numLogDirs)
			: this(testName, numResourceManagers, numNodeManagers, numLocalDirs, numLogDirs, 
				false)
		{
		}

		/// <param name="testName">name of the test</param>
		/// <param name="numNodeManagers">the number of node managers in the cluster</param>
		/// <param name="numLocalDirs">the number of nm-local-dirs per nodemanager</param>
		/// <param name="numLogDirs">the number of nm-log-dirs per nodemanager</param>
		public MiniYARNCluster(string testName, int numNodeManagers, int numLocalDirs, int
			 numLogDirs)
			: this(testName, 1, numNodeManagers, numLocalDirs, numLogDirs)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			useFixedPorts = conf.GetBoolean(YarnConfiguration.YarnMiniclusterFixedPorts, YarnConfiguration
				.DefaultYarnMiniclusterFixedPorts);
			useRpc = conf.GetBoolean(YarnConfiguration.YarnMiniclusterUseRpc, YarnConfiguration
				.DefaultYarnMiniclusterUseRpc);
			failoverTimeout = conf.GetInt(YarnConfiguration.RmZkTimeoutMs, YarnConfiguration.
				DefaultRmZkTimeoutMs);
			if (useRpc && !useFixedPorts)
			{
				throw new YarnRuntimeException("Invalid configuration!" + " Minicluster can use rpc only when configured to use fixed ports"
					);
			}
			conf.SetBoolean(YarnConfiguration.IsMiniYarnCluster, true);
			if (resourceManagers.Length > 1)
			{
				conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
				if (conf.Get(YarnConfiguration.RmHaIds) == null)
				{
					StringBuilder rmIds = new StringBuilder();
					for (int i = 0; i < resourceManagers.Length; i++)
					{
						if (i != 0)
						{
							rmIds.Append(",");
						}
						rmIds.Append("rm" + i);
					}
					conf.Set(YarnConfiguration.RmHaIds, rmIds.ToString());
				}
				ICollection<string> rmIdsCollection = HAUtil.GetRMHAIds(conf);
				rmIds = Sharpen.Collections.ToArray(rmIdsCollection, new string[rmIdsCollection.Count
					]);
			}
			for (int i_1 = 0; i_1 < resourceManagers.Length; i_1++)
			{
				resourceManagers[i_1] = CreateResourceManager();
				if (!useFixedPorts)
				{
					if (HAUtil.IsHAEnabled(conf))
					{
						SetHARMConfigurationWithEphemeralPorts(i_1, conf);
					}
					else
					{
						SetNonHARMConfigurationWithEphemeralPorts(conf);
					}
				}
				AddService(new MiniYARNCluster.ResourceManagerWrapper(this, i_1));
			}
			for (int index = 0; index < nodeManagers.Length; index++)
			{
				nodeManagers[index] = useRpc ? new MiniYARNCluster.CustomNodeManager(this) : new 
					MiniYARNCluster.ShortCircuitedNodeManager(this);
				AddService(new MiniYARNCluster.NodeManagerWrapper(this, index));
			}
			if (conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, YarnConfiguration.DefaultTimelineServiceEnabled
				) || enableAHS)
			{
				AddService(new MiniYARNCluster.ApplicationHistoryServerWrapper(this));
			}
			base.ServiceInit(conf is YarnConfiguration ? conf : new YarnConfiguration(conf));
		}

		private void SetNonHARMConfigurationWithEphemeralPorts(Configuration conf)
		{
			string hostname = Org.Apache.Hadoop.Yarn.Server.MiniYARNCluster.GetHostname();
			conf.Set(YarnConfiguration.RmAddress, hostname + ":0");
			conf.Set(YarnConfiguration.RmAdminAddress, hostname + ":0");
			conf.Set(YarnConfiguration.RmSchedulerAddress, hostname + ":0");
			conf.Set(YarnConfiguration.RmResourceTrackerAddress, hostname + ":0");
			WebAppUtils.SetRMWebAppHostnameAndPort(conf, hostname, 0);
		}

		private void SetHARMConfigurationWithEphemeralPorts(int index, Configuration conf
			)
		{
			string hostname = Org.Apache.Hadoop.Yarn.Server.MiniYARNCluster.GetHostname();
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				conf.Set(HAUtil.AddSuffix(confKey, rmIds[index]), hostname + ":0");
			}
		}

		private void InitResourceManager(int index, Configuration conf)
		{
			lock (this)
			{
				if (HAUtil.IsHAEnabled(conf))
				{
					conf.Set(YarnConfiguration.RmHaId, rmIds[index]);
				}
				resourceManagers[index].Init(conf);
				resourceManagers[index].GetRMContext().GetDispatcher().Register(typeof(RMAppAttemptEventType
					), new _EventHandler_296(this));
			}
		}

		private sealed class _EventHandler_296 : EventHandler<RMAppAttemptEvent>
		{
			public _EventHandler_296(MiniYARNCluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Handle(RMAppAttemptEvent @event)
			{
				if (@event is RMAppAttemptRegistrationEvent)
				{
					this._enclosing.appMasters[@event.GetApplicationAttemptId()] = @event.GetTimestamp
						();
				}
				else
				{
					if (@event is RMAppAttemptUnregistrationEvent)
					{
						Sharpen.Collections.Remove(this._enclosing.appMasters, @event.GetApplicationAttemptId
							());
					}
				}
			}

			private readonly MiniYARNCluster _enclosing;
		}

		private void StartResourceManager(int index)
		{
			lock (this)
			{
				try
				{
					Sharpen.Thread rmThread = new _Thread_310(this, index);
					rmThread.SetName("RM-" + index);
					rmThread.Start();
					int waitCount = 0;
					while (resourceManagers[index].GetServiceState() == Service.STATE.Inited && waitCount
						++ < 60)
					{
						Log.Info("Waiting for RM to start...");
						Sharpen.Thread.Sleep(1500);
					}
					if (resourceManagers[index].GetServiceState() != Service.STATE.Started)
					{
						// RM could have failed.
						throw new IOException("ResourceManager failed to start. Final state is " + resourceManagers
							[index].GetServiceState());
					}
				}
				catch (Exception t)
				{
					throw new YarnRuntimeException(t);
				}
				Log.Info("MiniYARN ResourceManager address: " + GetConfig().Get(YarnConfiguration
					.RmAddress));
				Log.Info("MiniYARN ResourceManager web address: " + WebAppUtils.GetRMWebAppURLWithoutScheme
					(GetConfig()));
			}
		}

		private sealed class _Thread_310 : Sharpen.Thread
		{
			public _Thread_310(MiniYARNCluster _enclosing, int index)
			{
				this._enclosing = _enclosing;
				this.index = index;
			}

			public override void Run()
			{
				this._enclosing.resourceManagers[index].Start();
			}

			private readonly MiniYARNCluster _enclosing;

			private readonly int index;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void StopResourceManager(int index)
		{
			lock (this)
			{
				if (resourceManagers[index] != null)
				{
					resourceManagers[index].Stop();
					resourceManagers[index] = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void RestartResourceManager(int index)
		{
			lock (this)
			{
				if (resourceManagers[index] != null)
				{
					resourceManagers[index].Stop();
					resourceManagers[index] = null;
				}
				Configuration conf = GetConfig();
				resourceManagers[index] = new ResourceManager();
				InitResourceManager(index, GetConfig());
				StartResourceManager(index);
			}
		}

		public virtual FilePath GetTestWorkDir()
		{
			return testWorkDir;
		}

		/// <summary>In a HA cluster, go through all the RMs and find the Active RM.</summary>
		/// <remarks>
		/// In a HA cluster, go through all the RMs and find the Active RM. In a
		/// non-HA cluster, return the index of the only RM.
		/// </remarks>
		/// <returns>index of the active RM or -1 if none of them turn active</returns>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual int GetActiveRMIndex()
		{
			if (resourceManagers.Length == 1)
			{
				return 0;
			}
			int numRetriesForRMBecomingActive = failoverTimeout / 100;
			while (numRetriesForRMBecomingActive-- > 0)
			{
				for (int i = 0; i < resourceManagers.Length; i++)
				{
					if (resourceManagers[i] == null)
					{
						continue;
					}
					try
					{
						if (HAServiceProtocol.HAServiceState.Active == resourceManagers[i].GetRMContext()
							.GetRMAdminService().GetServiceStatus().GetState())
						{
							return i;
						}
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException("Couldn't read the status of " + "a ResourceManger in the HA ensemble."
							, e);
					}
				}
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
					throw new YarnRuntimeException("Interrupted while waiting for one " + "of the ResourceManagers to become active"
						);
				}
			}
			return -1;
		}

		/// <returns>
		/// the active
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceManager"/>
		/// of the cluster,
		/// null if none of them are active.
		/// </returns>
		public virtual ResourceManager GetResourceManager()
		{
			int activeRMIndex = GetActiveRMIndex();
			return activeRMIndex == -1 ? null : this.resourceManagers[activeRMIndex];
		}

		public virtual ResourceManager GetResourceManager(int i)
		{
			return this.resourceManagers[i];
		}

		public virtual NodeManager GetNodeManager(int i)
		{
			return this.nodeManagers[i];
		}

		public static string GetHostname()
		{
			try
			{
				return Sharpen.Runtime.GetLocalHost().GetHostName();
			}
			catch (UnknownHostException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		private class ResourceManagerWrapper : AbstractService
		{
			private int index;

			public ResourceManagerWrapper(MiniYARNCluster _enclosing, int i)
				: base(typeof(MiniYARNCluster.ResourceManagerWrapper).FullName + "_" + i)
			{
				this._enclosing = _enclosing;
				this.index = i;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				lock (this)
				{
					this._enclosing.InitResourceManager(this.index, conf);
					base.ServiceInit(conf);
				}
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				lock (this)
				{
					this._enclosing.StartResourceManager(this.index);
					MiniYARNCluster.Log.Info("MiniYARN ResourceManager address: " + this.GetConfig().
						Get(YarnConfiguration.RmAddress));
					MiniYARNCluster.Log.Info("MiniYARN ResourceManager web address: " + WebAppUtils.GetRMWebAppURLWithoutScheme
						(this.GetConfig()));
					base.ServiceStart();
				}
			}

			/// <exception cref="System.Exception"/>
			private void WaitForAppMastersToFinish(long timeoutMillis)
			{
				long started = Runtime.CurrentTimeMillis();
				lock (this._enclosing.appMasters)
				{
					while (!this._enclosing.appMasters.IsEmpty() && Runtime.CurrentTimeMillis() - started
						 < timeoutMillis)
					{
						Sharpen.Runtime.Wait(this._enclosing.appMasters, 1000);
					}
				}
				if (!this._enclosing.appMasters.IsEmpty())
				{
					MiniYARNCluster.Log.Warn("Stopping RM while some app masters are still alive");
				}
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				lock (this)
				{
					if (this._enclosing.resourceManagers[this.index] != null)
					{
						this.WaitForAppMastersToFinish(5000);
						this._enclosing.resourceManagers[this.index].Stop();
					}
					if (Shell.Windows)
					{
						// On Windows, clean up the short temporary symlink that was created to
						// work around path length limitation.
						string testWorkDirPath = this._enclosing.testWorkDir.GetAbsolutePath();
						try
						{
							FileContext.GetLocalFSFileContext().Delete(new Path(testWorkDirPath), true);
						}
						catch (IOException)
						{
							MiniYARNCluster.Log.Warn("could not cleanup symlink: " + this._enclosing.testWorkDir
								.GetAbsolutePath());
						}
					}
					base.ServiceStop();
				}
			}

			private readonly MiniYARNCluster _enclosing;
		}

		private class NodeManagerWrapper : AbstractService
		{
			internal int index = 0;

			public NodeManagerWrapper(MiniYARNCluster _enclosing, int i)
				: base(typeof(MiniYARNCluster.NodeManagerWrapper).FullName + "_" + i)
			{
				this._enclosing = _enclosing;
				this.index = i;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				lock (this)
				{
					Configuration config = new YarnConfiguration(conf);
					// create nm-local-dirs and configure them for the nodemanager
					string localDirsString = this.PrepareDirs("local", this._enclosing.numLocalDirs);
					config.Set(YarnConfiguration.NmLocalDirs, localDirsString);
					// create nm-log-dirs and configure them for the nodemanager
					string logDirsString = this.PrepareDirs("log", this._enclosing.numLogDirs);
					config.Set(YarnConfiguration.NmLogDirs, logDirsString);
					config.SetInt(YarnConfiguration.NmPmemMb, config.GetInt(YarnConfiguration.YarnMiniclusterNmPmemMb
						, YarnConfiguration.DefaultYarnMiniclusterNmPmemMb));
					config.Set(YarnConfiguration.NmAddress, MiniYARNCluster.GetHostname() + ":0");
					config.Set(YarnConfiguration.NmLocalizerAddress, MiniYARNCluster.GetHostname() + 
						":0");
					WebAppUtils.SetNMWebAppHostNameAndPort(config, MiniYARNCluster.GetHostname(), 0);
					// Disable resource checks by default
					if (!config.GetBoolean(YarnConfiguration.YarnMiniclusterControlResourceMonitoring
						, YarnConfiguration.DefaultYarnMiniclusterControlResourceMonitoring))
					{
						config.SetBoolean(YarnConfiguration.NmPmemCheckEnabled, false);
						config.SetBoolean(YarnConfiguration.NmVmemCheckEnabled, false);
					}
					MiniYARNCluster.Log.Info("Starting NM: " + this.index);
					this._enclosing.nodeManagers[this.index].Init(config);
					base.ServiceInit(config);
				}
			}

			/// <summary>Create local/log directories</summary>
			/// <param name="dirType">type of directories i.e. local dirs or log dirs</param>
			/// <param name="numDirs">number of directories</param>
			/// <returns>the created directories as a comma delimited String</returns>
			private string PrepareDirs(string dirType, int numDirs)
			{
				FilePath[] dirs = new FilePath[numDirs];
				string dirsString = string.Empty;
				for (int i = 0; i < numDirs; i++)
				{
					dirs[i] = new FilePath(this._enclosing.testWorkDir, this._enclosing.GetName() + "-"
						 + dirType + "Dir-nm-" + this.index + "_" + i);
					dirs[i].Mkdirs();
					MiniYARNCluster.Log.Info("Created " + dirType + "Dir in " + dirs[i].GetAbsolutePath
						());
					string delimiter = (i > 0) ? "," : string.Empty;
					dirsString = dirsString.Concat(delimiter + dirs[i].GetAbsolutePath());
				}
				return dirsString;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				lock (this)
				{
					try
					{
						new _Thread_559(this).Start();
						int waitCount = 0;
						while (this._enclosing.nodeManagers[this.index].GetServiceState() == Service.STATE
							.Inited && waitCount++ < 60)
						{
							MiniYARNCluster.Log.Info("Waiting for NM " + this.index + " to start...");
							Sharpen.Thread.Sleep(1000);
						}
						if (this._enclosing.nodeManagers[this.index].GetServiceState() != Service.STATE.Started)
						{
							// RM could have failed.
							throw new IOException("NodeManager " + this.index + " failed to start");
						}
						base.ServiceStart();
					}
					catch (Exception t)
					{
						throw new YarnRuntimeException(t);
					}
				}
			}

			private sealed class _Thread_559 : Sharpen.Thread
			{
				public _Thread_559(NodeManagerWrapper _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					this._enclosing._enclosing.nodeManagers[this._enclosing.index].Start();
				}

				private readonly NodeManagerWrapper _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				lock (this)
				{
					if (this._enclosing.nodeManagers[this.index] != null)
					{
						this._enclosing.nodeManagers[this.index].Stop();
					}
					base.ServiceStop();
				}
			}

			private readonly MiniYARNCluster _enclosing;
		}

		private class CustomNodeManager : NodeManager
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin()
			{
			}

			internal CustomNodeManager(MiniYARNCluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MiniYARNCluster _enclosing;
			// Don't try to login using keytab in the testcase.
		}

		private class ShortCircuitedNodeManager : MiniYARNCluster.CustomNodeManager
		{
			protected override NodeStatusUpdater CreateNodeStatusUpdater(Context context, Dispatcher
				 dispatcher, NodeHealthCheckerService healthChecker)
			{
				return new _NodeStatusUpdaterImpl_601(this, context, dispatcher, healthChecker, this
					.metrics);
			}

			private sealed class _NodeStatusUpdaterImpl_601 : NodeStatusUpdaterImpl
			{
				public _NodeStatusUpdaterImpl_601(ShortCircuitedNodeManager _enclosing, Context baseArg1
					, Dispatcher baseArg2, NodeHealthCheckerService baseArg3, NodeManagerMetrics baseArg4
					)
					: base(baseArg1, baseArg2, baseArg3, baseArg4)
				{
					this._enclosing = _enclosing;
				}

				protected override ResourceTracker GetRMClient()
				{
					ResourceTrackerService rt = this._enclosing._enclosing.GetResourceManager().GetResourceTrackerService
						();
					RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
					// For in-process communication without RPC
					return new _ResourceTracker_610(rt);
				}

				private sealed class _ResourceTracker_610 : ResourceTracker
				{
					public _ResourceTracker_610(ResourceTrackerService rt)
					{
						this.rt = rt;
					}

					/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
					/// <exception cref="System.IO.IOException"/>
					public NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
					{
						NodeHeartbeatResponse response;
						try
						{
							response = rt.NodeHeartbeat(request);
						}
						catch (YarnException e)
						{
							MiniYARNCluster.Log.Info("Exception in heartbeat from node " + request.GetNodeStatus
								().GetNodeId(), e);
							throw;
						}
						return response;
					}

					/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
					/// <exception cref="System.IO.IOException"/>
					public RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
						 request)
					{
						RegisterNodeManagerResponse response;
						try
						{
							response = rt.RegisterNodeManager(request);
						}
						catch (YarnException e)
						{
							MiniYARNCluster.Log.Info("Exception in node registration from " + request.GetNodeId
								().ToString(), e);
							throw;
						}
						return response;
					}

					private readonly ResourceTrackerService rt;
				}

				protected override void StopRMProxy()
				{
				}

				private readonly ShortCircuitedNodeManager _enclosing;
			}

			internal ShortCircuitedNodeManager(MiniYARNCluster _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MiniYARNCluster _enclosing;
		}

		/// <summary>Wait for all the NodeManagers to connect to the ResourceManager.</summary>
		/// <param name="timeout">Time to wait (sleeps in 100 ms intervals) in milliseconds.</param>
		/// <returns>
		/// true if all NodeManagers connect to the (Active)
		/// ResourceManager, false otherwise.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool WaitForNodeManagersToConnect(long timeout)
		{
			GetClusterMetricsRequest req = GetClusterMetricsRequest.NewInstance();
			for (int i = 0; i < timeout / 100; i++)
			{
				ResourceManager rm = GetResourceManager();
				if (rm == null)
				{
					throw new YarnException("Can not find the active RM.");
				}
				else
				{
					if (nodeManagers.Length == rm.GetClientRMService().GetClusterMetrics(req).GetClusterMetrics
						().GetNumNodeManagers())
					{
						return true;
					}
				}
				Sharpen.Thread.Sleep(100);
			}
			return false;
		}

		private class ApplicationHistoryServerWrapper : AbstractService
		{
			public ApplicationHistoryServerWrapper(MiniYARNCluster _enclosing)
				: base(typeof(MiniYARNCluster.ApplicationHistoryServerWrapper).FullName)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				lock (this)
				{
					this._enclosing.appHistoryServer = new ApplicationHistoryServer();
					conf.SetClass(YarnConfiguration.ApplicationHistoryStore, typeof(MemoryApplicationHistoryStore
						), typeof(ApplicationHistoryStore));
					conf.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore)
						, typeof(TimelineStore));
					conf.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(MemoryTimelineStateStore
						), typeof(TimelineStateStore));
					if (!this._enclosing.useFixedPorts)
					{
						string hostname = MiniYARNCluster.GetHostname();
						conf.Set(YarnConfiguration.TimelineServiceAddress, hostname + ":0");
						conf.Set(YarnConfiguration.TimelineServiceWebappAddress, hostname + ":0");
					}
					this._enclosing.appHistoryServer.Init(conf);
					base.ServiceInit(conf);
				}
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				lock (this)
				{
					try
					{
						new _Thread_704(this).Start();
						int waitCount = 0;
						while (this._enclosing.appHistoryServer.GetServiceState() == Service.STATE.Inited
							 && waitCount++ < 60)
						{
							MiniYARNCluster.Log.Info("Waiting for Timeline Server to start...");
							Sharpen.Thread.Sleep(1500);
						}
						if (this._enclosing.appHistoryServer.GetServiceState() != Service.STATE.Started)
						{
							// AHS could have failed.
							throw new IOException("ApplicationHistoryServer failed to start. Final state is "
								 + this._enclosing.appHistoryServer.GetServiceState());
						}
						base.ServiceStart();
					}
					catch (Exception t)
					{
						throw new YarnRuntimeException(t);
					}
					MiniYARNCluster.Log.Info("MiniYARN ApplicationHistoryServer address: " + this.GetConfig
						().Get(YarnConfiguration.TimelineServiceAddress));
					MiniYARNCluster.Log.Info("MiniYARN ApplicationHistoryServer web address: " + this
						.GetConfig().Get(YarnConfiguration.TimelineServiceWebappAddress));
				}
			}

			private sealed class _Thread_704 : Sharpen.Thread
			{
				public _Thread_704(ApplicationHistoryServerWrapper _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					this._enclosing._enclosing.appHistoryServer.Start();
				}

				private readonly ApplicationHistoryServerWrapper _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				lock (this)
				{
					if (this._enclosing.appHistoryServer != null)
					{
						this._enclosing.appHistoryServer.Stop();
					}
				}
			}

			private readonly MiniYARNCluster _enclosing;
		}

		public virtual ApplicationHistoryServer GetApplicationHistoryServer()
		{
			return this.appHistoryServer;
		}

		protected internal virtual ResourceManager CreateResourceManager()
		{
			return new _ResourceManager_744();
		}

		private sealed class _ResourceManager_744 : ResourceManager
		{
			public _ResourceManager_744()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin()
			{
			}
		}

		// Don't try to login using keytab in the testcases.
		public virtual int GetNumOfResourceManager()
		{
			return this.resourceManagers.Length;
		}
	}
}
