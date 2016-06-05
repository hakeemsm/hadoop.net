using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;


namespace Org.Apache.Hadoop.HA
{
	public abstract class ZKFailoverController
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.ZKFailoverController
			));

		public const string ZkQuorumKey = "ha.zookeeper.quorum";

		private const string ZkSessionTimeoutKey = "ha.zookeeper.session-timeout.ms";

		private const int ZkSessionTimeoutDefault = 5 * 1000;

		private const string ZkParentZnodeKey = "ha.zookeeper.parent-znode";

		public const string ZkAclKey = "ha.zookeeper.acl";

		private const string ZkAclDefault = "world:anyone:rwcda";

		public const string ZkAuthKey = "ha.zookeeper.auth";

		internal const string ZkParentZnodeDefault = "/hadoop-ha";

		/// <summary>All of the conf keys used by the ZKFC.</summary>
		/// <remarks>
		/// All of the conf keys used by the ZKFC. This is used in order to allow
		/// them to be overridden on a per-nameservice or per-namenode basis.
		/// </remarks>
		protected internal static readonly string[] ZkfcConfKeys = new string[] { ZkQuorumKey
			, ZkSessionTimeoutKey, ZkParentZnodeKey, ZkAclKey, ZkAuthKey };

		protected internal const string Usage = "Usage: java zkfc [ -formatZK [-force] [-nonInteractive] ]";

		/// <summary>Unable to format the parent znode in ZK</summary>
		internal const int ErrCodeFormatDenied = 2;

		/// <summary>The parent znode doesn't exist in ZK</summary>
		internal const int ErrCodeNoParentZnode = 3;

		/// <summary>Fencing is not properly configured</summary>
		internal const int ErrCodeNoFencer = 4;

		/// <summary>Automatic failover is not enabled</summary>
		internal const int ErrCodeAutoFailoverNotEnabled = 5;

		/// <summary>Cannot connect to ZooKeeper</summary>
		internal const int ErrCodeNoZk = 6;

		protected internal Configuration conf;

		private string zkQuorum;

		protected internal readonly HAServiceTarget localTarget;

		private HealthMonitor healthMonitor;

		private ActiveStandbyElector elector;

		protected internal ZKFCRpcServer rpcServer;

		private HealthMonitor.State lastHealthState = HealthMonitor.State.Initializing;

		private volatile HAServiceProtocol.HAServiceState serviceState = HAServiceProtocol.HAServiceState
			.Initializing;

		/// <summary>Set if a fatal error occurs</summary>
		private string fatalError = null;

		/// <summary>A future nanotime before which the ZKFC will not join the election.</summary>
		/// <remarks>
		/// A future nanotime before which the ZKFC will not join the election.
		/// This is used during graceful failover.
		/// </remarks>
		private long delayJoiningUntilNanotime = 0;

		/// <summary>
		/// Executor on which
		/// <see cref="ScheduleRecheck(long)"/>
		/// schedules events
		/// </summary>
		private ScheduledExecutorService delayExecutor = Executors.NewScheduledThreadPool
			(1, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("ZKFC Delay timer #%d"
			).Build());

		private ZKFailoverController.ActiveAttemptRecord lastActiveAttemptRecord;

		private object activeAttemptRecordLock = new object();

		protected internal ZKFailoverController(Configuration conf, HAServiceTarget localTarget
			)
		{
			this.localTarget = localTarget;
			this.conf = conf;
		}

		protected internal abstract byte[] TargetToData(HAServiceTarget target);

		protected internal abstract HAServiceTarget DataToTarget(byte[] data);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void LoginAsFCUser();

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void CheckRpcAdminAccess();

		protected internal abstract IPEndPoint GetRpcAddressToBindTo();

		protected internal abstract PolicyProvider GetPolicyProvider();

		/// <summary>
		/// Return the name of a znode inside the configured parent znode in which
		/// the ZKFC will do all of its work.
		/// </summary>
		/// <remarks>
		/// Return the name of a znode inside the configured parent znode in which
		/// the ZKFC will do all of its work. This is so that multiple federated
		/// nameservices can run on the same ZK quorum without having to manually
		/// configure them to separate subdirectories.
		/// </remarks>
		protected internal abstract string GetScopeInsideParentNode();

		public virtual HAServiceTarget GetLocalTarget()
		{
			return localTarget;
		}

		internal virtual HAServiceProtocol.HAServiceState GetServiceState()
		{
			return serviceState;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (!localTarget.IsAutoFailoverEnabled())
			{
				Log.Fatal("Automatic failover is not enabled for " + localTarget + "." + " Please ensure that automatic failover is enabled in the "
					 + "configuration before running the ZK failover controller.");
				return ErrCodeAutoFailoverNotEnabled;
			}
			LoginAsFCUser();
			try
			{
				return SecurityUtil.DoAsLoginUserOrFatal(new _PrivilegedAction_168(this, args));
			}
			catch (RuntimeException rte)
			{
				throw (Exception)rte.InnerException;
			}
		}

		private sealed class _PrivilegedAction_168 : PrivilegedAction<int>
		{
			public _PrivilegedAction_168(ZKFailoverController _enclosing, string[] args)
			{
				this._enclosing = _enclosing;
				this.args = args;
			}

			public int Run()
			{
				try
				{
					return this._enclosing.DoRun(args);
				}
				catch (Exception t)
				{
					throw new RuntimeException(t);
				}
				finally
				{
					if (this._enclosing.elector != null)
					{
						this._enclosing.elector.TerminateConnection();
					}
				}
			}

			private readonly ZKFailoverController _enclosing;

			private readonly string[] args;
		}

		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private int DoRun(string[] args)
		{
			try
			{
				InitZK();
			}
			catch (KeeperException)
			{
				Log.Fatal("Unable to start failover controller. Unable to connect " + "to ZooKeeper quorum at "
					 + zkQuorum + ". Please check the " + "configured value for " + ZkQuorumKey + " and ensure that "
					 + "ZooKeeper is running.");
				return ErrCodeNoZk;
			}
			if (args.Length > 0)
			{
				if ("-formatZK".Equals(args[0]))
				{
					bool force = false;
					bool interactive = true;
					for (int i = 1; i < args.Length; i++)
					{
						if ("-force".Equals(args[i]))
						{
							force = true;
						}
						else
						{
							if ("-nonInteractive".Equals(args[i]))
							{
								interactive = false;
							}
							else
							{
								BadArg(args[i]);
							}
						}
					}
					return FormatZK(force, interactive);
				}
				else
				{
					BadArg(args[0]);
				}
			}
			if (!elector.ParentZNodeExists())
			{
				Log.Fatal("Unable to start failover controller. " + "Parent znode does not exist.\n"
					 + "Run with -formatZK flag to initialize ZooKeeper.");
				return ErrCodeNoParentZnode;
			}
			try
			{
				localTarget.CheckFencingConfigured();
			}
			catch (BadFencingConfigurationException e)
			{
				Log.Fatal("Fencing is not configured for " + localTarget + ".\n" + "You must configure a fencing method before using automatic "
					 + "failover.", e);
				return ErrCodeNoFencer;
			}
			InitRPC();
			InitHM();
			StartRPC();
			try
			{
				MainLoop();
			}
			finally
			{
				rpcServer.StopAndJoin();
				elector.QuitElection(true);
				healthMonitor.Shutdown();
				healthMonitor.Join();
			}
			return 0;
		}

		private void BadArg(string arg)
		{
			PrintUsage();
			throw new HadoopIllegalArgumentException("Bad argument: " + arg);
		}

		private void PrintUsage()
		{
			System.Console.Error.WriteLine(Usage + "\n");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private int FormatZK(bool force, bool interactive)
		{
			if (elector.ParentZNodeExists())
			{
				if (!force && (!interactive || !ConfirmFormat()))
				{
					return ErrCodeFormatDenied;
				}
				try
				{
					elector.ClearParentZNode();
				}
				catch (IOException e)
				{
					Log.Error("Unable to clear zk parent znode", e);
					return 1;
				}
			}
			elector.EnsureParentZNode();
			return 0;
		}

		private bool ConfirmFormat()
		{
			string parentZnode = GetParentZnode();
			System.Console.Error.WriteLine("===============================================\n"
				 + "The configured parent znode " + parentZnode + " already exists.\n" + "Are you sure you want to clear all failover information from\n"
				 + "ZooKeeper?\n" + "WARNING: Before proceeding, ensure that all HDFS services and\n"
				 + "failover controllers are stopped!\n" + "==============================================="
				);
			try
			{
				return ToolRunner.ConfirmPrompt("Proceed formatting " + parentZnode + "?");
			}
			catch (IOException e)
			{
				Log.Debug("Failed to confirm", e);
				return false;
			}
		}

		// ------------------------------------------
		// Begin actual guts of failover controller
		// ------------------------------------------
		private void InitHM()
		{
			healthMonitor = new HealthMonitor(conf, localTarget);
			healthMonitor.AddCallback(new ZKFailoverController.HealthCallbacks(this));
			healthMonitor.AddServiceStateCallback(new ZKFailoverController.ServiceStateCallBacks
				(this));
			healthMonitor.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void InitRPC()
		{
			IPEndPoint bindAddr = GetRpcAddressToBindTo();
			rpcServer = new ZKFCRpcServer(conf, bindAddr, this, GetPolicyProvider());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StartRPC()
		{
			rpcServer.Start();
		}

		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private void InitZK()
		{
			zkQuorum = conf.Get(ZkQuorumKey);
			int zkTimeout = conf.GetInt(ZkSessionTimeoutKey, ZkSessionTimeoutDefault);
			// Parse ACLs from configuration.
			string zkAclConf = conf.Get(ZkAclKey, ZkAclDefault);
			zkAclConf = ZKUtil.ResolveConfIndirection(zkAclConf);
			IList<ACL> zkAcls = ZKUtil.ParseACLs(zkAclConf);
			if (zkAcls.IsEmpty())
			{
				zkAcls = ZooDefs.Ids.CreatorAllAcl;
			}
			// Parse authentication from configuration.
			string zkAuthConf = conf.Get(ZkAuthKey);
			zkAuthConf = ZKUtil.ResolveConfIndirection(zkAuthConf);
			IList<ZKUtil.ZKAuthInfo> zkAuths;
			if (zkAuthConf != null)
			{
				zkAuths = ZKUtil.ParseAuth(zkAuthConf);
			}
			else
			{
				zkAuths = Collections.EmptyList();
			}
			// Sanity check configuration.
			Preconditions.CheckArgument(zkQuorum != null, "Missing required configuration '%s' for ZooKeeper quorum"
				, ZkQuorumKey);
			Preconditions.CheckArgument(zkTimeout > 0, "Invalid ZK session timeout %s", zkTimeout
				);
			int maxRetryNum = conf.GetInt(CommonConfigurationKeys.HaFcElectorZkOpRetriesKey, 
				CommonConfigurationKeys.HaFcElectorZkOpRetriesDefault);
			elector = new ActiveStandbyElector(zkQuorum, zkTimeout, GetParentZnode(), zkAcls, 
				zkAuths, new ZKFailoverController.ElectorCallbacks(this), maxRetryNum);
		}

		private string GetParentZnode()
		{
			string znode = conf.Get(ZkParentZnodeKey, ZkParentZnodeDefault);
			if (!znode.EndsWith("/"))
			{
				znode += "/";
			}
			return znode + GetScopeInsideParentNode();
		}

		/// <exception cref="System.Exception"/>
		private void MainLoop()
		{
			lock (this)
			{
				while (fatalError == null)
				{
					Runtime.Wait(this);
				}
				System.Diagnostics.Debug.Assert(fatalError != null);
				// only get here on fatal
				throw new RuntimeException("ZK Failover Controller failed: " + fatalError);
			}
		}

		private void FatalError(string err)
		{
			lock (this)
			{
				Log.Fatal("Fatal error occurred:" + err);
				fatalError = err;
				Runtime.NotifyAll(this);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private void BecomeActive()
		{
			lock (this)
			{
				Log.Info("Trying to make " + localTarget + " active...");
				try
				{
					HAServiceProtocolHelper.TransitionToActive(localTarget.GetProxy(conf, FailoverController
						.GetRpcTimeoutToNewActive(conf)), CreateReqInfo());
					string msg = "Successfully transitioned " + localTarget + " to active state";
					Log.Info(msg);
					serviceState = HAServiceProtocol.HAServiceState.Active;
					RecordActiveAttempt(new ZKFailoverController.ActiveAttemptRecord(true, msg));
				}
				catch (Exception t)
				{
					string msg = "Couldn't make " + localTarget + " active";
					Log.Fatal(msg, t);
					RecordActiveAttempt(new ZKFailoverController.ActiveAttemptRecord(false, msg + "\n"
						 + StringUtils.StringifyException(t)));
					if (t is ServiceFailedException)
					{
						throw (ServiceFailedException)t;
					}
					else
					{
						throw new ServiceFailedException("Couldn't transition to active", t);
					}
				}
			}
		}

		/*
		* TODO:
		* we need to make sure that if we get fenced and then quickly restarted,
		* none of these calls will retry across the restart boundary
		* perhaps the solution is that, whenever the nn starts, it gets a unique
		* ID, and when we start becoming active, we record it, and then any future
		* calls use the same ID
		*/
		/// <summary>Store the results of the last attempt to become active.</summary>
		/// <remarks>
		/// Store the results of the last attempt to become active.
		/// This is used so that, during manually initiated failover,
		/// we can report back the results of the attempt to become active
		/// to the initiator of the failover.
		/// </remarks>
		private void RecordActiveAttempt(ZKFailoverController.ActiveAttemptRecord record)
		{
			lock (activeAttemptRecordLock)
			{
				lastActiveAttemptRecord = record;
				Runtime.NotifyAll(activeAttemptRecordLock);
			}
		}

		/// <summary>
		/// Wait until one of the following events:
		/// <ul>
		/// <li>Another thread publishes the results of an attempt to become active
		/// using
		/// <see cref="RecordActiveAttempt(ActiveAttemptRecord)"/>
		/// </li>
		/// <li>The node enters bad health status</li>
		/// <li>The specified timeout elapses</li>
		/// </ul>
		/// </summary>
		/// <param name="timeoutMillis">number of millis to wait</param>
		/// <returns>
		/// the published record, or null if the timeout elapses or the
		/// service becomes unhealthy
		/// </returns>
		/// <exception cref="System.Exception">if the thread is interrupted.</exception>
		private ZKFailoverController.ActiveAttemptRecord WaitForActiveAttempt(int timeoutMillis
			)
		{
			long st = Runtime.NanoTime();
			long waitUntil = st + TimeUnit.Nanoseconds.Convert(timeoutMillis, TimeUnit.Milliseconds
				);
			do
			{
				// periodically check health state, because entering an
				// unhealthy state could prevent us from ever attempting to
				// become active. We can detect this and respond to the user
				// immediately.
				lock (this)
				{
					if (lastHealthState != HealthMonitor.State.ServiceHealthy)
					{
						// early out if service became unhealthy
						return null;
					}
				}
				lock (activeAttemptRecordLock)
				{
					if ((lastActiveAttemptRecord != null && lastActiveAttemptRecord.nanoTime >= st))
					{
						return lastActiveAttemptRecord;
					}
					// Only wait 1sec so that we periodically recheck the health state
					// above.
					Runtime.Wait(activeAttemptRecordLock, 1000);
				}
			}
			while (Runtime.NanoTime() < waitUntil);
			// Timeout elapsed.
			Log.Warn(timeoutMillis + "ms timeout elapsed waiting for an attempt " + "to become active"
				);
			return null;
		}

		private HAServiceProtocol.StateChangeRequestInfo CreateReqInfo()
		{
			return new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource
				.RequestByZkfc);
		}

		private void BecomeStandby()
		{
			lock (this)
			{
				Log.Info("ZK Election indicated that " + localTarget + " should become standby");
				try
				{
					int timeout = FailoverController.GetGracefulFenceTimeout(conf);
					localTarget.GetProxy(conf, timeout).TransitionToStandby(CreateReqInfo());
					Log.Info("Successfully transitioned " + localTarget + " to standby state");
				}
				catch (Exception e)
				{
					Log.Error("Couldn't transition " + localTarget + " to standby state", e);
				}
				// TODO handle this. It's a likely case since we probably got fenced
				// at the same time.
				serviceState = HAServiceProtocol.HAServiceState.Standby;
			}
		}

		private void FenceOldActive(byte[] data)
		{
			lock (this)
			{
				HAServiceTarget target = DataToTarget(data);
				try
				{
					DoFence(target);
				}
				catch (Exception t)
				{
					RecordActiveAttempt(new ZKFailoverController.ActiveAttemptRecord(false, "Unable to fence old active: "
						 + StringUtils.StringifyException(t)));
					Throwables.Propagate(t);
				}
			}
		}

		private void DoFence(HAServiceTarget target)
		{
			Log.Info("Should fence: " + target);
			bool gracefulWorked = new FailoverController(conf, HAServiceProtocol.RequestSource
				.RequestByZkfc).TryGracefulFence(target);
			if (gracefulWorked)
			{
				// It's possible that it's in standby but just about to go into active,
				// no? Is there some race here?
				Log.Info("Successfully transitioned " + target + " to standby " + "state without fencing"
					);
				return;
			}
			try
			{
				target.CheckFencingConfigured();
			}
			catch (BadFencingConfigurationException e)
			{
				Log.Error("Couldn't fence old active " + target, e);
				RecordActiveAttempt(new ZKFailoverController.ActiveAttemptRecord(false, "Unable to fence old active"
					));
				throw new RuntimeException(e);
			}
			if (!target.GetFencer().Fence(target))
			{
				throw new RuntimeException("Unable to fence " + target);
			}
		}

		/// <summary>Request from graceful failover to cede active role.</summary>
		/// <remarks>
		/// Request from graceful failover to cede active role. Causes
		/// this ZKFC to transition its local node to standby, then quit
		/// the election for the specified period of time, after which it
		/// will rejoin iff it is healthy.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CedeActive(int millisToCede)
		{
			try
			{
				UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_547(this, 
					millisToCede));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_547 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_547(ZKFailoverController _enclosing, int millisToCede
				)
			{
				this._enclosing = _enclosing;
				this.millisToCede = millisToCede;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.DoCedeActive(millisToCede);
				return null;
			}

			private readonly ZKFailoverController _enclosing;

			private readonly int millisToCede;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		private void DoCedeActive(int millisToCede)
		{
			int timeout = FailoverController.GetGracefulFenceTimeout(conf);
			// Lock elector to maintain lock ordering of elector -> ZKFC
			lock (elector)
			{
				lock (this)
				{
					if (millisToCede <= 0)
					{
						delayJoiningUntilNanotime = 0;
						RecheckElectability();
						return;
					}
					Log.Info("Requested by " + UserGroupInformation.GetCurrentUser() + " at " + Server
						.GetRemoteAddress() + " to cede active role.");
					bool needFence = false;
					try
					{
						localTarget.GetProxy(conf, timeout).TransitionToStandby(CreateReqInfo());
						Log.Info("Successfully ensured local node is in standby mode");
					}
					catch (IOException ioe)
					{
						Log.Warn("Unable to transition local node to standby: " + ioe.GetLocalizedMessage
							());
						Log.Warn("Quitting election but indicating that fencing is " + "necessary");
						needFence = true;
					}
					delayJoiningUntilNanotime = Runtime.NanoTime() + TimeUnit.Milliseconds.ToNanos(millisToCede
						);
					elector.QuitElection(needFence);
					serviceState = HAServiceProtocol.HAServiceState.Initializing;
				}
			}
			RecheckElectability();
		}

		/// <summary>Coordinate a graceful failover to this node.</summary>
		/// <exception cref="ServiceFailedException">if the node fails to become active</exception>
		/// <exception cref="System.IO.IOException">some other error occurs</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		internal virtual void GracefulFailoverToYou()
		{
			try
			{
				UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_601(this)
					);
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_601 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_601(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.DoGracefulFailover();
				return null;
			}

			private readonly ZKFailoverController _enclosing;
		}

		/// <summary>Coordinate a graceful failover.</summary>
		/// <remarks>
		/// Coordinate a graceful failover. This proceeds in several phases:
		/// 1) Pre-flight checks: ensure that the local node is healthy, and
		/// thus a candidate for failover.
		/// 2) Determine the current active node. If it is the local node, no
		/// need to failover - return success.
		/// 3) Ask that node to yield from the election for a number of seconds.
		/// 4) Allow the normal election path to run in other threads. Wait until
		/// we either become unhealthy or we see an election attempt recorded by
		/// the normal code path.
		/// 5) Allow the old active to rejoin the election, so a future
		/// failback is possible.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void DoGracefulFailover()
		{
			int timeout = FailoverController.GetGracefulFenceTimeout(conf) * 2;
			// Phase 1: pre-flight checks
			CheckEligibleForFailover();
			// Phase 2: determine old/current active node. Check that we're not
			// ourselves active, etc.
			HAServiceTarget oldActive = GetCurrentActive();
			if (oldActive == null)
			{
				// No node is currently active. So, if we aren't already
				// active ourselves by means of a normal election, then there's
				// probably something preventing us from becoming active.
				throw new ServiceFailedException("No other node is currently active.");
			}
			if (oldActive.GetAddress().Equals(localTarget.GetAddress()))
			{
				Log.Info("Local node " + localTarget + " is already active. " + "No need to failover. Returning success."
					);
				return;
			}
			// Phase 3: ask the old active to yield from the election.
			Log.Info("Asking " + oldActive + " to cede its active state for " + timeout + "ms"
				);
			ZKFCProtocol oldZkfc = oldActive.GetZKFCProxy(conf, timeout);
			oldZkfc.CedeActive(timeout);
			// Phase 4: wait for the normal election to make the local node
			// active.
			ZKFailoverController.ActiveAttemptRecord attempt = WaitForActiveAttempt(timeout +
				 60000);
			if (attempt == null)
			{
				// We didn't even make an attempt to become active.
				lock (this)
				{
					if (lastHealthState != HealthMonitor.State.ServiceHealthy)
					{
						throw new ServiceFailedException("Unable to become active. " + "Service became unhealthy while trying to failover."
							);
					}
				}
				throw new ServiceFailedException("Unable to become active. " + "Local node did not get an opportunity to do so from ZooKeeper, "
					 + "or the local node took too long to transition to active.");
			}
			// Phase 5. At this point, we made some attempt to become active. So we
			// can tell the old active to rejoin if it wants. This allows a quick
			// fail-back if we immediately crash.
			oldZkfc.CedeActive(-1);
			if (attempt.succeeded)
			{
				Log.Info("Successfully became active. " + attempt.status);
			}
			else
			{
				// Propagate failure
				string msg = "Failed to become active. " + attempt.status;
				throw new ServiceFailedException(msg);
			}
		}

		/// <summary>
		/// Ensure that the local node is in a healthy state, and thus
		/// eligible for graceful failover.
		/// </summary>
		/// <exception cref="ServiceFailedException">if the node is unhealthy</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		private void CheckEligibleForFailover()
		{
			lock (this)
			{
				// Check health
				if (this.GetLastHealthState() != HealthMonitor.State.ServiceHealthy)
				{
					throw new ServiceFailedException(localTarget + " is not currently healthy. " + "Cannot be failover target"
						);
				}
			}
		}

		/// <returns>
		/// an
		/// <see cref="HAServiceTarget"/>
		/// for the current active node
		/// in the cluster, or null if no node is active.
		/// </returns>
		/// <exception cref="System.IO.IOException">if a ZK-related issue occurs</exception>
		/// <exception cref="System.Exception">if thread is interrupted</exception>
		private HAServiceTarget GetCurrentActive()
		{
			lock (elector)
			{
				lock (this)
				{
					byte[] activeData;
					try
					{
						activeData = elector.GetActiveData();
					}
					catch (ActiveStandbyElector.ActiveNotFoundException)
					{
						return null;
					}
					catch (KeeperException ke)
					{
						throw new IOException("Unexpected ZooKeeper issue fetching active node info", ke);
					}
					HAServiceTarget oldActive = DataToTarget(activeData);
					return oldActive;
				}
			}
		}

		/// <summary>
		/// Check the current state of the service, and join the election
		/// if it should be in the election.
		/// </summary>
		private void RecheckElectability()
		{
			// Maintain lock ordering of elector -> ZKFC
			lock (elector)
			{
				lock (this)
				{
					bool healthy = lastHealthState == HealthMonitor.State.ServiceHealthy;
					long remainingDelay = delayJoiningUntilNanotime - Runtime.NanoTime();
					if (remainingDelay > 0)
					{
						if (healthy)
						{
							Log.Info("Would have joined master election, but this node is " + "prohibited from doing so for "
								 + TimeUnit.Nanoseconds.ToMillis(remainingDelay) + " more ms");
						}
						ScheduleRecheck(remainingDelay);
						return;
					}
					switch (lastHealthState)
					{
						case HealthMonitor.State.ServiceHealthy:
						{
							elector.JoinElection(TargetToData(localTarget));
							if (quitElectionOnBadState)
							{
								quitElectionOnBadState = false;
							}
							break;
						}

						case HealthMonitor.State.Initializing:
						{
							Log.Info("Ensuring that " + localTarget + " does not " + "participate in active master election"
								);
							elector.QuitElection(false);
							serviceState = HAServiceProtocol.HAServiceState.Initializing;
							break;
						}

						case HealthMonitor.State.ServiceUnhealthy:
						case HealthMonitor.State.ServiceNotResponding:
						{
							Log.Info("Quitting master election for " + localTarget + " and marking that fencing is necessary"
								);
							elector.QuitElection(true);
							serviceState = HAServiceProtocol.HAServiceState.Initializing;
							break;
						}

						case HealthMonitor.State.HealthMonitorFailed:
						{
							FatalError("Health monitor failed!");
							break;
						}

						default:
						{
							throw new ArgumentException("Unhandled state:" + lastHealthState);
						}
					}
				}
			}
		}

		/// <summary>
		/// Schedule a call to
		/// <see cref="RecheckElectability()"/>
		/// in the future.
		/// </summary>
		private void ScheduleRecheck(long whenNanos)
		{
			delayExecutor.Schedule(new _Runnable_790(this), whenNanos, TimeUnit.Nanoseconds);
		}

		private sealed class _Runnable_790 : Runnable
		{
			public _Runnable_790(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				try
				{
					this._enclosing.RecheckElectability();
				}
				catch (Exception t)
				{
					this._enclosing.FatalError("Failed to recheck electability: " + StringUtils.StringifyException
						(t));
				}
			}

			private readonly ZKFailoverController _enclosing;
		}

		internal int serviceStateMismatchCount = 0;

		internal bool quitElectionOnBadState = false;

		internal virtual void VerifyChangedServiceState(HAServiceProtocol.HAServiceState 
			changedState)
		{
			lock (elector)
			{
				lock (this)
				{
					if (serviceState == HAServiceProtocol.HAServiceState.Initializing)
					{
						if (quitElectionOnBadState)
						{
							Log.Debug("rechecking for electability from bad state");
							RecheckElectability();
						}
						return;
					}
					if (changedState == serviceState)
					{
						serviceStateMismatchCount = 0;
						return;
					}
					if (serviceStateMismatchCount == 0)
					{
						// recheck one more time. As this might be due to parallel transition.
						serviceStateMismatchCount++;
						return;
					}
					// quit the election as the expected state and reported state
					// mismatches.
					Log.Error("Local service " + localTarget + " has changed the serviceState to " + 
						changedState + ". Expected was " + serviceState + ". Quitting election marking fencing necessary."
						);
					delayJoiningUntilNanotime = Runtime.NanoTime() + TimeUnit.Milliseconds.ToNanos(1000
						);
					elector.QuitElection(true);
					quitElectionOnBadState = true;
					serviceStateMismatchCount = 0;
					serviceState = HAServiceProtocol.HAServiceState.Initializing;
				}
			}
		}

		/// <returns>
		/// the last health state passed to the FC
		/// by the HealthMonitor.
		/// </returns>
		[VisibleForTesting]
		internal virtual HealthMonitor.State GetLastHealthState()
		{
			lock (this)
			{
				return lastHealthState;
			}
		}

		private void SetLastHealthState(HealthMonitor.State newState)
		{
			lock (this)
			{
				Log.Info("Local service " + localTarget + " entered state: " + newState);
				lastHealthState = newState;
			}
		}

		[VisibleForTesting]
		internal virtual ActiveStandbyElector GetElectorForTests()
		{
			return elector;
		}

		[VisibleForTesting]
		internal virtual ZKFCRpcServer GetRpcServerForTests()
		{
			return rpcServer;
		}

		/// <summary>Callbacks from elector</summary>
		internal class ElectorCallbacks : ActiveStandbyElector.ActiveStandbyElectorCallback
		{
			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			public virtual void BecomeActive()
			{
				this._enclosing.BecomeActive();
			}

			public virtual void BecomeStandby()
			{
				this._enclosing.BecomeStandby();
			}

			public virtual void EnterNeutralMode()
			{
			}

			public virtual void NotifyFatalError(string errorMessage)
			{
				this._enclosing.FatalError(errorMessage);
			}

			public virtual void FenceOldActive(byte[] data)
			{
				this._enclosing.FenceOldActive(data);
			}

			public override string ToString()
			{
				lock (this._enclosing)
				{
					return "Elector callbacks for " + this._enclosing.localTarget;
				}
			}

			internal ElectorCallbacks(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKFailoverController _enclosing;
		}

		/// <summary>Callbacks from HealthMonitor</summary>
		internal class HealthCallbacks : HealthMonitor.Callback
		{
			public virtual void EnteredState(HealthMonitor.State newState)
			{
				this._enclosing.SetLastHealthState(newState);
				this._enclosing.RecheckElectability();
			}

			internal HealthCallbacks(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKFailoverController _enclosing;
		}

		/// <summary>Callbacks for HAServiceStatus</summary>
		internal class ServiceStateCallBacks : HealthMonitor.ServiceStateCallback
		{
			public virtual void ReportServiceStatus(HAServiceStatus status)
			{
				this._enclosing.VerifyChangedServiceState(status.GetState());
			}

			internal ServiceStateCallBacks(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKFailoverController _enclosing;
		}

		private class ActiveAttemptRecord
		{
			private readonly bool succeeded;

			private readonly string status;

			private readonly long nanoTime;

			public ActiveAttemptRecord(bool succeeded, string status)
			{
				this.succeeded = succeeded;
				this.status = status;
				this.nanoTime = Runtime.NanoTime();
			}
		}
	}
}
