using Sharpen;

namespace org.apache.hadoop.ha
{
	public abstract class ZKFailoverController
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ZKFailoverController
			)));

		public const string ZK_QUORUM_KEY = "ha.zookeeper.quorum";

		private const string ZK_SESSION_TIMEOUT_KEY = "ha.zookeeper.session-timeout.ms";

		private const int ZK_SESSION_TIMEOUT_DEFAULT = 5 * 1000;

		private const string ZK_PARENT_ZNODE_KEY = "ha.zookeeper.parent-znode";

		public const string ZK_ACL_KEY = "ha.zookeeper.acl";

		private const string ZK_ACL_DEFAULT = "world:anyone:rwcda";

		public const string ZK_AUTH_KEY = "ha.zookeeper.auth";

		internal const string ZK_PARENT_ZNODE_DEFAULT = "/hadoop-ha";

		/// <summary>All of the conf keys used by the ZKFC.</summary>
		/// <remarks>
		/// All of the conf keys used by the ZKFC. This is used in order to allow
		/// them to be overridden on a per-nameservice or per-namenode basis.
		/// </remarks>
		protected internal static readonly string[] ZKFC_CONF_KEYS = new string[] { ZK_QUORUM_KEY
			, ZK_SESSION_TIMEOUT_KEY, ZK_PARENT_ZNODE_KEY, ZK_ACL_KEY, ZK_AUTH_KEY };

		protected internal const string USAGE = "Usage: java zkfc [ -formatZK [-force] [-nonInteractive] ]";

		/// <summary>Unable to format the parent znode in ZK</summary>
		internal const int ERR_CODE_FORMAT_DENIED = 2;

		/// <summary>The parent znode doesn't exist in ZK</summary>
		internal const int ERR_CODE_NO_PARENT_ZNODE = 3;

		/// <summary>Fencing is not properly configured</summary>
		internal const int ERR_CODE_NO_FENCER = 4;

		/// <summary>Automatic failover is not enabled</summary>
		internal const int ERR_CODE_AUTO_FAILOVER_NOT_ENABLED = 5;

		/// <summary>Cannot connect to ZooKeeper</summary>
		internal const int ERR_CODE_NO_ZK = 6;

		protected internal org.apache.hadoop.conf.Configuration conf;

		private string zkQuorum;

		protected internal readonly org.apache.hadoop.ha.HAServiceTarget localTarget;

		private org.apache.hadoop.ha.HealthMonitor healthMonitor;

		private org.apache.hadoop.ha.ActiveStandbyElector elector;

		protected internal org.apache.hadoop.ha.ZKFCRpcServer rpcServer;

		private org.apache.hadoop.ha.HealthMonitor.State lastHealthState = org.apache.hadoop.ha.HealthMonitor.State
			.INITIALIZING;

		private volatile org.apache.hadoop.ha.HAServiceProtocol.HAServiceState serviceState
			 = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;

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
		/// <see cref="scheduleRecheck(long)"/>
		/// schedules events
		/// </summary>
		private java.util.concurrent.ScheduledExecutorService delayExecutor = java.util.concurrent.Executors
			.newScheduledThreadPool(1, new com.google.common.util.concurrent.ThreadFactoryBuilder
			().setDaemon(true).setNameFormat("ZKFC Delay timer #%d").build());

		private org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord lastActiveAttemptRecord;

		private object activeAttemptRecordLock = new object();

		protected internal ZKFailoverController(org.apache.hadoop.conf.Configuration conf
			, org.apache.hadoop.ha.HAServiceTarget localTarget)
		{
			this.localTarget = localTarget;
			this.conf = conf;
		}

		protected internal abstract byte[] targetToData(org.apache.hadoop.ha.HAServiceTarget
			 target);

		protected internal abstract org.apache.hadoop.ha.HAServiceTarget dataToTarget(byte
			[] data);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void loginAsFCUser();

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void checkRpcAdminAccess();

		protected internal abstract java.net.InetSocketAddress getRpcAddressToBindTo();

		protected internal abstract org.apache.hadoop.security.authorize.PolicyProvider getPolicyProvider
			();

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
		protected internal abstract string getScopeInsideParentNode();

		public virtual org.apache.hadoop.ha.HAServiceTarget getLocalTarget()
		{
			return localTarget;
		}

		internal virtual org.apache.hadoop.ha.HAServiceProtocol.HAServiceState getServiceState
			()
		{
			return serviceState;
		}

		/// <exception cref="System.Exception"/>
		public virtual int run(string[] args)
		{
			if (!localTarget.isAutoFailoverEnabled())
			{
				LOG.fatal("Automatic failover is not enabled for " + localTarget + "." + " Please ensure that automatic failover is enabled in the "
					 + "configuration before running the ZK failover controller.");
				return ERR_CODE_AUTO_FAILOVER_NOT_ENABLED;
			}
			loginAsFCUser();
			try
			{
				return org.apache.hadoop.security.SecurityUtil.doAsLoginUserOrFatal(new _PrivilegedAction_168
					(this, args));
			}
			catch (System.Exception rte)
			{
				throw (System.Exception)rte.InnerException;
			}
		}

		private sealed class _PrivilegedAction_168 : java.security.PrivilegedAction<int>
		{
			public _PrivilegedAction_168(ZKFailoverController _enclosing, string[] args)
			{
				this._enclosing = _enclosing;
				this.args = args;
			}

			public int run()
			{
				try
				{
					return this._enclosing.doRun(args);
				}
				catch (System.Exception t)
				{
					throw new System.Exception(t);
				}
				finally
				{
					if (this._enclosing.elector != null)
					{
						this._enclosing.elector.terminateConnection();
					}
				}
			}

			private readonly ZKFailoverController _enclosing;

			private readonly string[] args;
		}

		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private int doRun(string[] args)
		{
			try
			{
				initZK();
			}
			catch (org.apache.zookeeper.KeeperException)
			{
				LOG.fatal("Unable to start failover controller. Unable to connect " + "to ZooKeeper quorum at "
					 + zkQuorum + ". Please check the " + "configured value for " + ZK_QUORUM_KEY + 
					" and ensure that " + "ZooKeeper is running.");
				return ERR_CODE_NO_ZK;
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
								badArg(args[i]);
							}
						}
					}
					return formatZK(force, interactive);
				}
				else
				{
					badArg(args[0]);
				}
			}
			if (!elector.parentZNodeExists())
			{
				LOG.fatal("Unable to start failover controller. " + "Parent znode does not exist.\n"
					 + "Run with -formatZK flag to initialize ZooKeeper.");
				return ERR_CODE_NO_PARENT_ZNODE;
			}
			try
			{
				localTarget.checkFencingConfigured();
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException e)
			{
				LOG.fatal("Fencing is not configured for " + localTarget + ".\n" + "You must configure a fencing method before using automatic "
					 + "failover.", e);
				return ERR_CODE_NO_FENCER;
			}
			initRPC();
			initHM();
			startRPC();
			try
			{
				mainLoop();
			}
			finally
			{
				rpcServer.stopAndJoin();
				elector.quitElection(true);
				healthMonitor.shutdown();
				healthMonitor.join();
			}
			return 0;
		}

		private void badArg(string arg)
		{
			printUsage();
			throw new org.apache.hadoop.HadoopIllegalArgumentException("Bad argument: " + arg
				);
		}

		private void printUsage()
		{
			System.Console.Error.WriteLine(USAGE + "\n");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private int formatZK(bool force, bool interactive)
		{
			if (elector.parentZNodeExists())
			{
				if (!force && (!interactive || !confirmFormat()))
				{
					return ERR_CODE_FORMAT_DENIED;
				}
				try
				{
					elector.clearParentZNode();
				}
				catch (System.IO.IOException e)
				{
					LOG.error("Unable to clear zk parent znode", e);
					return 1;
				}
			}
			elector.ensureParentZNode();
			return 0;
		}

		private bool confirmFormat()
		{
			string parentZnode = getParentZnode();
			System.Console.Error.WriteLine("===============================================\n"
				 + "The configured parent znode " + parentZnode + " already exists.\n" + "Are you sure you want to clear all failover information from\n"
				 + "ZooKeeper?\n" + "WARNING: Before proceeding, ensure that all HDFS services and\n"
				 + "failover controllers are stopped!\n" + "==============================================="
				);
			try
			{
				return org.apache.hadoop.util.ToolRunner.confirmPrompt("Proceed formatting " + parentZnode
					 + "?");
			}
			catch (System.IO.IOException e)
			{
				LOG.debug("Failed to confirm", e);
				return false;
			}
		}

		// ------------------------------------------
		// Begin actual guts of failover controller
		// ------------------------------------------
		private void initHM()
		{
			healthMonitor = new org.apache.hadoop.ha.HealthMonitor(conf, localTarget);
			healthMonitor.addCallback(new org.apache.hadoop.ha.ZKFailoverController.HealthCallbacks
				(this));
			healthMonitor.addServiceStateCallback(new org.apache.hadoop.ha.ZKFailoverController.ServiceStateCallBacks
				(this));
			healthMonitor.start();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void initRPC()
		{
			java.net.InetSocketAddress bindAddr = getRpcAddressToBindTo();
			rpcServer = new org.apache.hadoop.ha.ZKFCRpcServer(conf, bindAddr, this, getPolicyProvider
				());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void startRPC()
		{
			rpcServer.start();
		}

		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private void initZK()
		{
			zkQuorum = conf.get(ZK_QUORUM_KEY);
			int zkTimeout = conf.getInt(ZK_SESSION_TIMEOUT_KEY, ZK_SESSION_TIMEOUT_DEFAULT);
			// Parse ACLs from configuration.
			string zkAclConf = conf.get(ZK_ACL_KEY, ZK_ACL_DEFAULT);
			zkAclConf = org.apache.hadoop.util.ZKUtil.resolveConfIndirection(zkAclConf);
			System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> zkAcls = org.apache.hadoop.util.ZKUtil
				.parseACLs(zkAclConf);
			if (zkAcls.isEmpty())
			{
				zkAcls = org.apache.zookeeper.ZooDefs.Ids.CREATOR_ALL_ACL;
			}
			// Parse authentication from configuration.
			string zkAuthConf = conf.get(ZK_AUTH_KEY);
			zkAuthConf = org.apache.hadoop.util.ZKUtil.resolveConfIndirection(zkAuthConf);
			System.Collections.Generic.IList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo> zkAuths;
			if (zkAuthConf != null)
			{
				zkAuths = org.apache.hadoop.util.ZKUtil.parseAuth(zkAuthConf);
			}
			else
			{
				zkAuths = java.util.Collections.emptyList();
			}
			// Sanity check configuration.
			com.google.common.@base.Preconditions.checkArgument(zkQuorum != null, "Missing required configuration '%s' for ZooKeeper quorum"
				, ZK_QUORUM_KEY);
			com.google.common.@base.Preconditions.checkArgument(zkTimeout > 0, "Invalid ZK session timeout %s"
				, zkTimeout);
			int maxRetryNum = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT
				);
			elector = new org.apache.hadoop.ha.ActiveStandbyElector(zkQuorum, zkTimeout, getParentZnode
				(), zkAcls, zkAuths, new org.apache.hadoop.ha.ZKFailoverController.ElectorCallbacks
				(this), maxRetryNum);
		}

		private string getParentZnode()
		{
			string znode = conf.get(ZK_PARENT_ZNODE_KEY, ZK_PARENT_ZNODE_DEFAULT);
			if (!znode.EndsWith("/"))
			{
				znode += "/";
			}
			return znode + getScopeInsideParentNode();
		}

		/// <exception cref="System.Exception"/>
		private void mainLoop()
		{
			lock (this)
			{
				while (fatalError == null)
				{
					Sharpen.Runtime.wait(this);
				}
				System.Diagnostics.Debug.Assert(fatalError != null);
				// only get here on fatal
				throw new System.Exception("ZK Failover Controller failed: " + fatalError);
			}
		}

		private void fatalError(string err)
		{
			lock (this)
			{
				LOG.fatal("Fatal error occurred:" + err);
				fatalError = err;
				Sharpen.Runtime.notifyAll(this);
			}
		}

		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private void becomeActive()
		{
			lock (this)
			{
				LOG.info("Trying to make " + localTarget + " active...");
				try
				{
					org.apache.hadoop.ha.HAServiceProtocolHelper.transitionToActive(localTarget.getProxy
						(conf, org.apache.hadoop.ha.FailoverController.getRpcTimeoutToNewActive(conf)), 
						createReqInfo());
					string msg = "Successfully transitioned " + localTarget + " to active state";
					LOG.info(msg);
					serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
					recordActiveAttempt(new org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord
						(true, msg));
				}
				catch (System.Exception t)
				{
					string msg = "Couldn't make " + localTarget + " active";
					LOG.fatal(msg, t);
					recordActiveAttempt(new org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord
						(false, msg + "\n" + org.apache.hadoop.util.StringUtils.stringifyException(t)));
					if (t is org.apache.hadoop.ha.ServiceFailedException)
					{
						throw (org.apache.hadoop.ha.ServiceFailedException)t;
					}
					else
					{
						throw new org.apache.hadoop.ha.ServiceFailedException("Couldn't transition to active"
							, t);
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
		private void recordActiveAttempt(org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord
			 record)
		{
			lock (activeAttemptRecordLock)
			{
				lastActiveAttemptRecord = record;
				Sharpen.Runtime.notifyAll(activeAttemptRecordLock);
			}
		}

		/// <summary>
		/// Wait until one of the following events:
		/// <ul>
		/// <li>Another thread publishes the results of an attempt to become active
		/// using
		/// <see cref="recordActiveAttempt(ActiveAttemptRecord)"/>
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
		private org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord waitForActiveAttempt
			(int timeoutMillis)
		{
			long st = Sharpen.Runtime.nanoTime();
			long waitUntil = st + java.util.concurrent.TimeUnit.NANOSECONDS.convert(timeoutMillis
				, java.util.concurrent.TimeUnit.MILLISECONDS);
			do
			{
				// periodically check health state, because entering an
				// unhealthy state could prevent us from ever attempting to
				// become active. We can detect this and respond to the user
				// immediately.
				lock (this)
				{
					if (lastHealthState != org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY)
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
					Sharpen.Runtime.wait(activeAttemptRecordLock, 1000);
				}
			}
			while (Sharpen.Runtime.nanoTime() < waitUntil);
			// Timeout elapsed.
			LOG.warn(timeoutMillis + "ms timeout elapsed waiting for an attempt " + "to become active"
				);
			return null;
		}

		private org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo createReqInfo
			()
		{
			return new org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo(org.apache.hadoop.ha.HAServiceProtocol.RequestSource
				.REQUEST_BY_ZKFC);
		}

		private void becomeStandby()
		{
			lock (this)
			{
				LOG.info("ZK Election indicated that " + localTarget + " should become standby");
				try
				{
					int timeout = org.apache.hadoop.ha.FailoverController.getGracefulFenceTimeout(conf
						);
					localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
					LOG.info("Successfully transitioned " + localTarget + " to standby state");
				}
				catch (System.Exception e)
				{
					LOG.error("Couldn't transition " + localTarget + " to standby state", e);
				}
				// TODO handle this. It's a likely case since we probably got fenced
				// at the same time.
				serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
			}
		}

		private void fenceOldActive(byte[] data)
		{
			lock (this)
			{
				org.apache.hadoop.ha.HAServiceTarget target = dataToTarget(data);
				try
				{
					doFence(target);
				}
				catch (System.Exception t)
				{
					recordActiveAttempt(new org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord
						(false, "Unable to fence old active: " + org.apache.hadoop.util.StringUtils.stringifyException
						(t)));
					com.google.common.@base.Throwables.propagate(t);
				}
			}
		}

		private void doFence(org.apache.hadoop.ha.HAServiceTarget target)
		{
			LOG.info("Should fence: " + target);
			bool gracefulWorked = new org.apache.hadoop.ha.FailoverController(conf, org.apache.hadoop.ha.HAServiceProtocol.RequestSource
				.REQUEST_BY_ZKFC).tryGracefulFence(target);
			if (gracefulWorked)
			{
				// It's possible that it's in standby but just about to go into active,
				// no? Is there some race here?
				LOG.info("Successfully transitioned " + target + " to standby " + "state without fencing"
					);
				return;
			}
			try
			{
				target.checkFencingConfigured();
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException e)
			{
				LOG.error("Couldn't fence old active " + target, e);
				recordActiveAttempt(new org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord
					(false, "Unable to fence old active"));
				throw new System.Exception(e);
			}
			if (!target.getFencer().fence(target))
			{
				throw new System.Exception("Unable to fence " + target);
			}
		}

		/// <summary>Request from graceful failover to cede active role.</summary>
		/// <remarks>
		/// Request from graceful failover to cede active role. Causes
		/// this ZKFC to transition its local node to standby, then quit
		/// the election for the specified period of time, after which it
		/// will rejoin iff it is healthy.
		/// </remarks>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void cedeActive(int millisToCede)
		{
			try
			{
				org.apache.hadoop.security.UserGroupInformation.getLoginUser().doAs(new _PrivilegedExceptionAction_547
					(this, millisToCede));
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_547 : java.security.PrivilegedExceptionAction
			<java.lang.Void>
		{
			public _PrivilegedExceptionAction_547(ZKFailoverController _enclosing, int millisToCede
				)
			{
				this._enclosing = _enclosing;
				this.millisToCede = millisToCede;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void run()
			{
				this._enclosing.doCedeActive(millisToCede);
				return null;
			}

			private readonly ZKFailoverController _enclosing;

			private readonly int millisToCede;
		}

		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		private void doCedeActive(int millisToCede)
		{
			int timeout = org.apache.hadoop.ha.FailoverController.getGracefulFenceTimeout(conf
				);
			// Lock elector to maintain lock ordering of elector -> ZKFC
			lock (elector)
			{
				lock (this)
				{
					if (millisToCede <= 0)
					{
						delayJoiningUntilNanotime = 0;
						recheckElectability();
						return;
					}
					LOG.info("Requested by " + org.apache.hadoop.security.UserGroupInformation.getCurrentUser
						() + " at " + org.apache.hadoop.ipc.Server.getRemoteAddress() + " to cede active role."
						);
					bool needFence = false;
					try
					{
						localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
						LOG.info("Successfully ensured local node is in standby mode");
					}
					catch (System.IO.IOException ioe)
					{
						LOG.warn("Unable to transition local node to standby: " + ioe.getLocalizedMessage
							());
						LOG.warn("Quitting election but indicating that fencing is " + "necessary");
						needFence = true;
					}
					delayJoiningUntilNanotime = Sharpen.Runtime.nanoTime() + java.util.concurrent.TimeUnit
						.MILLISECONDS.toNanos(millisToCede);
					elector.quitElection(needFence);
					serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;
				}
			}
			recheckElectability();
		}

		/// <summary>Coordinate a graceful failover to this node.</summary>
		/// <exception cref="ServiceFailedException">if the node fails to become active</exception>
		/// <exception cref="System.IO.IOException">some other error occurs</exception>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		internal virtual void gracefulFailoverToYou()
		{
			try
			{
				org.apache.hadoop.security.UserGroupInformation.getLoginUser().doAs(new _PrivilegedExceptionAction_601
					(this));
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_601 : java.security.PrivilegedExceptionAction
			<java.lang.Void>
		{
			public _PrivilegedExceptionAction_601(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public java.lang.Void run()
			{
				this._enclosing.doGracefulFailover();
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
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void doGracefulFailover()
		{
			int timeout = org.apache.hadoop.ha.FailoverController.getGracefulFenceTimeout(conf
				) * 2;
			// Phase 1: pre-flight checks
			checkEligibleForFailover();
			// Phase 2: determine old/current active node. Check that we're not
			// ourselves active, etc.
			org.apache.hadoop.ha.HAServiceTarget oldActive = getCurrentActive();
			if (oldActive == null)
			{
				// No node is currently active. So, if we aren't already
				// active ourselves by means of a normal election, then there's
				// probably something preventing us from becoming active.
				throw new org.apache.hadoop.ha.ServiceFailedException("No other node is currently active."
					);
			}
			if (oldActive.getAddress().Equals(localTarget.getAddress()))
			{
				LOG.info("Local node " + localTarget + " is already active. " + "No need to failover. Returning success."
					);
				return;
			}
			// Phase 3: ask the old active to yield from the election.
			LOG.info("Asking " + oldActive + " to cede its active state for " + timeout + "ms"
				);
			org.apache.hadoop.ha.ZKFCProtocol oldZkfc = oldActive.getZKFCProxy(conf, timeout);
			oldZkfc.cedeActive(timeout);
			// Phase 4: wait for the normal election to make the local node
			// active.
			org.apache.hadoop.ha.ZKFailoverController.ActiveAttemptRecord attempt = waitForActiveAttempt
				(timeout + 60000);
			if (attempt == null)
			{
				// We didn't even make an attempt to become active.
				lock (this)
				{
					if (lastHealthState != org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY)
					{
						throw new org.apache.hadoop.ha.ServiceFailedException("Unable to become active. "
							 + "Service became unhealthy while trying to failover.");
					}
				}
				throw new org.apache.hadoop.ha.ServiceFailedException("Unable to become active. "
					 + "Local node did not get an opportunity to do so from ZooKeeper, " + "or the local node took too long to transition to active."
					);
			}
			// Phase 5. At this point, we made some attempt to become active. So we
			// can tell the old active to rejoin if it wants. This allows a quick
			// fail-back if we immediately crash.
			oldZkfc.cedeActive(-1);
			if (attempt.succeeded)
			{
				LOG.info("Successfully became active. " + attempt.status);
			}
			else
			{
				// Propagate failure
				string msg = "Failed to become active. " + attempt.status;
				throw new org.apache.hadoop.ha.ServiceFailedException(msg);
			}
		}

		/// <summary>
		/// Ensure that the local node is in a healthy state, and thus
		/// eligible for graceful failover.
		/// </summary>
		/// <exception cref="ServiceFailedException">if the node is unhealthy</exception>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		private void checkEligibleForFailover()
		{
			lock (this)
			{
				// Check health
				if (this.getLastHealthState() != org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY)
				{
					throw new org.apache.hadoop.ha.ServiceFailedException(localTarget + " is not currently healthy. "
						 + "Cannot be failover target");
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
		private org.apache.hadoop.ha.HAServiceTarget getCurrentActive()
		{
			lock (elector)
			{
				lock (this)
				{
					byte[] activeData;
					try
					{
						activeData = elector.getActiveData();
					}
					catch (org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException)
					{
						return null;
					}
					catch (org.apache.zookeeper.KeeperException ke)
					{
						throw new System.IO.IOException("Unexpected ZooKeeper issue fetching active node info"
							, ke);
					}
					org.apache.hadoop.ha.HAServiceTarget oldActive = dataToTarget(activeData);
					return oldActive;
				}
			}
		}

		/// <summary>
		/// Check the current state of the service, and join the election
		/// if it should be in the election.
		/// </summary>
		private void recheckElectability()
		{
			// Maintain lock ordering of elector -> ZKFC
			lock (elector)
			{
				lock (this)
				{
					bool healthy = lastHealthState == org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY;
					long remainingDelay = delayJoiningUntilNanotime - Sharpen.Runtime.nanoTime();
					if (remainingDelay > 0)
					{
						if (healthy)
						{
							LOG.info("Would have joined master election, but this node is " + "prohibited from doing so for "
								 + java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remainingDelay) + " more ms"
								);
						}
						scheduleRecheck(remainingDelay);
						return;
					}
					switch (lastHealthState)
					{
						case org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY:
						{
							elector.joinElection(targetToData(localTarget));
							if (quitElectionOnBadState)
							{
								quitElectionOnBadState = false;
							}
							break;
						}

						case org.apache.hadoop.ha.HealthMonitor.State.INITIALIZING:
						{
							LOG.info("Ensuring that " + localTarget + " does not " + "participate in active master election"
								);
							elector.quitElection(false);
							serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;
							break;
						}

						case org.apache.hadoop.ha.HealthMonitor.State.SERVICE_UNHEALTHY:
						case org.apache.hadoop.ha.HealthMonitor.State.SERVICE_NOT_RESPONDING:
						{
							LOG.info("Quitting master election for " + localTarget + " and marking that fencing is necessary"
								);
							elector.quitElection(true);
							serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;
							break;
						}

						case org.apache.hadoop.ha.HealthMonitor.State.HEALTH_MONITOR_FAILED:
						{
							fatalError("Health monitor failed!");
							break;
						}

						default:
						{
							throw new System.ArgumentException("Unhandled state:" + lastHealthState);
						}
					}
				}
			}
		}

		/// <summary>
		/// Schedule a call to
		/// <see cref="recheckElectability()"/>
		/// in the future.
		/// </summary>
		private void scheduleRecheck(long whenNanos)
		{
			delayExecutor.schedule(new _Runnable_790(this), whenNanos, java.util.concurrent.TimeUnit
				.NANOSECONDS);
		}

		private sealed class _Runnable_790 : java.lang.Runnable
		{
			public _Runnable_790(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void run()
			{
				try
				{
					this._enclosing.recheckElectability();
				}
				catch (System.Exception t)
				{
					this._enclosing.fatalError("Failed to recheck electability: " + org.apache.hadoop.util.StringUtils
						.stringifyException(t));
				}
			}

			private readonly ZKFailoverController _enclosing;
		}

		internal int serviceStateMismatchCount = 0;

		internal bool quitElectionOnBadState = false;

		internal virtual void verifyChangedServiceState(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
			 changedState)
		{
			lock (elector)
			{
				lock (this)
				{
					if (serviceState == org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING)
					{
						if (quitElectionOnBadState)
						{
							LOG.debug("rechecking for electability from bad state");
							recheckElectability();
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
					LOG.error("Local service " + localTarget + " has changed the serviceState to " + 
						changedState + ". Expected was " + serviceState + ". Quitting election marking fencing necessary."
						);
					delayJoiningUntilNanotime = Sharpen.Runtime.nanoTime() + java.util.concurrent.TimeUnit
						.MILLISECONDS.toNanos(1000);
					elector.quitElection(true);
					quitElectionOnBadState = true;
					serviceStateMismatchCount = 0;
					serviceState = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING;
				}
			}
		}

		/// <returns>
		/// the last health state passed to the FC
		/// by the HealthMonitor.
		/// </returns>
		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.ha.HealthMonitor.State getLastHealthState()
		{
			lock (this)
			{
				return lastHealthState;
			}
		}

		private void setLastHealthState(org.apache.hadoop.ha.HealthMonitor.State newState
			)
		{
			lock (this)
			{
				LOG.info("Local service " + localTarget + " entered state: " + newState);
				lastHealthState = newState;
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.ha.ActiveStandbyElector getElectorForTests()
		{
			return elector;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.ha.ZKFCRpcServer getRpcServerForTests()
		{
			return rpcServer;
		}

		/// <summary>Callbacks from elector</summary>
		internal class ElectorCallbacks : org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
		{
			/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
			public virtual void becomeActive()
			{
				this._enclosing.becomeActive();
			}

			public virtual void becomeStandby()
			{
				this._enclosing.becomeStandby();
			}

			public virtual void enterNeutralMode()
			{
			}

			public virtual void notifyFatalError(string errorMessage)
			{
				this._enclosing.fatalError(errorMessage);
			}

			public virtual void fenceOldActive(byte[] data)
			{
				this._enclosing.fenceOldActive(data);
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
		internal class HealthCallbacks : org.apache.hadoop.ha.HealthMonitor.Callback
		{
			public virtual void enteredState(org.apache.hadoop.ha.HealthMonitor.State newState
				)
			{
				this._enclosing.setLastHealthState(newState);
				this._enclosing.recheckElectability();
			}

			internal HealthCallbacks(ZKFailoverController _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKFailoverController _enclosing;
		}

		/// <summary>Callbacks for HAServiceStatus</summary>
		internal class ServiceStateCallBacks : org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback
		{
			public virtual void reportServiceStatus(org.apache.hadoop.ha.HAServiceStatus status
				)
			{
				this._enclosing.verifyChangedServiceState(status.getState());
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
				this.nanoTime = Sharpen.Runtime.nanoTime();
			}
		}
	}
}
