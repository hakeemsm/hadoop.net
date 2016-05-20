using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// This class implements a simple library to perform leader election on top of
	/// Apache Zookeeper.
	/// </summary>
	/// <remarks>
	/// This class implements a simple library to perform leader election on top of
	/// Apache Zookeeper. Using Zookeeper as a coordination service, leader election
	/// can be performed by atomically creating an ephemeral lock file (znode) on
	/// Zookeeper. The service instance that successfully creates the znode becomes
	/// active and the rest become standbys. <br/>
	/// This election mechanism is only efficient for small number of election
	/// candidates (order of 10's) because contention on single znode by a large
	/// number of candidates can result in Zookeeper overload. <br/>
	/// The elector does not guarantee fencing (protection of shared resources) among
	/// service instances. After it has notified an instance about becoming a leader,
	/// then that instance must ensure that it meets the service consistency
	/// requirements. If it cannot do so, then it is recommended to quit the
	/// election. The application implements the
	/// <see cref="ActiveStandbyElectorCallback"/>
	/// to interact with the elector
	/// </remarks>
	public class ActiveStandbyElector : org.apache.zookeeper.AsyncCallback.StatCallback
		, org.apache.zookeeper.AsyncCallback.StringCallback
	{
		/// <summary>Callback interface to interact with the ActiveStandbyElector object.</summary>
		/// <remarks>
		/// Callback interface to interact with the ActiveStandbyElector object. <br/>
		/// The application will be notified with a callback only on state changes
		/// (i.e. there will never be successive calls to becomeActive without an
		/// intermediate call to enterNeutralMode). <br/>
		/// The callbacks will be running on Zookeeper client library threads. The
		/// application should return from these callbacks quickly so as not to impede
		/// Zookeeper client library performance and notifications. The app will
		/// typically remember the state change and return from the callback. It will
		/// then proceed with implementing actions around that state change. It is
		/// possible to be called back again while these actions are in flight and the
		/// app should handle this scenario.
		/// </remarks>
		public interface ActiveStandbyElectorCallback
		{
			/// <summary>This method is called when the app becomes the active leader.</summary>
			/// <remarks>
			/// This method is called when the app becomes the active leader.
			/// If the service fails to become active, it should throw
			/// ServiceFailedException. This will cause the elector to
			/// sleep for a short period, then re-join the election.
			/// Callback implementations are expected to manage their own
			/// timeouts (e.g. when making an RPC to a remote node).
			/// </remarks>
			/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
			void becomeActive();

			/// <summary>This method is called when the app becomes a standby</summary>
			void becomeStandby();

			/// <summary>
			/// If the elector gets disconnected from Zookeeper and does not know about
			/// the lock state, then it will notify the service via the enterNeutralMode
			/// interface.
			/// </summary>
			/// <remarks>
			/// If the elector gets disconnected from Zookeeper and does not know about
			/// the lock state, then it will notify the service via the enterNeutralMode
			/// interface. The service may choose to ignore this or stop doing state
			/// changing operations. Upon reconnection, the elector verifies the leader
			/// status and calls back on the becomeActive and becomeStandby app
			/// interfaces. <br/>
			/// Zookeeper disconnects can happen due to network issues or loss of
			/// Zookeeper quorum. Thus enterNeutralMode can be used to guard against
			/// split-brain issues. In such situations it might be prudent to call
			/// becomeStandby too. However, such state change operations might be
			/// expensive and enterNeutralMode can help guard against doing that for
			/// transient issues.
			/// </remarks>
			void enterNeutralMode();

			/// <summary>If there is any fatal error (e.g.</summary>
			/// <remarks>
			/// If there is any fatal error (e.g. wrong ACL's, unexpected Zookeeper
			/// errors or Zookeeper persistent unavailability) then notifyFatalError is
			/// called to notify the app about it.
			/// </remarks>
			void notifyFatalError(string errorMessage);

			/// <summary>
			/// If an old active has failed, rather than exited gracefully, then
			/// the new active may need to take some fencing actions against it
			/// before proceeding with failover.
			/// </summary>
			/// <param name="oldActiveData">the application data provided by the prior active</param>
			void fenceOldActive(byte[] oldActiveData);
		}

		/// <summary>Name of the lock znode used by the library.</summary>
		/// <remarks>
		/// Name of the lock znode used by the library. Protected for access in test
		/// classes
		/// </remarks>
		[com.google.common.annotations.VisibleForTesting]
		protected internal const string LOCK_FILENAME = "ActiveStandbyElectorLock";

		[com.google.common.annotations.VisibleForTesting]
		protected internal const string BREADCRUMB_FILENAME = "ActiveBreadCrumb";

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ActiveStandbyElector
			)));

		private const int SLEEP_AFTER_FAILURE_TO_BECOME_ACTIVE = 1000;

		private enum ConnectionState
		{
			DISCONNECTED,
			CONNECTED,
			TERMINATED
		}

		internal enum State
		{
			INIT,
			ACTIVE,
			STANDBY,
			NEUTRAL
		}

		private org.apache.hadoop.ha.ActiveStandbyElector.State state = org.apache.hadoop.ha.ActiveStandbyElector.State
			.INIT;

		private int createRetryCount = 0;

		private int statRetryCount = 0;

		private org.apache.zookeeper.ZooKeeper zkClient;

		private org.apache.hadoop.ha.ActiveStandbyElector.WatcherWithClientRef watcher;

		private org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState zkConnectionState
			 = org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState.TERMINATED;

		private readonly org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
			 appClient;

		private readonly string zkHostPort;

		private readonly int zkSessionTimeout;

		private readonly System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> 
			zkAcl;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo
			> zkAuthInfo;

		private byte[] appData;

		private readonly string zkLockFilePath;

		private readonly string zkBreadCrumbPath;

		private readonly string znodeWorkingDir;

		private readonly int maxRetryNum;

		private java.util.concurrent.locks.Lock sessionReestablishLockForTests = new java.util.concurrent.locks.ReentrantLock
			();

		private bool wantToBeInElection;

		private bool monitorLockNodePending = false;

		private org.apache.zookeeper.ZooKeeper monitorLockNodeClient;

		/// <summary>
		/// Create a new ActiveStandbyElector object <br/>
		/// The elector is created by providing to it the Zookeeper configuration, the
		/// parent znode under which to create the znode and a reference to the
		/// callback interface.
		/// </summary>
		/// <remarks>
		/// Create a new ActiveStandbyElector object <br/>
		/// The elector is created by providing to it the Zookeeper configuration, the
		/// parent znode under which to create the znode and a reference to the
		/// callback interface. <br/>
		/// The parent znode name must be the same for all service instances and
		/// different across services. <br/>
		/// After the leader has been lost, a new leader will be elected after the
		/// session timeout expires. Hence, the app must set this parameter based on
		/// its needs for failure response time. The session timeout must be greater
		/// than the Zookeeper disconnect timeout and is recommended to be 3X that
		/// value to enable Zookeeper to retry transient disconnections. Setting a very
		/// short session timeout may result in frequent transitions between active and
		/// standby states during issues like network outages/GS pauses.
		/// </remarks>
		/// <param name="zookeeperHostPorts">ZooKeeper hostPort for all ZooKeeper servers</param>
		/// <param name="zookeeperSessionTimeout">ZooKeeper session timeout</param>
		/// <param name="parentZnodeName">znode under which to create the lock</param>
		/// <param name="acl">ZooKeeper ACL's</param>
		/// <param name="authInfo">
		/// a list of authentication credentials to add to the
		/// ZK connection
		/// </param>
		/// <param name="app">reference to callback interface object</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		public ActiveStandbyElector(string zookeeperHostPorts, int zookeeperSessionTimeout
			, string parentZnodeName, System.Collections.Generic.IList<org.apache.zookeeper.data.ACL
			> acl, System.Collections.Generic.IList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo
			> authInfo, org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
			 app, int maxRetryNum)
		{
			if (app == null || acl == null || parentZnodeName == null || zookeeperHostPorts ==
				 null || zookeeperSessionTimeout <= 0)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Invalid argument");
			}
			zkHostPort = zookeeperHostPorts;
			zkSessionTimeout = zookeeperSessionTimeout;
			zkAcl = acl;
			zkAuthInfo = authInfo;
			appClient = app;
			znodeWorkingDir = parentZnodeName;
			zkLockFilePath = znodeWorkingDir + "/" + LOCK_FILENAME;
			zkBreadCrumbPath = znodeWorkingDir + "/" + BREADCRUMB_FILENAME;
			this.maxRetryNum = maxRetryNum;
			// createConnection for future API calls
			createConnection();
		}

		/// <summary>To participate in election, the app will call joinElection.</summary>
		/// <remarks>
		/// To participate in election, the app will call joinElection. The result will
		/// be notified by a callback on either the becomeActive or becomeStandby app
		/// interfaces. <br/>
		/// After this the elector will automatically monitor the leader status and
		/// perform re-election if necessary<br/>
		/// The app could potentially start off in standby mode and ignore the
		/// becomeStandby call.
		/// </remarks>
		/// <param name="data">to be set by the app. non-null data must be set.</param>
		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException">if valid data is not supplied
		/// 	</exception>
		public virtual void joinElection(byte[] data)
		{
			lock (this)
			{
				if (data == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("data cannot be null");
				}
				if (wantToBeInElection)
				{
					LOG.info("Already in election. Not re-connecting.");
					return;
				}
				appData = new byte[data.Length];
				System.Array.Copy(data, 0, appData, 0, data.Length);
				LOG.debug("Attempting active election for " + this);
				joinElectionInternal();
			}
		}

		/// <returns>true if the configured parent znode exists</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool parentZNodeExists()
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkState(zkClient != null);
				try
				{
					return zkClient.exists(znodeWorkingDir, false) != null;
				}
				catch (org.apache.zookeeper.KeeperException e)
				{
					throw new System.IO.IOException("Couldn't determine existence of znode '" + znodeWorkingDir
						 + "'", e);
				}
			}
		}

		/// <summary>Utility function to ensure that the configured base znode exists.</summary>
		/// <remarks>
		/// Utility function to ensure that the configured base znode exists.
		/// This recursively creates the znode as well as all of its parents.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void ensureParentZNode()
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkState(!wantToBeInElection, "ensureParentZNode() may not be called while in the election"
					);
				string[] pathParts = znodeWorkingDir.split("/");
				com.google.common.@base.Preconditions.checkArgument(pathParts.Length >= 1 && pathParts
					[0].isEmpty(), "Invalid path: %s", znodeWorkingDir);
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				for (int i = 1; i < pathParts.Length; i++)
				{
					sb.Append("/").Append(pathParts[i]);
					string prefixPath = sb.ToString();
					LOG.debug("Ensuring existence of " + prefixPath);
					try
					{
						createWithRetries(prefixPath, new byte[] {  }, zkAcl, org.apache.zookeeper.CreateMode
							.PERSISTENT);
					}
					catch (org.apache.zookeeper.KeeperException e)
					{
						if (isNodeExists(e.code()))
						{
							// This is OK - just ensuring existence.
							continue;
						}
						else
						{
							throw new System.IO.IOException("Couldn't create " + prefixPath, e);
						}
					}
				}
				LOG.info("Successfully created " + znodeWorkingDir + " in ZK.");
			}
		}

		/// <summary>Clear all of the state held within the parent ZNode.</summary>
		/// <remarks>
		/// Clear all of the state held within the parent ZNode.
		/// This recursively deletes everything within the znode as well as the
		/// parent znode itself. It should only be used when it's certain that
		/// no electors are currently participating in the election.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void clearParentZNode()
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkState(!wantToBeInElection, "clearParentZNode() may not be called while in the election"
					);
				try
				{
					LOG.info("Recursively deleting " + znodeWorkingDir + " from ZK...");
					zkDoWithRetries(new _ZKAction_327(this));
				}
				catch (org.apache.zookeeper.KeeperException e)
				{
					throw new System.IO.IOException("Couldn't clear parent znode " + znodeWorkingDir, 
						e);
				}
				LOG.info("Successfully deleted " + znodeWorkingDir + " from ZK.");
			}
		}

		private sealed class _ZKAction_327 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<java.lang.Void>
		{
			public _ZKAction_327(ActiveStandbyElector _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public java.lang.Void run()
			{
				org.apache.zookeeper.ZKUtil.deleteRecursive(this._enclosing.zkClient, this._enclosing
					.znodeWorkingDir);
				return null;
			}

			private readonly ActiveStandbyElector _enclosing;
		}

		/// <summary>Any service instance can drop out of the election by calling quitElection.
		/// 	</summary>
		/// <remarks>
		/// Any service instance can drop out of the election by calling quitElection.
		/// <br/>
		/// This will lose any leader status, if held, and stop monitoring of the lock
		/// node. <br/>
		/// If the instance wants to participate in election again, then it needs to
		/// call joinElection(). <br/>
		/// This allows service instances to take themselves out of rotation for known
		/// impending unavailable states (e.g. long GC pause or software upgrade).
		/// </remarks>
		/// <param name="needFence">
		/// true if the underlying daemon may need to be fenced
		/// if a failover occurs due to dropping out of the election.
		/// </param>
		public virtual void quitElection(bool needFence)
		{
			lock (this)
			{
				LOG.info("Yielding from election");
				if (!needFence && state == org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE)
				{
					// If active is gracefully going back to standby mode, remove
					// our permanent znode so no one fences us.
					tryDeleteOwnBreadCrumbNode();
				}
				reset();
				wantToBeInElection = false;
			}
		}

		/// <summary>Exception thrown when there is no active leader</summary>
		[System.Serializable]
		public class ActiveNotFoundException : System.Exception
		{
			private const long serialVersionUID = 3505396722342846462L;
		}

		/// <summary>get data set by the active leader</summary>
		/// <returns>data set by the active instance</returns>
		/// <exception cref="ActiveNotFoundException">when there is no active leader</exception>
		/// <exception cref="org.apache.zookeeper.KeeperException">other zookeeper operation errors
		/// 	</exception>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException">when ZooKeeper connection could not be established
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException
		/// 	"/>
		public virtual byte[] getActiveData()
		{
			lock (this)
			{
				try
				{
					if (zkClient == null)
					{
						createConnection();
					}
					org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
					return getDataWithRetries(zkLockFilePath, false, stat);
				}
				catch (org.apache.zookeeper.KeeperException e)
				{
					org.apache.zookeeper.KeeperException.Code code = e.code();
					if (isNodeDoesNotExist(code))
					{
						// handle the commonly expected cases that make sense for us
						throw new org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException();
					}
					else
					{
						throw;
					}
				}
			}
		}

		/// <summary>interface implementation of Zookeeper callback for create</summary>
		public virtual void processResult(int rc, string path, object ctx, string name)
		{
			lock (this)
			{
				if (isStaleClient(ctx))
				{
					return;
				}
				LOG.debug("CreateNode result: " + rc + " for path: " + path + " connectionState: "
					 + zkConnectionState + "  for " + this);
				org.apache.zookeeper.KeeperException.Code code = org.apache.zookeeper.KeeperException.Code
					.get(rc);
				if (isSuccess(code))
				{
					// we successfully created the znode. we are the leader. start monitoring
					if (becomeActive())
					{
						monitorActiveStatus();
					}
					else
					{
						reJoinElectionAfterFailureToBecomeActive();
					}
					return;
				}
				if (isNodeExists(code))
				{
					if (createRetryCount == 0)
					{
						// znode exists and we did not retry the operation. so a different
						// instance has created it. become standby and monitor lock.
						becomeStandby();
					}
					// if we had retried then the znode could have been created by our first
					// attempt to the server (that we lost) and this node exists response is
					// for the second attempt. verify this case via ephemeral node owner. this
					// will happen on the callback for monitoring the lock.
					monitorActiveStatus();
					return;
				}
				string errorMessage = "Received create error from Zookeeper. code:" + code.ToString
					() + " for path " + path;
				LOG.debug(errorMessage);
				if (shouldRetry(code))
				{
					if (createRetryCount < maxRetryNum)
					{
						LOG.debug("Retrying createNode createRetryCount: " + createRetryCount);
						++createRetryCount;
						createLockNodeAsync();
						return;
					}
					errorMessage = errorMessage + ". Not retrying further znode create connection errors.";
				}
				else
				{
					if (isSessionExpired(code))
					{
						// This isn't fatal - the client Watcher will re-join the election
						LOG.warn("Lock acquisition failed because session was lost");
						return;
					}
				}
				fatalError(errorMessage);
			}
		}

		/// <summary>interface implementation of Zookeeper callback for monitor (exists)</summary>
		public virtual void processResult(int rc, string path, object ctx, org.apache.zookeeper.data.Stat
			 stat)
		{
			lock (this)
			{
				if (isStaleClient(ctx))
				{
					return;
				}
				monitorLockNodePending = false;
				System.Diagnostics.Debug.Assert(wantToBeInElection, "Got a StatNode result after quitting election"
					);
				LOG.debug("StatNode result: " + rc + " for path: " + path + " connectionState: " 
					+ zkConnectionState + " for " + this);
				org.apache.zookeeper.KeeperException.Code code = org.apache.zookeeper.KeeperException.Code
					.get(rc);
				if (isSuccess(code))
				{
					// the following owner check completes verification in case the lock znode
					// creation was retried
					if (stat.getEphemeralOwner() == zkClient.getSessionId())
					{
						// we own the lock znode. so we are the leader
						if (!becomeActive())
						{
							reJoinElectionAfterFailureToBecomeActive();
						}
					}
					else
					{
						// we dont own the lock znode. so we are a standby.
						becomeStandby();
					}
					// the watch set by us will notify about changes
					return;
				}
				if (isNodeDoesNotExist(code))
				{
					// the lock znode disappeared before we started monitoring it
					enterNeutralMode();
					joinElectionInternal();
					return;
				}
				string errorMessage = "Received stat error from Zookeeper. code:" + code.ToString
					();
				LOG.debug(errorMessage);
				if (shouldRetry(code))
				{
					if (statRetryCount < maxRetryNum)
					{
						++statRetryCount;
						monitorLockNodeAsync();
						return;
					}
					errorMessage = errorMessage + ". Not retrying further znode monitoring connection errors.";
				}
				else
				{
					if (isSessionExpired(code))
					{
						// This isn't fatal - the client Watcher will re-join the election
						LOG.warn("Lock monitoring failed because session was lost");
						return;
					}
				}
				fatalError(errorMessage);
			}
		}

		/// <summary>We failed to become active.</summary>
		/// <remarks>
		/// We failed to become active. Re-join the election, but
		/// sleep for a few seconds after terminating our existing
		/// session, so that other nodes have a chance to become active.
		/// The failure to become active is already logged inside
		/// becomeActive().
		/// </remarks>
		private void reJoinElectionAfterFailureToBecomeActive()
		{
			reJoinElection(SLEEP_AFTER_FAILURE_TO_BECOME_ACTIVE);
		}

		/// <summary>
		/// interface implementation of Zookeeper watch events (connection and node),
		/// proxied by
		/// <see cref="WatcherWithClientRef"/>
		/// .
		/// </summary>
		internal virtual void processWatchEvent(org.apache.zookeeper.ZooKeeper zk, org.apache.zookeeper.WatchedEvent
			 @event)
		{
			lock (this)
			{
				org.apache.zookeeper.Watcher.Event.EventType eventType = @event.getType();
				if (isStaleClient(zk))
				{
					return;
				}
				LOG.debug("Watcher event type: " + eventType + " with state:" + @event.getState()
					 + " for path:" + @event.getPath() + " connectionState: " + zkConnectionState + 
					" for " + this);
				if (eventType == org.apache.zookeeper.Watcher.Event.EventType.None)
				{
					switch (@event.getState())
					{
						case org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected:
						{
							// the connection state has changed
							LOG.info("Session connected.");
							// if the listener was asked to move to safe state then it needs to
							// be undone
							org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState prevConnectionState = zkConnectionState;
							zkConnectionState = org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState.CONNECTED;
							if (prevConnectionState == org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState
								.DISCONNECTED && wantToBeInElection)
							{
								monitorActiveStatus();
							}
							break;
						}

						case org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected:
						{
							LOG.info("Session disconnected. Entering neutral mode...");
							// ask the app to move to safe state because zookeeper connection
							// is not active and we dont know our state
							zkConnectionState = org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState.DISCONNECTED;
							enterNeutralMode();
							break;
						}

						case org.apache.zookeeper.Watcher.Event.KeeperState.Expired:
						{
							// the connection got terminated because of session timeout
							// call listener to reconnect
							LOG.info("Session expired. Entering neutral mode and rejoining...");
							enterNeutralMode();
							reJoinElection(0);
							break;
						}

						case org.apache.zookeeper.Watcher.Event.KeeperState.SaslAuthenticated:
						{
							LOG.info("Successfully authenticated to ZooKeeper using SASL.");
							break;
						}

						default:
						{
							fatalError("Unexpected Zookeeper watch event state: " + @event.getState());
							break;
						}
					}
					return;
				}
				// a watch on lock path in zookeeper has fired. so something has changed on
				// the lock. ideally we should check that the path is the same as the lock
				// path but trusting zookeeper for now
				string path = @event.getPath();
				if (path != null)
				{
					switch (eventType)
					{
						case org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted:
						{
							if (state == org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE)
							{
								enterNeutralMode();
							}
							joinElectionInternal();
							break;
						}

						case org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged:
						{
							monitorActiveStatus();
							break;
						}

						default:
						{
							LOG.debug("Unexpected node event: " + eventType + " for path: " + path);
							monitorActiveStatus();
							break;
						}
					}
					return;
				}
				// some unexpected error has occurred
				fatalError("Unexpected watch error from Zookeeper");
			}
		}

		/// <summary>Get a new zookeeper client instance.</summary>
		/// <remarks>
		/// Get a new zookeeper client instance. protected so that test class can
		/// inherit and pass in a mock object for zookeeper
		/// </remarks>
		/// <returns>new zookeeper client instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.zookeeper.KeeperException">zookeeper connectionloss exception
		/// 	</exception>
		protected internal virtual org.apache.zookeeper.ZooKeeper getNewZooKeeper()
		{
			lock (this)
			{
				// Unfortunately, the ZooKeeper constructor connects to ZooKeeper and
				// may trigger the Connected event immediately. So, if we register the
				// watcher after constructing ZooKeeper, we may miss that event. Instead,
				// we construct the watcher first, and have it block any events it receives
				// before we can set its ZooKeeper reference.
				watcher = new org.apache.hadoop.ha.ActiveStandbyElector.WatcherWithClientRef(this
					);
				org.apache.zookeeper.ZooKeeper zk = new org.apache.zookeeper.ZooKeeper(zkHostPort
					, zkSessionTimeout, watcher);
				watcher.setZooKeeperRef(zk);
				// Wait for the asynchronous success/failure. This may throw an exception
				// if we don't connect within the session timeout.
				watcher.waitForZKConnectionEvent(zkSessionTimeout);
				foreach (org.apache.hadoop.util.ZKUtil.ZKAuthInfo auth in zkAuthInfo)
				{
					zk.addAuthInfo(auth.getScheme(), auth.getAuth());
				}
				return zk;
			}
		}

		private void fatalError(string errorMessage)
		{
			LOG.fatal(errorMessage);
			reset();
			appClient.notifyFatalError(errorMessage);
		}

		private void monitorActiveStatus()
		{
			System.Diagnostics.Debug.Assert(wantToBeInElection);
			LOG.debug("Monitoring active leader for " + this);
			statRetryCount = 0;
			monitorLockNodeAsync();
		}

		private void joinElectionInternal()
		{
			com.google.common.@base.Preconditions.checkState(appData != null, "trying to join election without any app data"
				);
			if (zkClient == null)
			{
				if (!reEstablishSession())
				{
					fatalError("Failed to reEstablish connection with ZooKeeper");
					return;
				}
			}
			createRetryCount = 0;
			wantToBeInElection = true;
			createLockNodeAsync();
		}

		private void reJoinElection(int sleepTime)
		{
			LOG.info("Trying to re-establish ZK session");
			// Some of the test cases rely on expiring the ZK sessions and
			// ensuring that the other node takes over. But, there's a race
			// where the original lease holder could reconnect faster than the other
			// thread manages to take the lock itself. This lock allows the
			// tests to block the reconnection. It's a shame that this leaked
			// into non-test code, but the lock is only acquired here so will never
			// be contended.
			sessionReestablishLockForTests.Lock();
			try
			{
				terminateConnection();
				sleepFor(sleepTime);
				// Should not join election even before the SERVICE is reported
				// as HEALTHY from ZKFC monitoring.
				if (appData != null)
				{
					joinElectionInternal();
				}
				else
				{
					LOG.info("Not joining election since service has not yet been " + "reported as healthy."
						);
				}
			}
			finally
			{
				sessionReestablishLockForTests.unlock();
			}
		}

		/// <summary>Sleep for the given number of milliseconds.</summary>
		/// <remarks>
		/// Sleep for the given number of milliseconds.
		/// This is non-static, and separated out, so that unit tests
		/// can override the behavior not to sleep.
		/// </remarks>
		[com.google.common.annotations.VisibleForTesting]
		protected internal virtual void sleepFor(int sleepMs)
		{
			if (sleepMs > 0)
			{
				try
				{
					java.lang.Thread.sleep(sleepMs);
				}
				catch (System.Exception)
				{
					java.lang.Thread.currentThread().interrupt();
				}
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual void preventSessionReestablishmentForTests()
		{
			sessionReestablishLockForTests.Lock();
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual void allowSessionReestablishmentForTests()
		{
			sessionReestablishLockForTests.unlock();
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual long getZKSessionIdForTests()
		{
			lock (this)
			{
				if (zkClient != null)
				{
					return zkClient.getSessionId();
				}
				else
				{
					return -1;
				}
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.ha.ActiveStandbyElector.State getStateForTests
			()
		{
			lock (this)
			{
				return state;
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual bool isMonitorLockNodePending()
		{
			lock (this)
			{
				return monitorLockNodePending;
			}
		}

		private bool reEstablishSession()
		{
			int connectionRetryCount = 0;
			bool success = false;
			while (!success && connectionRetryCount < maxRetryNum)
			{
				LOG.debug("Establishing zookeeper connection for " + this);
				try
				{
					createConnection();
					success = true;
				}
				catch (System.IO.IOException e)
				{
					LOG.warn(e);
					sleepFor(5000);
				}
				catch (org.apache.zookeeper.KeeperException e)
				{
					LOG.warn(e);
					sleepFor(5000);
				}
				++connectionRetryCount;
			}
			return success;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private void createConnection()
		{
			if (zkClient != null)
			{
				try
				{
					zkClient.close();
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException("Interrupted while closing ZK", e);
				}
				zkClient = null;
				watcher = null;
			}
			zkClient = getNewZooKeeper();
			LOG.debug("Created new connection for " + this);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void terminateConnection()
		{
			lock (this)
			{
				if (zkClient == null)
				{
					return;
				}
				LOG.debug("Terminating ZK connection for " + this);
				org.apache.zookeeper.ZooKeeper tempZk = zkClient;
				zkClient = null;
				watcher = null;
				try
				{
					tempZk.close();
				}
				catch (System.Exception e)
				{
					LOG.warn(e);
				}
				zkConnectionState = org.apache.hadoop.ha.ActiveStandbyElector.ConnectionState.TERMINATED;
				wantToBeInElection = false;
			}
		}

		private void reset()
		{
			state = org.apache.hadoop.ha.ActiveStandbyElector.State.INIT;
			terminateConnection();
		}

		private bool becomeActive()
		{
			System.Diagnostics.Debug.Assert(wantToBeInElection);
			if (state == org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE)
			{
				// already active
				return true;
			}
			try
			{
				org.apache.zookeeper.data.Stat oldBreadcrumbStat = fenceOldActive();
				writeBreadCrumbNode(oldBreadcrumbStat);
				LOG.debug("Becoming active for " + this);
				appClient.becomeActive();
				state = org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE;
				return true;
			}
			catch (System.Exception e)
			{
				LOG.warn("Exception handling the winning of election", e);
				// Caller will handle quitting and rejoining the election.
				return false;
			}
		}

		/// <summary>
		/// Write the "ActiveBreadCrumb" node, indicating that this node may need
		/// to be fenced on failover.
		/// </summary>
		/// <param name="oldBreadcrumbStat"></param>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private void writeBreadCrumbNode(org.apache.zookeeper.data.Stat oldBreadcrumbStat
			)
		{
			com.google.common.@base.Preconditions.checkState(appData != null, "no appdata");
			LOG.info("Writing znode " + zkBreadCrumbPath + " to indicate that the local node is the most recent active..."
				);
			if (oldBreadcrumbStat == null)
			{
				// No previous active, just create the node
				createWithRetries(zkBreadCrumbPath, appData, zkAcl, org.apache.zookeeper.CreateMode
					.PERSISTENT);
			}
			else
			{
				// There was a previous active, update the node
				setDataWithRetries(zkBreadCrumbPath, appData, oldBreadcrumbStat.getVersion());
			}
		}

		/// <summary>
		/// Try to delete the "ActiveBreadCrumb" node when gracefully giving up
		/// active status.
		/// </summary>
		/// <remarks>
		/// Try to delete the "ActiveBreadCrumb" node when gracefully giving up
		/// active status.
		/// If this fails, it will simply warn, since the graceful release behavior
		/// is only an optimization.
		/// </remarks>
		private void tryDeleteOwnBreadCrumbNode()
		{
			System.Diagnostics.Debug.Assert(state == org.apache.hadoop.ha.ActiveStandbyElector.State
				.ACTIVE);
			LOG.info("Deleting bread-crumb of active node...");
			// Sanity check the data. This shouldn't be strictly necessary,
			// but better to play it safe.
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			byte[] data = null;
			try
			{
				data = zkClient.getData(zkBreadCrumbPath, false, stat);
				if (!java.util.Arrays.equals(data, appData))
				{
					throw new System.InvalidOperationException("We thought we were active, but in fact "
						 + "the active znode had the wrong data: " + org.apache.hadoop.util.StringUtils.
						byteToHexString(data) + " (stat=" + stat + ")");
				}
				deleteWithRetries(zkBreadCrumbPath, stat.getVersion());
			}
			catch (System.Exception e)
			{
				LOG.warn("Unable to delete our own bread-crumb of being active at " + zkBreadCrumbPath
					 + ": " + e.getLocalizedMessage() + ". " + "Expecting to be fenced by the next active."
					);
			}
		}

		/// <summary>
		/// If there is a breadcrumb node indicating that another node may need
		/// fencing, try to fence that node.
		/// </summary>
		/// <returns>
		/// the Stat of the breadcrumb node that was read, or null
		/// if no breadcrumb node existed
		/// </returns>
		/// <exception cref="System.Exception"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private org.apache.zookeeper.data.Stat fenceOldActive()
		{
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			byte[] data;
			LOG.info("Checking for any old active which needs to be fenced...");
			try
			{
				data = zkDoWithRetries(new _ZKAction_887(this, stat));
			}
			catch (org.apache.zookeeper.KeeperException ke)
			{
				if (isNodeDoesNotExist(ke.code()))
				{
					LOG.info("No old node to fence");
					return null;
				}
				// If we failed to read for any other reason, then likely we lost
				// our session, or we don't have permissions, etc. In any case,
				// we probably shouldn't become active, and failing the whole
				// thing is the best bet.
				throw;
			}
			LOG.info("Old node exists: " + org.apache.hadoop.util.StringUtils.byteToHexString
				(data));
			if (java.util.Arrays.equals(data, appData))
			{
				LOG.info("But old node has our own data, so don't need to fence it.");
			}
			else
			{
				appClient.fenceOldActive(data);
			}
			return stat;
		}

		private sealed class _ZKAction_887 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<byte[]>
		{
			public _ZKAction_887(ActiveStandbyElector _enclosing, org.apache.zookeeper.data.Stat
				 stat)
			{
				this._enclosing = _enclosing;
				this.stat = stat;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public byte[] run()
			{
				return this._enclosing.zkClient.getData(this._enclosing.zkBreadCrumbPath, false, 
					stat);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly org.apache.zookeeper.data.Stat stat;
		}

		private void becomeStandby()
		{
			if (state != org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY)
			{
				LOG.debug("Becoming standby for " + this);
				state = org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY;
				appClient.becomeStandby();
			}
		}

		private void enterNeutralMode()
		{
			if (state != org.apache.hadoop.ha.ActiveStandbyElector.State.NEUTRAL)
			{
				LOG.debug("Entering neutral mode for " + this);
				state = org.apache.hadoop.ha.ActiveStandbyElector.State.NEUTRAL;
				appClient.enterNeutralMode();
			}
		}

		private void createLockNodeAsync()
		{
			zkClient.create(zkLockFilePath, appData, zkAcl, org.apache.zookeeper.CreateMode.EPHEMERAL
				, this, zkClient);
		}

		private void monitorLockNodeAsync()
		{
			if (monitorLockNodePending && monitorLockNodeClient == zkClient)
			{
				LOG.info("Ignore duplicate monitor lock-node request.");
				return;
			}
			monitorLockNodePending = true;
			monitorLockNodeClient = zkClient;
			zkClient.exists(zkLockFilePath, watcher, this, zkClient);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private string createWithRetries(string path, byte[] data, System.Collections.Generic.IList
			<org.apache.zookeeper.data.ACL> acl, org.apache.zookeeper.CreateMode mode)
		{
			return zkDoWithRetries(new _ZKAction_951(this, path, data, acl, mode));
		}

		private sealed class _ZKAction_951 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<string>
		{
			public _ZKAction_951(ActiveStandbyElector _enclosing, string path, byte[] data, System.Collections.Generic.IList
				<org.apache.zookeeper.data.ACL> acl, org.apache.zookeeper.CreateMode mode)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.data = data;
				this.acl = acl;
				this.mode = mode;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public string run()
			{
				return this._enclosing.zkClient.create(path, data, acl, mode);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly byte[] data;

			private readonly System.Collections.Generic.IList<org.apache.zookeeper.data.ACL> 
				acl;

			private readonly org.apache.zookeeper.CreateMode mode;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private byte[] getDataWithRetries(string path, bool watch, org.apache.zookeeper.data.Stat
			 stat)
		{
			return zkDoWithRetries(new _ZKAction_961(this, path, watch, stat));
		}

		private sealed class _ZKAction_961 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<byte[]>
		{
			public _ZKAction_961(ActiveStandbyElector _enclosing, string path, bool watch, org.apache.zookeeper.data.Stat
				 stat)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
				this.stat = stat;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public byte[] run()
			{
				return this._enclosing.zkClient.getData(path, watch, stat);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly bool watch;

			private readonly org.apache.zookeeper.data.Stat stat;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		private org.apache.zookeeper.data.Stat setDataWithRetries(string path, byte[] data
			, int version)
		{
			return zkDoWithRetries(new _ZKAction_971(this, path, data, version));
		}

		private sealed class _ZKAction_971 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<org.apache.zookeeper.data.Stat>
		{
			public _ZKAction_971(ActiveStandbyElector _enclosing, string path, byte[] data, int
				 version)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.data = data;
				this.version = version;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public org.apache.zookeeper.data.Stat run()
			{
				return this._enclosing.zkClient.setData(path, data, version);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly byte[] data;

			private readonly int version;
		}

		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private void deleteWithRetries(string path, int version)
		{
			zkDoWithRetries(new _ZKAction_981(this, path, version));
		}

		private sealed class _ZKAction_981 : org.apache.hadoop.ha.ActiveStandbyElector.ZKAction
			<java.lang.Void>
		{
			public _ZKAction_981(ActiveStandbyElector _enclosing, string path, int version)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.version = version;
			}

			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public java.lang.Void run()
			{
				this._enclosing.zkClient.delete(path, version);
				return null;
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly int version;
		}

		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private T zkDoWithRetries<T>(org.apache.hadoop.ha.ActiveStandbyElector.ZKAction<T
			> action)
		{
			int retry = 0;
			while (true)
			{
				try
				{
					return action.run();
				}
				catch (org.apache.zookeeper.KeeperException ke)
				{
					if (shouldRetry(ke.code()) && ++retry < maxRetryNum)
					{
						continue;
					}
					throw;
				}
			}
		}

		private interface ZKAction<T>
		{
			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			T run();
		}

		/// <summary>
		/// The callbacks and watchers pass a reference to the ZK client
		/// which made the original call.
		/// </summary>
		/// <remarks>
		/// The callbacks and watchers pass a reference to the ZK client
		/// which made the original call. We don't want to take action
		/// based on any callbacks from prior clients after we quit
		/// the election.
		/// </remarks>
		/// <param name="ctx">the ZK client passed into the watcher</param>
		/// <returns>true if it matches the current client</returns>
		private bool isStaleClient(object ctx)
		{
			lock (this)
			{
				com.google.common.@base.Preconditions.checkNotNull(ctx);
				if (zkClient != (org.apache.zookeeper.ZooKeeper)ctx)
				{
					LOG.warn("Ignoring stale result from old client with sessionId " + string.format(
						"0x%08x", ((org.apache.zookeeper.ZooKeeper)ctx).getSessionId()));
					return true;
				}
				return false;
			}
		}

		/// <summary>
		/// Watcher implementation which keeps a reference around to the
		/// original ZK connection, and passes it back along with any
		/// events.
		/// </summary>
		private sealed class WatcherWithClientRef : org.apache.zookeeper.Watcher
		{
			private org.apache.zookeeper.ZooKeeper zk;

			/// <summary>Latch fired whenever any event arrives.</summary>
			/// <remarks>
			/// Latch fired whenever any event arrives. This is used in order
			/// to wait for the Connected event when the client is first created.
			/// </remarks>
			private java.util.concurrent.CountDownLatch hasReceivedEvent = new java.util.concurrent.CountDownLatch
				(1);

			/// <summary>Latch used to wait until the reference to ZooKeeper is set.</summary>
			private java.util.concurrent.CountDownLatch hasSetZooKeeper = new java.util.concurrent.CountDownLatch
				(1);

			/// <summary>Waits for the next event from ZooKeeper to arrive.</summary>
			/// <param name="connectionTimeoutMs">zookeeper connection timeout in milliseconds</param>
			/// <exception cref="org.apache.zookeeper.KeeperException">
			/// if the connection attempt times out. This will
			/// be a ZooKeeper ConnectionLoss exception code.
			/// </exception>
			/// <exception cref="System.IO.IOException">if interrupted while connecting to ZooKeeper
			/// 	</exception>
			private void waitForZKConnectionEvent(int connectionTimeoutMs)
			{
				try
				{
					if (!this.hasReceivedEvent.await(connectionTimeoutMs, java.util.concurrent.TimeUnit
						.MILLISECONDS))
					{
						org.apache.hadoop.ha.ActiveStandbyElector.LOG.error("Connection timed out: couldn't connect to ZooKeeper in "
							 + connectionTimeoutMs + " milliseconds");
						this.zk.close();
						throw org.apache.zookeeper.KeeperException.create(org.apache.zookeeper.KeeperException.Code
							.CONNECTIONLOSS);
					}
				}
				catch (System.Exception e)
				{
					java.lang.Thread.currentThread().interrupt();
					throw new System.IO.IOException("Interrupted when connecting to zookeeper server"
						, e);
				}
			}

			private void setZooKeeperRef(org.apache.zookeeper.ZooKeeper zk)
			{
				com.google.common.@base.Preconditions.checkState(this.zk == null, "zk already set -- must be set exactly once"
					);
				this.zk = zk;
				this.hasSetZooKeeper.countDown();
			}

			public override void process(org.apache.zookeeper.WatchedEvent @event)
			{
				this.hasReceivedEvent.countDown();
				try
				{
					if (!this.hasSetZooKeeper.await(this._enclosing.zkSessionTimeout, java.util.concurrent.TimeUnit
						.MILLISECONDS))
					{
						org.apache.hadoop.ha.ActiveStandbyElector.LOG.debug("Event received with stale zk"
							);
					}
					this._enclosing.processWatchEvent(this.zk, @event);
				}
				catch (System.Exception t)
				{
					this._enclosing.fatalError("Failed to process watcher event " + @event + ": " + org.apache.hadoop.util.StringUtils
						.stringifyException(t));
				}
			}

			internal WatcherWithClientRef(ActiveStandbyElector _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ActiveStandbyElector _enclosing;
		}

		private static bool isSuccess(org.apache.zookeeper.KeeperException.Code code)
		{
			return (code == org.apache.zookeeper.KeeperException.Code.OK);
		}

		private static bool isNodeExists(org.apache.zookeeper.KeeperException.Code code)
		{
			return (code == org.apache.zookeeper.KeeperException.Code.NODEEXISTS);
		}

		private static bool isNodeDoesNotExist(org.apache.zookeeper.KeeperException.Code 
			code)
		{
			return (code == org.apache.zookeeper.KeeperException.Code.NONODE);
		}

		private static bool isSessionExpired(org.apache.zookeeper.KeeperException.Code code
			)
		{
			return (code == org.apache.zookeeper.KeeperException.Code.SESSIONEXPIRED);
		}

		private static bool shouldRetry(org.apache.zookeeper.KeeperException.Code code)
		{
			return code == org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS || code ==
				 org.apache.zookeeper.KeeperException.Code.OPERATIONTIMEOUT;
		}

		public override string ToString()
		{
			return "elector id=" + Sharpen.Runtime.identityHashCode(this) + " appData=" + ((appData
				 == null) ? "null" : org.apache.hadoop.util.StringUtils.byteToHexString(appData)
				) + " cb=" + appClient;
		}

		public virtual string getHAZookeeperConnectionState()
		{
			return this.zkConnectionState.ToString();
		}
	}
}
