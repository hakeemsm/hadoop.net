using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;


namespace Org.Apache.Hadoop.HA
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
	public class ActiveStandbyElector : AsyncCallback.StatCallback, AsyncCallback.StringCallback
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
			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			void BecomeActive();

			/// <summary>This method is called when the app becomes a standby</summary>
			void BecomeStandby();

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
			void EnterNeutralMode();

			/// <summary>If there is any fatal error (e.g.</summary>
			/// <remarks>
			/// If there is any fatal error (e.g. wrong ACL's, unexpected Zookeeper
			/// errors or Zookeeper persistent unavailability) then notifyFatalError is
			/// called to notify the app about it.
			/// </remarks>
			void NotifyFatalError(string errorMessage);

			/// <summary>
			/// If an old active has failed, rather than exited gracefully, then
			/// the new active may need to take some fencing actions against it
			/// before proceeding with failover.
			/// </summary>
			/// <param name="oldActiveData">the application data provided by the prior active</param>
			void FenceOldActive(byte[] oldActiveData);
		}

		/// <summary>Name of the lock znode used by the library.</summary>
		/// <remarks>
		/// Name of the lock znode used by the library. Protected for access in test
		/// classes
		/// </remarks>
		[VisibleForTesting]
		protected internal const string LockFilename = "ActiveStandbyElectorLock";

		[VisibleForTesting]
		protected internal const string BreadcrumbFilename = "ActiveBreadCrumb";

		public static readonly Log Log = LogFactory.GetLog(typeof(ActiveStandbyElector));

		private const int SleepAfterFailureToBecomeActive = 1000;

		private enum ConnectionState
		{
			Disconnected,
			Connected,
			Terminated
		}

		internal enum State
		{
			Init,
			Active,
			Standby,
			Neutral
		}

		private ActiveStandbyElector.State state = ActiveStandbyElector.State.Init;

		private int createRetryCount = 0;

		private int statRetryCount = 0;

		private ZooKeeper zkClient;

		private ActiveStandbyElector.WatcherWithClientRef watcher;

		private ActiveStandbyElector.ConnectionState zkConnectionState = ActiveStandbyElector.ConnectionState
			.Terminated;

		private readonly ActiveStandbyElector.ActiveStandbyElectorCallback appClient;

		private readonly string zkHostPort;

		private readonly int zkSessionTimeout;

		private readonly IList<ACL> zkAcl;

		private readonly IList<ZKUtil.ZKAuthInfo> zkAuthInfo;

		private byte[] appData;

		private readonly string zkLockFilePath;

		private readonly string zkBreadCrumbPath;

		private readonly string znodeWorkingDir;

		private readonly int maxRetryNum;

		private Lock sessionReestablishLockForTests = new ReentrantLock();

		private bool wantToBeInElection;

		private bool monitorLockNodePending = false;

		private ZooKeeper monitorLockNodeClient;

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
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		public ActiveStandbyElector(string zookeeperHostPorts, int zookeeperSessionTimeout
			, string parentZnodeName, IList<ACL> acl, IList<ZKUtil.ZKAuthInfo> authInfo, ActiveStandbyElector.ActiveStandbyElectorCallback
			 app, int maxRetryNum)
		{
			if (app == null || acl == null || parentZnodeName == null || zookeeperHostPorts ==
				 null || zookeeperSessionTimeout <= 0)
			{
				throw new HadoopIllegalArgumentException("Invalid argument");
			}
			zkHostPort = zookeeperHostPorts;
			zkSessionTimeout = zookeeperSessionTimeout;
			zkAcl = acl;
			zkAuthInfo = authInfo;
			appClient = app;
			znodeWorkingDir = parentZnodeName;
			zkLockFilePath = znodeWorkingDir + "/" + LockFilename;
			zkBreadCrumbPath = znodeWorkingDir + "/" + BreadcrumbFilename;
			this.maxRetryNum = maxRetryNum;
			// createConnection for future API calls
			CreateConnection();
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
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">if valid data is not supplied
		/// 	</exception>
		public virtual void JoinElection(byte[] data)
		{
			lock (this)
			{
				if (data == null)
				{
					throw new HadoopIllegalArgumentException("data cannot be null");
				}
				if (wantToBeInElection)
				{
					Log.Info("Already in election. Not re-connecting.");
					return;
				}
				appData = new byte[data.Length];
				System.Array.Copy(data, 0, appData, 0, data.Length);
				Log.Debug("Attempting active election for " + this);
				JoinElectionInternal();
			}
		}

		/// <returns>true if the configured parent znode exists</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool ParentZNodeExists()
		{
			lock (this)
			{
				Preconditions.CheckState(zkClient != null);
				try
				{
					return zkClient.Exists(znodeWorkingDir, false) != null;
				}
				catch (KeeperException e)
				{
					throw new IOException("Couldn't determine existence of znode '" + znodeWorkingDir
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
		public virtual void EnsureParentZNode()
		{
			lock (this)
			{
				Preconditions.CheckState(!wantToBeInElection, "ensureParentZNode() may not be called while in the election"
					);
				string[] pathParts = znodeWorkingDir.Split("/");
				Preconditions.CheckArgument(pathParts.Length >= 1 && pathParts[0].IsEmpty(), "Invalid path: %s"
					, znodeWorkingDir);
				StringBuilder sb = new StringBuilder();
				for (int i = 1; i < pathParts.Length; i++)
				{
					sb.Append("/").Append(pathParts[i]);
					string prefixPath = sb.ToString();
					Log.Debug("Ensuring existence of " + prefixPath);
					try
					{
						CreateWithRetries(prefixPath, new byte[] {  }, zkAcl, CreateMode.Persistent);
					}
					catch (KeeperException e)
					{
						if (IsNodeExists(e.Code()))
						{
							// This is OK - just ensuring existence.
							continue;
						}
						else
						{
							throw new IOException("Couldn't create " + prefixPath, e);
						}
					}
				}
				Log.Info("Successfully created " + znodeWorkingDir + " in ZK.");
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
		public virtual void ClearParentZNode()
		{
			lock (this)
			{
				Preconditions.CheckState(!wantToBeInElection, "clearParentZNode() may not be called while in the election"
					);
				try
				{
					Log.Info("Recursively deleting " + znodeWorkingDir + " from ZK...");
					ZkDoWithRetries(new _ZKAction_327(this));
				}
				catch (KeeperException e)
				{
					throw new IOException("Couldn't clear parent znode " + znodeWorkingDir, e);
				}
				Log.Info("Successfully deleted " + znodeWorkingDir + " from ZK.");
			}
		}

		private sealed class _ZKAction_327 : ActiveStandbyElector.ZKAction<Void>
		{
			public _ZKAction_327(ActiveStandbyElector _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				ZKUtil.DeleteRecursive(this._enclosing.zkClient, this._enclosing.znodeWorkingDir);
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
		public virtual void QuitElection(bool needFence)
		{
			lock (this)
			{
				Log.Info("Yielding from election");
				if (!needFence && state == ActiveStandbyElector.State.Active)
				{
					// If active is gracefully going back to standby mode, remove
					// our permanent znode so no one fences us.
					TryDeleteOwnBreadCrumbNode();
				}
				Reset();
				wantToBeInElection = false;
			}
		}

		/// <summary>Exception thrown when there is no active leader</summary>
		[System.Serializable]
		public class ActiveNotFoundException : Exception
		{
			private const long serialVersionUID = 3505396722342846462L;
		}

		/// <summary>get data set by the active leader</summary>
		/// <returns>data set by the active instance</returns>
		/// <exception cref="ActiveNotFoundException">when there is no active leader</exception>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException">other zookeeper operation errors
		/// 	</exception>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException">when ZooKeeper connection could not be established
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.ActiveStandbyElector.ActiveNotFoundException
		/// 	"/>
		public virtual byte[] GetActiveData()
		{
			lock (this)
			{
				try
				{
					if (zkClient == null)
					{
						CreateConnection();
					}
					Stat stat = new Stat();
					return GetDataWithRetries(zkLockFilePath, false, stat);
				}
				catch (KeeperException e)
				{
					KeeperException.Code code = e.Code();
					if (IsNodeDoesNotExist(code))
					{
						// handle the commonly expected cases that make sense for us
						throw new ActiveStandbyElector.ActiveNotFoundException();
					}
					else
					{
						throw;
					}
				}
			}
		}

		/// <summary>interface implementation of Zookeeper callback for create</summary>
		public virtual void ProcessResult(int rc, string path, object ctx, string name)
		{
			lock (this)
			{
				if (IsStaleClient(ctx))
				{
					return;
				}
				Log.Debug("CreateNode result: " + rc + " for path: " + path + " connectionState: "
					 + zkConnectionState + "  for " + this);
				KeeperException.Code code = KeeperException.Code.Get(rc);
				if (IsSuccess(code))
				{
					// we successfully created the znode. we are the leader. start monitoring
					if (BecomeActive())
					{
						MonitorActiveStatus();
					}
					else
					{
						ReJoinElectionAfterFailureToBecomeActive();
					}
					return;
				}
				if (IsNodeExists(code))
				{
					if (createRetryCount == 0)
					{
						// znode exists and we did not retry the operation. so a different
						// instance has created it. become standby and monitor lock.
						BecomeStandby();
					}
					// if we had retried then the znode could have been created by our first
					// attempt to the server (that we lost) and this node exists response is
					// for the second attempt. verify this case via ephemeral node owner. this
					// will happen on the callback for monitoring the lock.
					MonitorActiveStatus();
					return;
				}
				string errorMessage = "Received create error from Zookeeper. code:" + code.ToString
					() + " for path " + path;
				Log.Debug(errorMessage);
				if (ShouldRetry(code))
				{
					if (createRetryCount < maxRetryNum)
					{
						Log.Debug("Retrying createNode createRetryCount: " + createRetryCount);
						++createRetryCount;
						CreateLockNodeAsync();
						return;
					}
					errorMessage = errorMessage + ". Not retrying further znode create connection errors.";
				}
				else
				{
					if (IsSessionExpired(code))
					{
						// This isn't fatal - the client Watcher will re-join the election
						Log.Warn("Lock acquisition failed because session was lost");
						return;
					}
				}
				FatalError(errorMessage);
			}
		}

		/// <summary>interface implementation of Zookeeper callback for monitor (exists)</summary>
		public virtual void ProcessResult(int rc, string path, object ctx, Stat stat)
		{
			lock (this)
			{
				if (IsStaleClient(ctx))
				{
					return;
				}
				monitorLockNodePending = false;
				System.Diagnostics.Debug.Assert(wantToBeInElection, "Got a StatNode result after quitting election"
					);
				Log.Debug("StatNode result: " + rc + " for path: " + path + " connectionState: " 
					+ zkConnectionState + " for " + this);
				KeeperException.Code code = KeeperException.Code.Get(rc);
				if (IsSuccess(code))
				{
					// the following owner check completes verification in case the lock znode
					// creation was retried
					if (stat.GetEphemeralOwner() == zkClient.GetSessionId())
					{
						// we own the lock znode. so we are the leader
						if (!BecomeActive())
						{
							ReJoinElectionAfterFailureToBecomeActive();
						}
					}
					else
					{
						// we dont own the lock znode. so we are a standby.
						BecomeStandby();
					}
					// the watch set by us will notify about changes
					return;
				}
				if (IsNodeDoesNotExist(code))
				{
					// the lock znode disappeared before we started monitoring it
					EnterNeutralMode();
					JoinElectionInternal();
					return;
				}
				string errorMessage = "Received stat error from Zookeeper. code:" + code.ToString
					();
				Log.Debug(errorMessage);
				if (ShouldRetry(code))
				{
					if (statRetryCount < maxRetryNum)
					{
						++statRetryCount;
						MonitorLockNodeAsync();
						return;
					}
					errorMessage = errorMessage + ". Not retrying further znode monitoring connection errors.";
				}
				else
				{
					if (IsSessionExpired(code))
					{
						// This isn't fatal - the client Watcher will re-join the election
						Log.Warn("Lock monitoring failed because session was lost");
						return;
					}
				}
				FatalError(errorMessage);
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
		private void ReJoinElectionAfterFailureToBecomeActive()
		{
			ReJoinElection(SleepAfterFailureToBecomeActive);
		}

		/// <summary>
		/// interface implementation of Zookeeper watch events (connection and node),
		/// proxied by
		/// <see cref="WatcherWithClientRef"/>
		/// .
		/// </summary>
		internal virtual void ProcessWatchEvent(ZooKeeper zk, WatchedEvent @event)
		{
			lock (this)
			{
				Watcher.Event.EventType eventType = @event.GetType();
				if (IsStaleClient(zk))
				{
					return;
				}
				Log.Debug("Watcher event type: " + eventType + " with state:" + @event.GetState()
					 + " for path:" + @event.GetPath() + " connectionState: " + zkConnectionState + 
					" for " + this);
				if (eventType == Watcher.Event.EventType.None)
				{
					switch (@event.GetState())
					{
						case Watcher.Event.KeeperState.SyncConnected:
						{
							// the connection state has changed
							Log.Info("Session connected.");
							// if the listener was asked to move to safe state then it needs to
							// be undone
							ActiveStandbyElector.ConnectionState prevConnectionState = zkConnectionState;
							zkConnectionState = ActiveStandbyElector.ConnectionState.Connected;
							if (prevConnectionState == ActiveStandbyElector.ConnectionState.Disconnected && wantToBeInElection)
							{
								MonitorActiveStatus();
							}
							break;
						}

						case Watcher.Event.KeeperState.Disconnected:
						{
							Log.Info("Session disconnected. Entering neutral mode...");
							// ask the app to move to safe state because zookeeper connection
							// is not active and we dont know our state
							zkConnectionState = ActiveStandbyElector.ConnectionState.Disconnected;
							EnterNeutralMode();
							break;
						}

						case Watcher.Event.KeeperState.Expired:
						{
							// the connection got terminated because of session timeout
							// call listener to reconnect
							Log.Info("Session expired. Entering neutral mode and rejoining...");
							EnterNeutralMode();
							ReJoinElection(0);
							break;
						}

						case Watcher.Event.KeeperState.SaslAuthenticated:
						{
							Log.Info("Successfully authenticated to ZooKeeper using SASL.");
							break;
						}

						default:
						{
							FatalError("Unexpected Zookeeper watch event state: " + @event.GetState());
							break;
						}
					}
					return;
				}
				// a watch on lock path in zookeeper has fired. so something has changed on
				// the lock. ideally we should check that the path is the same as the lock
				// path but trusting zookeeper for now
				string path = @event.GetPath();
				if (path != null)
				{
					switch (eventType)
					{
						case Watcher.Event.EventType.NodeDeleted:
						{
							if (state == ActiveStandbyElector.State.Active)
							{
								EnterNeutralMode();
							}
							JoinElectionInternal();
							break;
						}

						case Watcher.Event.EventType.NodeDataChanged:
						{
							MonitorActiveStatus();
							break;
						}

						default:
						{
							Log.Debug("Unexpected node event: " + eventType + " for path: " + path);
							MonitorActiveStatus();
							break;
						}
					}
					return;
				}
				// some unexpected error has occurred
				FatalError("Unexpected watch error from Zookeeper");
			}
		}

		/// <summary>Get a new zookeeper client instance.</summary>
		/// <remarks>
		/// Get a new zookeeper client instance. protected so that test class can
		/// inherit and pass in a mock object for zookeeper
		/// </remarks>
		/// <returns>new zookeeper client instance</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException">zookeeper connectionloss exception
		/// 	</exception>
		protected internal virtual ZooKeeper GetNewZooKeeper()
		{
			lock (this)
			{
				// Unfortunately, the ZooKeeper constructor connects to ZooKeeper and
				// may trigger the Connected event immediately. So, if we register the
				// watcher after constructing ZooKeeper, we may miss that event. Instead,
				// we construct the watcher first, and have it block any events it receives
				// before we can set its ZooKeeper reference.
				watcher = new ActiveStandbyElector.WatcherWithClientRef(this);
				ZooKeeper zk = new ZooKeeper(zkHostPort, zkSessionTimeout, watcher);
				watcher.SetZooKeeperRef(zk);
				// Wait for the asynchronous success/failure. This may throw an exception
				// if we don't connect within the session timeout.
				watcher.WaitForZKConnectionEvent(zkSessionTimeout);
				foreach (ZKUtil.ZKAuthInfo auth in zkAuthInfo)
				{
					zk.AddAuthInfo(auth.GetScheme(), auth.GetAuth());
				}
				return zk;
			}
		}

		private void FatalError(string errorMessage)
		{
			Log.Fatal(errorMessage);
			Reset();
			appClient.NotifyFatalError(errorMessage);
		}

		private void MonitorActiveStatus()
		{
			System.Diagnostics.Debug.Assert(wantToBeInElection);
			Log.Debug("Monitoring active leader for " + this);
			statRetryCount = 0;
			MonitorLockNodeAsync();
		}

		private void JoinElectionInternal()
		{
			Preconditions.CheckState(appData != null, "trying to join election without any app data"
				);
			if (zkClient == null)
			{
				if (!ReEstablishSession())
				{
					FatalError("Failed to reEstablish connection with ZooKeeper");
					return;
				}
			}
			createRetryCount = 0;
			wantToBeInElection = true;
			CreateLockNodeAsync();
		}

		private void ReJoinElection(int sleepTime)
		{
			Log.Info("Trying to re-establish ZK session");
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
				TerminateConnection();
				SleepFor(sleepTime);
				// Should not join election even before the SERVICE is reported
				// as HEALTHY from ZKFC monitoring.
				if (appData != null)
				{
					JoinElectionInternal();
				}
				else
				{
					Log.Info("Not joining election since service has not yet been " + "reported as healthy."
						);
				}
			}
			finally
			{
				sessionReestablishLockForTests.Unlock();
			}
		}

		/// <summary>Sleep for the given number of milliseconds.</summary>
		/// <remarks>
		/// Sleep for the given number of milliseconds.
		/// This is non-static, and separated out, so that unit tests
		/// can override the behavior not to sleep.
		/// </remarks>
		[VisibleForTesting]
		protected internal virtual void SleepFor(int sleepMs)
		{
			if (sleepMs > 0)
			{
				try
				{
					Thread.Sleep(sleepMs);
				}
				catch (Exception)
				{
					Thread.CurrentThread().Interrupt();
				}
			}
		}

		[VisibleForTesting]
		internal virtual void PreventSessionReestablishmentForTests()
		{
			sessionReestablishLockForTests.Lock();
		}

		[VisibleForTesting]
		internal virtual void AllowSessionReestablishmentForTests()
		{
			sessionReestablishLockForTests.Unlock();
		}

		[VisibleForTesting]
		internal virtual long GetZKSessionIdForTests()
		{
			lock (this)
			{
				if (zkClient != null)
				{
					return zkClient.GetSessionId();
				}
				else
				{
					return -1;
				}
			}
		}

		[VisibleForTesting]
		internal virtual ActiveStandbyElector.State GetStateForTests()
		{
			lock (this)
			{
				return state;
			}
		}

		[VisibleForTesting]
		internal virtual bool IsMonitorLockNodePending()
		{
			lock (this)
			{
				return monitorLockNodePending;
			}
		}

		private bool ReEstablishSession()
		{
			int connectionRetryCount = 0;
			bool success = false;
			while (!success && connectionRetryCount < maxRetryNum)
			{
				Log.Debug("Establishing zookeeper connection for " + this);
				try
				{
					CreateConnection();
					success = true;
				}
				catch (IOException e)
				{
					Log.Warn(e);
					SleepFor(5000);
				}
				catch (KeeperException e)
				{
					Log.Warn(e);
					SleepFor(5000);
				}
				++connectionRetryCount;
			}
			return success;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private void CreateConnection()
		{
			if (zkClient != null)
			{
				try
				{
					zkClient.Close();
				}
				catch (Exception e)
				{
					throw new IOException("Interrupted while closing ZK", e);
				}
				zkClient = null;
				watcher = null;
			}
			zkClient = GetNewZooKeeper();
			Log.Debug("Created new connection for " + this);
		}

		[InterfaceAudience.Private]
		public virtual void TerminateConnection()
		{
			lock (this)
			{
				if (zkClient == null)
				{
					return;
				}
				Log.Debug("Terminating ZK connection for " + this);
				ZooKeeper tempZk = zkClient;
				zkClient = null;
				watcher = null;
				try
				{
					tempZk.Close();
				}
				catch (Exception e)
				{
					Log.Warn(e);
				}
				zkConnectionState = ActiveStandbyElector.ConnectionState.Terminated;
				wantToBeInElection = false;
			}
		}

		private void Reset()
		{
			state = ActiveStandbyElector.State.Init;
			TerminateConnection();
		}

		private bool BecomeActive()
		{
			System.Diagnostics.Debug.Assert(wantToBeInElection);
			if (state == ActiveStandbyElector.State.Active)
			{
				// already active
				return true;
			}
			try
			{
				Stat oldBreadcrumbStat = FenceOldActive();
				WriteBreadCrumbNode(oldBreadcrumbStat);
				Log.Debug("Becoming active for " + this);
				appClient.BecomeActive();
				state = ActiveStandbyElector.State.Active;
				return true;
			}
			catch (Exception e)
			{
				Log.Warn("Exception handling the winning of election", e);
				// Caller will handle quitting and rejoining the election.
				return false;
			}
		}

		/// <summary>
		/// Write the "ActiveBreadCrumb" node, indicating that this node may need
		/// to be fenced on failover.
		/// </summary>
		/// <param name="oldBreadcrumbStat"></param>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private void WriteBreadCrumbNode(Stat oldBreadcrumbStat)
		{
			Preconditions.CheckState(appData != null, "no appdata");
			Log.Info("Writing znode " + zkBreadCrumbPath + " to indicate that the local node is the most recent active..."
				);
			if (oldBreadcrumbStat == null)
			{
				// No previous active, just create the node
				CreateWithRetries(zkBreadCrumbPath, appData, zkAcl, CreateMode.Persistent);
			}
			else
			{
				// There was a previous active, update the node
				SetDataWithRetries(zkBreadCrumbPath, appData, oldBreadcrumbStat.GetVersion());
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
		private void TryDeleteOwnBreadCrumbNode()
		{
			System.Diagnostics.Debug.Assert(state == ActiveStandbyElector.State.Active);
			Log.Info("Deleting bread-crumb of active node...");
			// Sanity check the data. This shouldn't be strictly necessary,
			// but better to play it safe.
			Stat stat = new Stat();
			byte[] data = null;
			try
			{
				data = zkClient.GetData(zkBreadCrumbPath, false, stat);
				if (!Arrays.Equals(data, appData))
				{
					throw new InvalidOperationException("We thought we were active, but in fact " + "the active znode had the wrong data: "
						 + StringUtils.ByteToHexString(data) + " (stat=" + stat + ")");
				}
				DeleteWithRetries(zkBreadCrumbPath, stat.GetVersion());
			}
			catch (Exception e)
			{
				Log.Warn("Unable to delete our own bread-crumb of being active at " + zkBreadCrumbPath
					 + ": " + e.GetLocalizedMessage() + ". " + "Expecting to be fenced by the next active."
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
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private Stat FenceOldActive()
		{
			Stat stat = new Stat();
			byte[] data;
			Log.Info("Checking for any old active which needs to be fenced...");
			try
			{
				data = ZkDoWithRetries(new _ZKAction_887(this, stat));
			}
			catch (KeeperException ke)
			{
				if (IsNodeDoesNotExist(ke.Code()))
				{
					Log.Info("No old node to fence");
					return null;
				}
				// If we failed to read for any other reason, then likely we lost
				// our session, or we don't have permissions, etc. In any case,
				// we probably shouldn't become active, and failing the whole
				// thing is the best bet.
				throw;
			}
			Log.Info("Old node exists: " + StringUtils.ByteToHexString(data));
			if (Arrays.Equals(data, appData))
			{
				Log.Info("But old node has our own data, so don't need to fence it.");
			}
			else
			{
				appClient.FenceOldActive(data);
			}
			return stat;
		}

		private sealed class _ZKAction_887 : ActiveStandbyElector.ZKAction<byte[]>
		{
			public _ZKAction_887(ActiveStandbyElector _enclosing, Stat stat)
			{
				this._enclosing = _enclosing;
				this.stat = stat;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public byte[] Run()
			{
				return this._enclosing.zkClient.GetData(this._enclosing.zkBreadCrumbPath, false, 
					stat);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly Stat stat;
		}

		private void BecomeStandby()
		{
			if (state != ActiveStandbyElector.State.Standby)
			{
				Log.Debug("Becoming standby for " + this);
				state = ActiveStandbyElector.State.Standby;
				appClient.BecomeStandby();
			}
		}

		private void EnterNeutralMode()
		{
			if (state != ActiveStandbyElector.State.Neutral)
			{
				Log.Debug("Entering neutral mode for " + this);
				state = ActiveStandbyElector.State.Neutral;
				appClient.EnterNeutralMode();
			}
		}

		private void CreateLockNodeAsync()
		{
			zkClient.Create(zkLockFilePath, appData, zkAcl, CreateMode.Ephemeral, this, zkClient
				);
		}

		private void MonitorLockNodeAsync()
		{
			if (monitorLockNodePending && monitorLockNodeClient == zkClient)
			{
				Log.Info("Ignore duplicate monitor lock-node request.");
				return;
			}
			monitorLockNodePending = true;
			monitorLockNodeClient = zkClient;
			zkClient.Exists(zkLockFilePath, watcher, this, zkClient);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private string CreateWithRetries(string path, byte[] data, IList<ACL> acl, CreateMode
			 mode)
		{
			return ZkDoWithRetries(new _ZKAction_951(this, path, data, acl, mode));
		}

		private sealed class _ZKAction_951 : ActiveStandbyElector.ZKAction<string>
		{
			public _ZKAction_951(ActiveStandbyElector _enclosing, string path, byte[] data, IList
				<ACL> acl, CreateMode mode)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.data = data;
				this.acl = acl;
				this.mode = mode;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public string Run()
			{
				return this._enclosing.zkClient.Create(path, data, acl, mode);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly byte[] data;

			private readonly IList<ACL> acl;

			private readonly CreateMode mode;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private byte[] GetDataWithRetries(string path, bool watch, Stat stat)
		{
			return ZkDoWithRetries(new _ZKAction_961(this, path, watch, stat));
		}

		private sealed class _ZKAction_961 : ActiveStandbyElector.ZKAction<byte[]>
		{
			public _ZKAction_961(ActiveStandbyElector _enclosing, string path, bool watch, Stat
				 stat)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
				this.stat = stat;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public byte[] Run()
			{
				return this._enclosing.zkClient.GetData(path, watch, stat);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly bool watch;

			private readonly Stat stat;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		private Stat SetDataWithRetries(string path, byte[] data, int version)
		{
			return ZkDoWithRetries(new _ZKAction_971(this, path, data, version));
		}

		private sealed class _ZKAction_971 : ActiveStandbyElector.ZKAction<Stat>
		{
			public _ZKAction_971(ActiveStandbyElector _enclosing, string path, byte[] data, int
				 version)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.data = data;
				this.version = version;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public Stat Run()
			{
				return this._enclosing.zkClient.SetData(path, data, version);
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly byte[] data;

			private readonly int version;
		}

		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private void DeleteWithRetries(string path, int version)
		{
			ZkDoWithRetries(new _ZKAction_981(this, path, version));
		}

		private sealed class _ZKAction_981 : ActiveStandbyElector.ZKAction<Void>
		{
			public _ZKAction_981(ActiveStandbyElector _enclosing, string path, int version)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.version = version;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.zkClient.Delete(path, version);
				return null;
			}

			private readonly ActiveStandbyElector _enclosing;

			private readonly string path;

			private readonly int version;
		}

		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private T ZkDoWithRetries<T>(ActiveStandbyElector.ZKAction<T> action)
		{
			int retry = 0;
			while (true)
			{
				try
				{
					return action.Run();
				}
				catch (KeeperException ke)
				{
					if (ShouldRetry(ke.Code()) && ++retry < maxRetryNum)
					{
						continue;
					}
					throw;
				}
			}
		}

		private interface ZKAction<T>
		{
			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			T Run();
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
		private bool IsStaleClient(object ctx)
		{
			lock (this)
			{
				Preconditions.CheckNotNull(ctx);
				if (zkClient != (ZooKeeper)ctx)
				{
					Log.Warn("Ignoring stale result from old client with sessionId " + string.Format(
						"0x%08x", ((ZooKeeper)ctx).GetSessionId()));
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
		private sealed class WatcherWithClientRef : Watcher
		{
			private ZooKeeper zk;

			/// <summary>Latch fired whenever any event arrives.</summary>
			/// <remarks>
			/// Latch fired whenever any event arrives. This is used in order
			/// to wait for the Connected event when the client is first created.
			/// </remarks>
			private CountDownLatch hasReceivedEvent = new CountDownLatch(1);

			/// <summary>Latch used to wait until the reference to ZooKeeper is set.</summary>
			private CountDownLatch hasSetZooKeeper = new CountDownLatch(1);

			/// <summary>Waits for the next event from ZooKeeper to arrive.</summary>
			/// <param name="connectionTimeoutMs">zookeeper connection timeout in milliseconds</param>
			/// <exception cref="Org.Apache.Zookeeper.KeeperException">
			/// if the connection attempt times out. This will
			/// be a ZooKeeper ConnectionLoss exception code.
			/// </exception>
			/// <exception cref="System.IO.IOException">if interrupted while connecting to ZooKeeper
			/// 	</exception>
			private void WaitForZKConnectionEvent(int connectionTimeoutMs)
			{
				try
				{
					if (!this.hasReceivedEvent.Await(connectionTimeoutMs, TimeUnit.Milliseconds))
					{
						ActiveStandbyElector.Log.Error("Connection timed out: couldn't connect to ZooKeeper in "
							 + connectionTimeoutMs + " milliseconds");
						this.zk.Close();
						throw KeeperException.Create(KeeperException.Code.Connectionloss);
					}
				}
				catch (Exception e)
				{
					Thread.CurrentThread().Interrupt();
					throw new IOException("Interrupted when connecting to zookeeper server", e);
				}
			}

			private void SetZooKeeperRef(ZooKeeper zk)
			{
				Preconditions.CheckState(this.zk == null, "zk already set -- must be set exactly once"
					);
				this.zk = zk;
				this.hasSetZooKeeper.CountDown();
			}

			public override void Process(WatchedEvent @event)
			{
				this.hasReceivedEvent.CountDown();
				try
				{
					if (!this.hasSetZooKeeper.Await(this._enclosing.zkSessionTimeout, TimeUnit.Milliseconds
						))
					{
						ActiveStandbyElector.Log.Debug("Event received with stale zk");
					}
					this._enclosing.ProcessWatchEvent(this.zk, @event);
				}
				catch (Exception t)
				{
					this._enclosing.FatalError("Failed to process watcher event " + @event + ": " + StringUtils
						.StringifyException(t));
				}
			}

			internal WatcherWithClientRef(ActiveStandbyElector _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ActiveStandbyElector _enclosing;
		}

		private static bool IsSuccess(KeeperException.Code code)
		{
			return (code == KeeperException.Code.Ok);
		}

		private static bool IsNodeExists(KeeperException.Code code)
		{
			return (code == KeeperException.Code.Nodeexists);
		}

		private static bool IsNodeDoesNotExist(KeeperException.Code code)
		{
			return (code == KeeperException.Code.Nonode);
		}

		private static bool IsSessionExpired(KeeperException.Code code)
		{
			return (code == KeeperException.Code.Sessionexpired);
		}

		private static bool ShouldRetry(KeeperException.Code code)
		{
			return code == KeeperException.Code.Connectionloss || code == KeeperException.Code
				.Operationtimeout;
		}

		public override string ToString()
		{
			return "elector id=" + Runtime.IdentityHashCode(this) + " appData=" + ((appData ==
				 null) ? "null" : StringUtils.ByteToHexString(appData)) + " cb=" + appClient;
		}

		public virtual string GetHAZookeeperConnectionState()
		{
			return this.zkConnectionState.ToString();
		}
	}
}
