using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// This class is a daemon which runs in a loop, periodically heartbeating
	/// with an HA service.
	/// </summary>
	/// <remarks>
	/// This class is a daemon which runs in a loop, periodically heartbeating
	/// with an HA service. It is responsible for keeping track of that service's
	/// health and exposing callbacks to the failover controller when the health
	/// status changes.
	/// Classes which need callbacks should implement the
	/// <see cref="Callback"/>
	/// interface.
	/// </remarks>
	public class HealthMonitor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.HealthMonitor
			)));

		private org.apache.hadoop.util.Daemon daemon;

		private long connectRetryInterval;

		private long checkIntervalMillis;

		private long sleepAfterDisconnectMillis;

		private int rpcTimeout;

		private volatile bool shouldRun = true;

		/// <summary>The connected proxy</summary>
		private org.apache.hadoop.ha.HAServiceProtocol proxy;

		/// <summary>The HA service to monitor</summary>
		private readonly org.apache.hadoop.ha.HAServiceTarget targetToMonitor;

		private readonly org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.ha.HealthMonitor.State state = org.apache.hadoop.ha.HealthMonitor.State
			.INITIALIZING;

		/// <summary>Listeners for state changes</summary>
		private System.Collections.Generic.IList<org.apache.hadoop.ha.HealthMonitor.Callback
			> callbacks = java.util.Collections.synchronizedList(new System.Collections.Generic.LinkedList
			<org.apache.hadoop.ha.HealthMonitor.Callback>());

		private System.Collections.Generic.IList<org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback
			> serviceStateCallbacks = java.util.Collections.synchronizedList(new System.Collections.Generic.LinkedList
			<org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback>());

		private org.apache.hadoop.ha.HAServiceStatus lastServiceState = new org.apache.hadoop.ha.HAServiceStatus
			(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING);

		public enum State
		{
			INITIALIZING,
			SERVICE_NOT_RESPONDING,
			SERVICE_HEALTHY,
			SERVICE_UNHEALTHY,
			HEALTH_MONITOR_FAILED
		}

		internal HealthMonitor(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.ha.HAServiceTarget
			 target)
		{
			this.targetToMonitor = target;
			this.conf = conf;
			this.sleepAfterDisconnectMillis = conf.getLong(HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 
				HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT);
			this.checkIntervalMillis = conf.getLong(HA_HM_CHECK_INTERVAL_KEY, HA_HM_CHECK_INTERVAL_DEFAULT
				);
			this.connectRetryInterval = conf.getLong(HA_HM_CONNECT_RETRY_INTERVAL_KEY, HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT
				);
			this.rpcTimeout = conf.getInt(HA_HM_RPC_TIMEOUT_KEY, HA_HM_RPC_TIMEOUT_DEFAULT);
			this.daemon = new org.apache.hadoop.ha.HealthMonitor.MonitorDaemon(this);
		}

		public virtual void addCallback(org.apache.hadoop.ha.HealthMonitor.Callback cb)
		{
			this.callbacks.add(cb);
		}

		public virtual void removeCallback(org.apache.hadoop.ha.HealthMonitor.Callback cb
			)
		{
			callbacks.remove(cb);
		}

		public virtual void addServiceStateCallback(org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback
			 cb)
		{
			lock (this)
			{
				this.serviceStateCallbacks.add(cb);
			}
		}

		public virtual void removeServiceStateCallback(org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback
			 cb)
		{
			lock (this)
			{
				serviceStateCallbacks.remove(cb);
			}
		}

		public virtual void shutdown()
		{
			LOG.info("Stopping HealthMonitor thread");
			shouldRun = false;
			daemon.interrupt();
		}

		/// <returns>
		/// the current proxy object to the underlying service.
		/// Note that this may return null in the case that the service
		/// is not responding. Also note that, even if the last indicated
		/// state is healthy, the service may have gone down in the meantime.
		/// </returns>
		public virtual org.apache.hadoop.ha.HAServiceProtocol getProxy()
		{
			lock (this)
			{
				return proxy;
			}
		}

		/// <exception cref="System.Exception"/>
		private void loopUntilConnected()
		{
			tryConnect();
			while (proxy == null)
			{
				java.lang.Thread.sleep(connectRetryInterval);
				tryConnect();
			}
			System.Diagnostics.Debug.Assert(proxy != null);
		}

		private void tryConnect()
		{
			com.google.common.@base.Preconditions.checkState(proxy == null);
			try
			{
				lock (this)
				{
					proxy = createProxy();
				}
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Could not connect to local service at " + targetToMonitor + ": " + e.Message
					);
				proxy = null;
				enterState(org.apache.hadoop.ha.HealthMonitor.State.SERVICE_NOT_RESPONDING);
			}
		}

		/// <summary>Connect to the service to be monitored.</summary>
		/// <remarks>Connect to the service to be monitored. Stubbed out for easier testing.</remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.ha.HAServiceProtocol createProxy()
		{
			return targetToMonitor.getProxy(conf, rpcTimeout);
		}

		/// <exception cref="System.Exception"/>
		private void doHealthChecks()
		{
			while (shouldRun)
			{
				org.apache.hadoop.ha.HAServiceStatus status = null;
				bool healthy = false;
				try
				{
					status = proxy.getServiceStatus();
					proxy.monitorHealth();
					healthy = true;
				}
				catch (System.Exception t)
				{
					if (isHealthCheckFailedException(t))
					{
						LOG.warn("Service health check failed for " + targetToMonitor + ": " + t.Message);
						enterState(org.apache.hadoop.ha.HealthMonitor.State.SERVICE_UNHEALTHY);
					}
					else
					{
						LOG.warn("Transport-level exception trying to monitor health of " + targetToMonitor
							 + ": " + t.InnerException + " " + t.getLocalizedMessage());
						org.apache.hadoop.ipc.RPC.stopProxy(proxy);
						proxy = null;
						enterState(org.apache.hadoop.ha.HealthMonitor.State.SERVICE_NOT_RESPONDING);
						java.lang.Thread.sleep(sleepAfterDisconnectMillis);
						return;
					}
				}
				if (status != null)
				{
					setLastServiceStatus(status);
				}
				if (healthy)
				{
					enterState(org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY);
				}
				java.lang.Thread.sleep(checkIntervalMillis);
			}
		}

		private bool isHealthCheckFailedException(System.Exception t)
		{
			return ((t is org.apache.hadoop.ha.HealthCheckFailedException) || (t is org.apache.hadoop.ipc.RemoteException
				 && ((org.apache.hadoop.ipc.RemoteException)t).unwrapRemoteException(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.HealthCheckFailedException))) is org.apache.hadoop.ha.HealthCheckFailedException
				));
		}

		private void setLastServiceStatus(org.apache.hadoop.ha.HAServiceStatus status)
		{
			lock (this)
			{
				this.lastServiceState = status;
				foreach (org.apache.hadoop.ha.HealthMonitor.ServiceStateCallback cb in serviceStateCallbacks)
				{
					cb.reportServiceStatus(lastServiceState);
				}
			}
		}

		private void enterState(org.apache.hadoop.ha.HealthMonitor.State newState)
		{
			lock (this)
			{
				if (newState != state)
				{
					LOG.info("Entering state " + newState);
					state = newState;
					lock (callbacks)
					{
						foreach (org.apache.hadoop.ha.HealthMonitor.Callback cb in callbacks)
						{
							cb.enteredState(newState);
						}
					}
				}
			}
		}

		internal virtual org.apache.hadoop.ha.HealthMonitor.State getHealthState()
		{
			lock (this)
			{
				return state;
			}
		}

		internal virtual org.apache.hadoop.ha.HAServiceStatus getLastServiceStatus()
		{
			lock (this)
			{
				return lastServiceState;
			}
		}

		internal virtual bool isAlive()
		{
			return daemon.isAlive();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void join()
		{
			daemon.join();
		}

		internal virtual void start()
		{
			daemon.start();
		}

		private class MonitorDaemon : org.apache.hadoop.util.Daemon
		{
			private MonitorDaemon(HealthMonitor _enclosing)
				: base()
			{
				this._enclosing = _enclosing;
				this.setName("Health Monitor for " + this._enclosing.targetToMonitor);
				this.setUncaughtExceptionHandler(new _UncaughtExceptionHandler_283(this));
			}

			private sealed class _UncaughtExceptionHandler_283 : java.lang.Thread.UncaughtExceptionHandler
			{
				public _UncaughtExceptionHandler_283(MonitorDaemon _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void uncaughtException(java.lang.Thread t, System.Exception e)
				{
					org.apache.hadoop.ha.HealthMonitor.LOG.fatal("Health monitor failed", e);
					this._enclosing._enclosing.enterState(org.apache.hadoop.ha.HealthMonitor.State.HEALTH_MONITOR_FAILED
						);
				}

				private readonly MonitorDaemon _enclosing;
			}

			public override void run()
			{
				while (this._enclosing.shouldRun)
				{
					try
					{
						this._enclosing.loopUntilConnected();
						this._enclosing.doHealthChecks();
					}
					catch (System.Exception)
					{
						com.google.common.@base.Preconditions.checkState(!this._enclosing.shouldRun, "Interrupted but still supposed to run"
							);
					}
				}
			}

			private readonly HealthMonitor _enclosing;
		}

		/// <summary>Callback interface for state change events.</summary>
		/// <remarks>
		/// Callback interface for state change events.
		/// This interface is called from a single thread which also performs
		/// the health monitoring. If the callback processing takes a long time,
		/// no further health checks will be made during this period, nor will
		/// other registered callbacks be called.
		/// If the callback itself throws an unchecked exception, no other
		/// callbacks following it will be called, and the health monitor
		/// will terminate, entering HEALTH_MONITOR_FAILED state.
		/// </remarks>
		internal interface Callback
		{
			void enteredState(org.apache.hadoop.ha.HealthMonitor.State newState);
		}

		/// <summary>Callback interface for service states.</summary>
		internal interface ServiceStateCallback
		{
			void reportServiceStatus(org.apache.hadoop.ha.HAServiceStatus status);
		}
	}
}
