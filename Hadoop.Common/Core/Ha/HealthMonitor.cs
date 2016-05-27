using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.HA
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
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.HealthMonitor
			));

		private Daemon daemon;

		private long connectRetryInterval;

		private long checkIntervalMillis;

		private long sleepAfterDisconnectMillis;

		private int rpcTimeout;

		private volatile bool shouldRun = true;

		/// <summary>The connected proxy</summary>
		private HAServiceProtocol proxy;

		/// <summary>The HA service to monitor</summary>
		private readonly HAServiceTarget targetToMonitor;

		private readonly Configuration conf;

		private HealthMonitor.State state = HealthMonitor.State.Initializing;

		/// <summary>Listeners for state changes</summary>
		private IList<HealthMonitor.Callback> callbacks = Sharpen.Collections.SynchronizedList
			(new List<HealthMonitor.Callback>());

		private IList<HealthMonitor.ServiceStateCallback> serviceStateCallbacks = Sharpen.Collections
			.SynchronizedList(new List<HealthMonitor.ServiceStateCallback>());

		private HAServiceStatus lastServiceState = new HAServiceStatus(HAServiceProtocol.HAServiceState
			.Initializing);

		public enum State
		{
			Initializing,
			ServiceNotResponding,
			ServiceHealthy,
			ServiceUnhealthy,
			HealthMonitorFailed
		}

		internal HealthMonitor(Configuration conf, HAServiceTarget target)
		{
			this.targetToMonitor = target;
			this.conf = conf;
			this.sleepAfterDisconnectMillis = conf.GetLong(HaHmSleepAfterDisconnectKey, HaHmSleepAfterDisconnectDefault
				);
			this.checkIntervalMillis = conf.GetLong(HaHmCheckIntervalKey, HaHmCheckIntervalDefault
				);
			this.connectRetryInterval = conf.GetLong(HaHmConnectRetryIntervalKey, HaHmConnectRetryIntervalDefault
				);
			this.rpcTimeout = conf.GetInt(HaHmRpcTimeoutKey, HaHmRpcTimeoutDefault);
			this.daemon = new HealthMonitor.MonitorDaemon(this);
		}

		public virtual void AddCallback(HealthMonitor.Callback cb)
		{
			this.callbacks.AddItem(cb);
		}

		public virtual void RemoveCallback(HealthMonitor.Callback cb)
		{
			callbacks.Remove(cb);
		}

		public virtual void AddServiceStateCallback(HealthMonitor.ServiceStateCallback cb
			)
		{
			lock (this)
			{
				this.serviceStateCallbacks.AddItem(cb);
			}
		}

		public virtual void RemoveServiceStateCallback(HealthMonitor.ServiceStateCallback
			 cb)
		{
			lock (this)
			{
				serviceStateCallbacks.Remove(cb);
			}
		}

		public virtual void Shutdown()
		{
			Log.Info("Stopping HealthMonitor thread");
			shouldRun = false;
			daemon.Interrupt();
		}

		/// <returns>
		/// the current proxy object to the underlying service.
		/// Note that this may return null in the case that the service
		/// is not responding. Also note that, even if the last indicated
		/// state is healthy, the service may have gone down in the meantime.
		/// </returns>
		public virtual HAServiceProtocol GetProxy()
		{
			lock (this)
			{
				return proxy;
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoopUntilConnected()
		{
			TryConnect();
			while (proxy == null)
			{
				Sharpen.Thread.Sleep(connectRetryInterval);
				TryConnect();
			}
			System.Diagnostics.Debug.Assert(proxy != null);
		}

		private void TryConnect()
		{
			Preconditions.CheckState(proxy == null);
			try
			{
				lock (this)
				{
					proxy = CreateProxy();
				}
			}
			catch (IOException e)
			{
				Log.Warn("Could not connect to local service at " + targetToMonitor + ": " + e.Message
					);
				proxy = null;
				EnterState(HealthMonitor.State.ServiceNotResponding);
			}
		}

		/// <summary>Connect to the service to be monitored.</summary>
		/// <remarks>Connect to the service to be monitored. Stubbed out for easier testing.</remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual HAServiceProtocol CreateProxy()
		{
			return targetToMonitor.GetProxy(conf, rpcTimeout);
		}

		/// <exception cref="System.Exception"/>
		private void DoHealthChecks()
		{
			while (shouldRun)
			{
				HAServiceStatus status = null;
				bool healthy = false;
				try
				{
					status = proxy.GetServiceStatus();
					proxy.MonitorHealth();
					healthy = true;
				}
				catch (Exception t)
				{
					if (IsHealthCheckFailedException(t))
					{
						Log.Warn("Service health check failed for " + targetToMonitor + ": " + t.Message);
						EnterState(HealthMonitor.State.ServiceUnhealthy);
					}
					else
					{
						Log.Warn("Transport-level exception trying to monitor health of " + targetToMonitor
							 + ": " + t.InnerException + " " + t.GetLocalizedMessage());
						RPC.StopProxy(proxy);
						proxy = null;
						EnterState(HealthMonitor.State.ServiceNotResponding);
						Sharpen.Thread.Sleep(sleepAfterDisconnectMillis);
						return;
					}
				}
				if (status != null)
				{
					SetLastServiceStatus(status);
				}
				if (healthy)
				{
					EnterState(HealthMonitor.State.ServiceHealthy);
				}
				Sharpen.Thread.Sleep(checkIntervalMillis);
			}
		}

		private bool IsHealthCheckFailedException(Exception t)
		{
			return ((t is HealthCheckFailedException) || (t is RemoteException && ((RemoteException
				)t).UnwrapRemoteException(typeof(HealthCheckFailedException)) is HealthCheckFailedException
				));
		}

		private void SetLastServiceStatus(HAServiceStatus status)
		{
			lock (this)
			{
				this.lastServiceState = status;
				foreach (HealthMonitor.ServiceStateCallback cb in serviceStateCallbacks)
				{
					cb.ReportServiceStatus(lastServiceState);
				}
			}
		}

		private void EnterState(HealthMonitor.State newState)
		{
			lock (this)
			{
				if (newState != state)
				{
					Log.Info("Entering state " + newState);
					state = newState;
					lock (callbacks)
					{
						foreach (HealthMonitor.Callback cb in callbacks)
						{
							cb.EnteredState(newState);
						}
					}
				}
			}
		}

		internal virtual HealthMonitor.State GetHealthState()
		{
			lock (this)
			{
				return state;
			}
		}

		internal virtual HAServiceStatus GetLastServiceStatus()
		{
			lock (this)
			{
				return lastServiceState;
			}
		}

		internal virtual bool IsAlive()
		{
			return daemon.IsAlive();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Join()
		{
			daemon.Join();
		}

		internal virtual void Start()
		{
			daemon.Start();
		}

		private class MonitorDaemon : Daemon
		{
			private MonitorDaemon(HealthMonitor _enclosing)
				: base()
			{
				this._enclosing = _enclosing;
				this.SetName("Health Monitor for " + this._enclosing.targetToMonitor);
				this.SetUncaughtExceptionHandler(new _UncaughtExceptionHandler_283(this));
			}

			private sealed class _UncaughtExceptionHandler_283 : Sharpen.Thread.UncaughtExceptionHandler
			{
				public _UncaughtExceptionHandler_283(MonitorDaemon _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void UncaughtException(Sharpen.Thread t, Exception e)
				{
					HealthMonitor.Log.Fatal("Health monitor failed", e);
					this._enclosing._enclosing.EnterState(HealthMonitor.State.HealthMonitorFailed);
				}

				private readonly MonitorDaemon _enclosing;
			}

			public override void Run()
			{
				while (this._enclosing.shouldRun)
				{
					try
					{
						this._enclosing.LoopUntilConnected();
						this._enclosing.DoHealthChecks();
					}
					catch (Exception)
					{
						Preconditions.CheckState(!this._enclosing.shouldRun, "Interrupted but still supposed to run"
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
			void EnteredState(HealthMonitor.State newState);
		}

		/// <summary>Callback interface for service states.</summary>
		internal interface ServiceStateCallback
		{
			void ReportServiceStatus(HAServiceStatus status);
		}
	}
}
