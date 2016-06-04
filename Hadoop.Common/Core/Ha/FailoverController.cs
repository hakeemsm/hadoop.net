using System;
using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// The FailOverController is responsible for electing an active service
	/// on startup or when the current active is changing (eg due to failure),
	/// monitoring the health of a service, and performing a fail-over when a
	/// new active service is either manually selected by a user or elected.
	/// </summary>
	public class FailoverController
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.HA.FailoverController
			));

		private readonly int gracefulFenceTimeout;

		private readonly int rpcTimeoutToNewActive;

		private readonly Configuration conf;

		private readonly Configuration gracefulFenceConf;

		private readonly HAServiceProtocol.RequestSource requestSource;

		public FailoverController(Configuration conf, HAServiceProtocol.RequestSource source
			)
		{
			/*
			* Need a copy of conf for graceful fence to set
			* configurable retries for IPC client.
			* Refer HDFS-3561
			*/
			this.conf = conf;
			this.gracefulFenceConf = new Configuration(conf);
			this.requestSource = source;
			this.gracefulFenceTimeout = GetGracefulFenceTimeout(conf);
			this.rpcTimeoutToNewActive = GetRpcTimeoutToNewActive(conf);
			//Configure less retries for graceful fence 
			int gracefulFenceConnectRetries = conf.GetInt(CommonConfigurationKeys.HaFcGracefulFenceConnectionRetries
				, CommonConfigurationKeys.HaFcGracefulFenceConnectionRetriesDefault);
			gracefulFenceConf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesKey, gracefulFenceConnectRetries
				);
			gracefulFenceConf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, gracefulFenceConnectRetries);
		}

		internal static int GetGracefulFenceTimeout(Configuration conf)
		{
			return conf.GetInt(CommonConfigurationKeys.HaFcGracefulFenceTimeoutKey, CommonConfigurationKeys
				.HaFcGracefulFenceTimeoutDefault);
		}

		internal static int GetRpcTimeoutToNewActive(Configuration conf)
		{
			return conf.GetInt(CommonConfigurationKeys.HaFcNewActiveTimeoutKey, CommonConfigurationKeys
				.HaFcNewActiveTimeoutDefault);
		}

		/// <summary>
		/// Perform pre-failover checks on the given service we plan to
		/// failover to, eg to prevent failing over to a service (eg due
		/// to it being inaccessible, already active, not healthy, etc).
		/// </summary>
		/// <remarks>
		/// Perform pre-failover checks on the given service we plan to
		/// failover to, eg to prevent failing over to a service (eg due
		/// to it being inaccessible, already active, not healthy, etc).
		/// An option to ignore toSvc if it claims it is not ready to
		/// become active is provided in case performing a failover will
		/// allow it to become active, eg because it triggers a log roll
		/// so the standby can learn about new blocks and leave safemode.
		/// </remarks>
		/// <param name="from">currently active service</param>
		/// <param name="target">service to make active</param>
		/// <param name="forceActive">ignore toSvc if it reports that it is not ready</param>
		/// <exception cref="FailoverFailedException">if we should avoid failover</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.FailoverFailedException"/>
		private void PreFailoverChecks(HAServiceTarget from, HAServiceTarget target, bool
			 forceActive)
		{
			HAServiceStatus toSvcStatus;
			HAServiceProtocol toSvc;
			if (from.GetAddress().Equals(target.GetAddress()))
			{
				throw new FailoverFailedException("Can't failover a service to itself");
			}
			try
			{
				toSvc = target.GetProxy(conf, rpcTimeoutToNewActive);
				toSvcStatus = toSvc.GetServiceStatus();
			}
			catch (IOException e)
			{
				string msg = "Unable to get service state for " + target;
				Log.Error(msg + ": " + e.GetLocalizedMessage());
				throw new FailoverFailedException(msg, e);
			}
			if (!toSvcStatus.GetState().Equals(HAServiceProtocol.HAServiceState.Standby))
			{
				throw new FailoverFailedException("Can't failover to an active service");
			}
			if (!toSvcStatus.IsReadyToBecomeActive())
			{
				string notReadyReason = toSvcStatus.GetNotReadyReason();
				if (!forceActive)
				{
					throw new FailoverFailedException(target + " is not ready to become active: " + notReadyReason
						);
				}
				else
				{
					Log.Warn("Service is not ready to become active, but forcing: " + notReadyReason);
				}
			}
			try
			{
				HAServiceProtocolHelper.MonitorHealth(toSvc, CreateReqInfo());
			}
			catch (HealthCheckFailedException hce)
			{
				throw new FailoverFailedException("Can't failover to an unhealthy service", hce);
			}
			catch (IOException e)
			{
				throw new FailoverFailedException("Got an IO exception", e);
			}
		}

		private HAServiceProtocol.StateChangeRequestInfo CreateReqInfo()
		{
			return new HAServiceProtocol.StateChangeRequestInfo(requestSource);
		}

		/// <summary>Try to get the HA state of the node at the given address.</summary>
		/// <remarks>
		/// Try to get the HA state of the node at the given address. This
		/// function is guaranteed to be "quick" -- ie it has a short timeout
		/// and no retries. Its only purpose is to avoid fencing a node that
		/// has already restarted.
		/// </remarks>
		internal virtual bool TryGracefulFence(HAServiceTarget svc)
		{
			HAServiceProtocol proxy = null;
			try
			{
				proxy = svc.GetProxy(gracefulFenceConf, gracefulFenceTimeout);
				proxy.TransitionToStandby(CreateReqInfo());
				return true;
			}
			catch (ServiceFailedException sfe)
			{
				Log.Warn("Unable to gracefully make " + svc + " standby (" + sfe.Message + ")");
			}
			catch (IOException ioe)
			{
				Log.Warn("Unable to gracefully make " + svc + " standby (unable to connect)", ioe
					);
			}
			finally
			{
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
			return false;
		}

		/// <summary>Failover from service 1 to service 2.</summary>
		/// <remarks>
		/// Failover from service 1 to service 2. If the failover fails
		/// then try to failback.
		/// </remarks>
		/// <param name="fromSvc">currently active service</param>
		/// <param name="toSvc">service to make active</param>
		/// <param name="forceFence">to fence fromSvc even if not strictly necessary</param>
		/// <param name="forceActive">try to make toSvc active even if it is not ready</param>
		/// <exception cref="FailoverFailedException">if the failover fails</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.FailoverFailedException"/>
		public virtual void Failover(HAServiceTarget fromSvc, HAServiceTarget toSvc, bool
			 forceFence, bool forceActive)
		{
			Preconditions.CheckArgument(fromSvc.GetFencer() != null, "failover requires a fencer"
				);
			PreFailoverChecks(fromSvc, toSvc, forceActive);
			// Try to make fromSvc standby
			bool tryFence = true;
			if (TryGracefulFence(fromSvc))
			{
				tryFence = forceFence;
			}
			// Fence fromSvc if it's required or forced by the user
			if (tryFence)
			{
				if (!fromSvc.GetFencer().Fence(fromSvc))
				{
					throw new FailoverFailedException("Unable to fence " + fromSvc + ". Fencing failed."
						);
				}
			}
			// Try to make toSvc active
			bool failed = false;
			Exception cause = null;
			try
			{
				HAServiceProtocolHelper.TransitionToActive(toSvc.GetProxy(conf, rpcTimeoutToNewActive
					), CreateReqInfo());
			}
			catch (ServiceFailedException sfe)
			{
				Log.Error("Unable to make " + toSvc + " active (" + sfe.Message + "). Failing back."
					);
				failed = true;
				cause = sfe;
			}
			catch (IOException ioe)
			{
				Log.Error("Unable to make " + toSvc + " active (unable to connect). Failing back."
					, ioe);
				failed = true;
				cause = ioe;
			}
			// We failed to make toSvc active
			if (failed)
			{
				string msg = "Unable to failover to " + toSvc;
				// Only try to failback if we didn't fence fromSvc
				if (!tryFence)
				{
					try
					{
						// Unconditionally fence toSvc in case it is still trying to
						// become active, eg we timed out waiting for its response.
						// Unconditionally force fromSvc to become active since it
						// was previously active when we initiated failover.
						Failover(toSvc, fromSvc, true, true);
					}
					catch (FailoverFailedException ffe)
					{
						msg += ". Failback to " + fromSvc + " failed (" + ffe.Message + ")";
						Log.Fatal(msg);
					}
				}
				throw new FailoverFailedException(msg, cause);
			}
		}
	}
}
