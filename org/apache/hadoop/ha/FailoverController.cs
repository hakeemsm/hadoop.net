using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// The FailOverController is responsible for electing an active service
	/// on startup or when the current active is changing (eg due to failure),
	/// monitoring the health of a service, and performing a fail-over when a
	/// new active service is either manually selected by a user or elected.
	/// </summary>
	public class FailoverController
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.FailoverController
			)));

		private readonly int gracefulFenceTimeout;

		private readonly int rpcTimeoutToNewActive;

		private readonly org.apache.hadoop.conf.Configuration conf;

		private readonly org.apache.hadoop.conf.Configuration gracefulFenceConf;

		private readonly org.apache.hadoop.ha.HAServiceProtocol.RequestSource requestSource;

		public FailoverController(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.ha.HAServiceProtocol.RequestSource
			 source)
		{
			/*
			* Need a copy of conf for graceful fence to set
			* configurable retries for IPC client.
			* Refer HDFS-3561
			*/
			this.conf = conf;
			this.gracefulFenceConf = new org.apache.hadoop.conf.Configuration(conf);
			this.requestSource = source;
			this.gracefulFenceTimeout = getGracefulFenceTimeout(conf);
			this.rpcTimeoutToNewActive = getRpcTimeoutToNewActive(conf);
			//Configure less retries for graceful fence 
			int gracefulFenceConnectRetries = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys
				.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES, org.apache.hadoop.fs.CommonConfigurationKeys
				.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);
			gracefulFenceConf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
				, gracefulFenceConnectRetries);
			gracefulFenceConf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY
				, gracefulFenceConnectRetries);
		}

		internal static int getGracefulFenceTimeout(org.apache.hadoop.conf.Configuration 
			conf)
		{
			return conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT
				);
		}

		internal static int getRpcTimeoutToNewActive(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);
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
		/// <exception cref="org.apache.hadoop.ha.FailoverFailedException"/>
		private void preFailoverChecks(org.apache.hadoop.ha.HAServiceTarget from, org.apache.hadoop.ha.HAServiceTarget
			 target, bool forceActive)
		{
			org.apache.hadoop.ha.HAServiceStatus toSvcStatus;
			org.apache.hadoop.ha.HAServiceProtocol toSvc;
			if (from.getAddress().Equals(target.getAddress()))
			{
				throw new org.apache.hadoop.ha.FailoverFailedException("Can't failover a service to itself"
					);
			}
			try
			{
				toSvc = target.getProxy(conf, rpcTimeoutToNewActive);
				toSvcStatus = toSvc.getServiceStatus();
			}
			catch (System.IO.IOException e)
			{
				string msg = "Unable to get service state for " + target;
				LOG.error(msg + ": " + e.getLocalizedMessage());
				throw new org.apache.hadoop.ha.FailoverFailedException(msg, e);
			}
			if (!toSvcStatus.getState().Equals(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY))
			{
				throw new org.apache.hadoop.ha.FailoverFailedException("Can't failover to an active service"
					);
			}
			if (!toSvcStatus.isReadyToBecomeActive())
			{
				string notReadyReason = toSvcStatus.getNotReadyReason();
				if (!forceActive)
				{
					throw new org.apache.hadoop.ha.FailoverFailedException(target + " is not ready to become active: "
						 + notReadyReason);
				}
				else
				{
					LOG.warn("Service is not ready to become active, but forcing: " + notReadyReason);
				}
			}
			try
			{
				org.apache.hadoop.ha.HAServiceProtocolHelper.monitorHealth(toSvc, createReqInfo()
					);
			}
			catch (org.apache.hadoop.ha.HealthCheckFailedException hce)
			{
				throw new org.apache.hadoop.ha.FailoverFailedException("Can't failover to an unhealthy service"
					, hce);
			}
			catch (System.IO.IOException e)
			{
				throw new org.apache.hadoop.ha.FailoverFailedException("Got an IO exception", e);
			}
		}

		private org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo createReqInfo
			()
		{
			return new org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo(requestSource
				);
		}

		/// <summary>Try to get the HA state of the node at the given address.</summary>
		/// <remarks>
		/// Try to get the HA state of the node at the given address. This
		/// function is guaranteed to be "quick" -- ie it has a short timeout
		/// and no retries. Its only purpose is to avoid fencing a node that
		/// has already restarted.
		/// </remarks>
		internal virtual bool tryGracefulFence(org.apache.hadoop.ha.HAServiceTarget svc)
		{
			org.apache.hadoop.ha.HAServiceProtocol proxy = null;
			try
			{
				proxy = svc.getProxy(gracefulFenceConf, gracefulFenceTimeout);
				proxy.transitionToStandby(createReqInfo());
				return true;
			}
			catch (org.apache.hadoop.ha.ServiceFailedException sfe)
			{
				LOG.warn("Unable to gracefully make " + svc + " standby (" + sfe.Message + ")");
			}
			catch (System.IO.IOException ioe)
			{
				LOG.warn("Unable to gracefully make " + svc + " standby (unable to connect)", ioe
					);
			}
			finally
			{
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
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
		/// <exception cref="org.apache.hadoop.ha.FailoverFailedException"/>
		public virtual void failover(org.apache.hadoop.ha.HAServiceTarget fromSvc, org.apache.hadoop.ha.HAServiceTarget
			 toSvc, bool forceFence, bool forceActive)
		{
			com.google.common.@base.Preconditions.checkArgument(fromSvc.getFencer() != null, 
				"failover requires a fencer");
			preFailoverChecks(fromSvc, toSvc, forceActive);
			// Try to make fromSvc standby
			bool tryFence = true;
			if (tryGracefulFence(fromSvc))
			{
				tryFence = forceFence;
			}
			// Fence fromSvc if it's required or forced by the user
			if (tryFence)
			{
				if (!fromSvc.getFencer().fence(fromSvc))
				{
					throw new org.apache.hadoop.ha.FailoverFailedException("Unable to fence " + fromSvc
						 + ". Fencing failed.");
				}
			}
			// Try to make toSvc active
			bool failed = false;
			System.Exception cause = null;
			try
			{
				org.apache.hadoop.ha.HAServiceProtocolHelper.transitionToActive(toSvc.getProxy(conf
					, rpcTimeoutToNewActive), createReqInfo());
			}
			catch (org.apache.hadoop.ha.ServiceFailedException sfe)
			{
				LOG.error("Unable to make " + toSvc + " active (" + sfe.Message + "). Failing back."
					);
				failed = true;
				cause = sfe;
			}
			catch (System.IO.IOException ioe)
			{
				LOG.error("Unable to make " + toSvc + " active (unable to connect). Failing back."
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
						failover(toSvc, fromSvc, true, true);
					}
					catch (org.apache.hadoop.ha.FailoverFailedException ffe)
					{
						msg += ". Failback to " + fromSvc + " failed (" + ffe.Message + ")";
						LOG.fatal(msg);
					}
				}
				throw new org.apache.hadoop.ha.FailoverFailedException(msg, cause);
			}
		}
	}
}
