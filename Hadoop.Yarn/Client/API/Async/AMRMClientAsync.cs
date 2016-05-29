using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Client.Api.Async.Impl;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Async
{
	/// <summary>
	/// <code>AMRMClientAsync</code> handles communication with the ResourceManager
	/// and provides asynchronous updates on events such as container allocations and
	/// completions.
	/// </summary>
	/// <remarks>
	/// <code>AMRMClientAsync</code> handles communication with the ResourceManager
	/// and provides asynchronous updates on events such as container allocations and
	/// completions.  It contains a thread that sends periodic heartbeats to the
	/// ResourceManager.
	/// It should be used by implementing a CallbackHandler:
	/// <pre>
	/// <c/>
	/// class MyCallbackHandler implements AMRMClientAsync.CallbackHandler
	/// public void onContainersAllocated(List<Container> containers) {
	/// [run tasks on the containers]
	/// }
	/// public void onContainersCompleted(List<ContainerStatus> statuses) {
	/// [update progress, check whether app is done]
	/// }
	/// public void onNodesUpdated(List<NodeReport> updated) {}
	/// public void onReboot() {}
	/// }
	/// }
	/// </pre>
	/// The client's lifecycle should be managed similarly to the following:
	/// <pre>
	/// <c>
	/// AMRMClientAsync asyncClient =
	/// createAMRMClientAsync(appAttId, 1000, new MyCallbackhandler());
	/// asyncClient.init(conf);
	/// asyncClient.start();
	/// RegisterApplicationMasterResponse response = asyncClient
	/// .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
	/// appMasterTrackingUrl);
	/// asyncClient.addContainerRequest(containerRequest);
	/// [... wait for application to complete]
	/// asyncClient.unregisterApplicationMaster(status, appMsg, trackingUrl);
	/// asyncClient.stop();
	/// </c>
	/// </pre>
	/// </remarks>
	public abstract class AMRMClientAsync<T> : AbstractService
		where T : AMRMClient.ContainerRequest
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.AMRMClientAsync
			));

		protected internal readonly AMRMClient<T> client;

		protected internal readonly AMRMClientAsync.CallbackHandler handler;

		protected internal readonly AtomicInteger heartbeatIntervalMs = new AtomicInteger
			();

		public static Org.Apache.Hadoop.Yarn.Client.Api.Async.AMRMClientAsync<T> CreateAMRMClientAsync
			<T>(int intervalMs, AMRMClientAsync.CallbackHandler callbackHandler)
			where T : AMRMClient.ContainerRequest
		{
			return new AMRMClientAsyncImpl<T>(intervalMs, callbackHandler);
		}

		public static Org.Apache.Hadoop.Yarn.Client.Api.Async.AMRMClientAsync<T> CreateAMRMClientAsync
			<T>(AMRMClient<T> client, int intervalMs, AMRMClientAsync.CallbackHandler callbackHandler
			)
			where T : AMRMClient.ContainerRequest
		{
			return new AMRMClientAsyncImpl<T>(client, intervalMs, callbackHandler);
		}

		protected internal AMRMClientAsync(int intervalMs, AMRMClientAsync.CallbackHandler
			 callbackHandler)
			: this(new AMRMClientImpl<T>(), intervalMs, callbackHandler)
		{
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal AMRMClientAsync(AMRMClient<T> client, int intervalMs, AMRMClientAsync.CallbackHandler
			 callbackHandler)
			: base(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Async.AMRMClientAsync).FullName)
		{
			this.client = client;
			this.heartbeatIntervalMs.Set(intervalMs);
			this.handler = callbackHandler;
		}

		public virtual void SetHeartbeatInterval(int interval)
		{
			heartbeatIntervalMs.Set(interval);
		}

		public abstract IList<ICollection<T>> GetMatchingRequests(Priority priority, string
			 resourceName, Resource capability);

		/// <summary>Registers this application master with the resource manager.</summary>
		/// <remarks>
		/// Registers this application master with the resource manager. On successful
		/// registration, starts the heartbeating thread.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract RegisterApplicationMasterResponse RegisterApplicationMaster(string
			 appHostName, int appHostPort, string appTrackingUrl);

		/// <summary>Unregister the application master.</summary>
		/// <remarks>Unregister the application master. This must be called in the end.</remarks>
		/// <param name="appStatus">Success/Failure status of the master</param>
		/// <param name="appMessage">Diagnostics message on failure</param>
		/// <param name="appTrackingUrl">New URL to get master info</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void UnregisterApplicationMaster(FinalApplicationStatus appStatus
			, string appMessage, string appTrackingUrl);

		/// <summary>Request containers for resources before calling <code>allocate</code></summary>
		/// <param name="req">Resource request</param>
		public abstract void AddContainerRequest(T req);

		/// <summary>Remove previous container request.</summary>
		/// <remarks>
		/// Remove previous container request. The previous container request may have
		/// already been sent to the ResourceManager. So even after the remove request
		/// the app must be prepared to receive an allocation for the previous request
		/// even after the remove request
		/// </remarks>
		/// <param name="req">Resource request</param>
		public abstract void RemoveContainerRequest(T req);

		/// <summary>Release containers assigned by the Resource Manager.</summary>
		/// <remarks>
		/// Release containers assigned by the Resource Manager. If the app cannot use
		/// the container or wants to give up the container then it can release them.
		/// The app needs to make new requests for the released resource capability if
		/// it still needs it. eg. it released non-local resources
		/// </remarks>
		/// <param name="containerId"/>
		public abstract void ReleaseAssignedContainer(ContainerId containerId);

		/// <summary>Get the currently available resources in the cluster.</summary>
		/// <remarks>
		/// Get the currently available resources in the cluster.
		/// A valid value is available after a call to allocate has been made
		/// </remarks>
		/// <returns>Currently available resources</returns>
		public abstract Resource GetAvailableResources();

		/// <summary>Get the current number of nodes in the cluster.</summary>
		/// <remarks>
		/// Get the current number of nodes in the cluster.
		/// A valid values is available after a call to allocate has been made
		/// </remarks>
		/// <returns>Current number of nodes in the cluster</returns>
		public abstract int GetClusterNodeCount();

		/// <summary>Update application's blacklist with addition or removal resources.</summary>
		/// <param name="blacklistAdditions">
		/// list of resources which should be added to the
		/// application blacklist
		/// </param>
		/// <param name="blacklistRemovals">
		/// list of resources which should be removed from the
		/// application blacklist
		/// </param>
		public abstract void UpdateBlacklist(IList<string> blacklistAdditions, IList<string
			> blacklistRemovals);

		/// <summary>Wait for <code>check</code> to return true for each 1000 ms.</summary>
		/// <remarks>
		/// Wait for <code>check</code> to return true for each 1000 ms.
		/// See also
		/// <see cref="AMRMClientAsync{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int)"/>
		/// and
		/// <see cref="AMRMClientAsync{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int, int)
		/// 	"/>
		/// </remarks>
		/// <param name="check"/>
		/// <exception cref="System.Exception"/>
		public virtual void WaitFor(Supplier<bool> check)
		{
			WaitFor(check, 1000);
		}

		/// <summary>
		/// Wait for <code>check</code> to return true for each
		/// <code>checkEveryMillis</code> ms.
		/// </summary>
		/// <remarks>
		/// Wait for <code>check</code> to return true for each
		/// <code>checkEveryMillis</code> ms.
		/// See also
		/// <see cref="AMRMClientAsync{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int, int)
		/// 	"/>
		/// </remarks>
		/// <param name="check">user defined checker</param>
		/// <param name="checkEveryMillis">interval to call <code>check</code></param>
		/// <exception cref="System.Exception"/>
		public virtual void WaitFor(Supplier<bool> check, int checkEveryMillis)
		{
			WaitFor(check, checkEveryMillis, 1);
		}

		/// <summary>
		/// Wait for <code>check</code> to return true for each
		/// <code>checkEveryMillis</code> ms.
		/// </summary>
		/// <remarks>
		/// Wait for <code>check</code> to return true for each
		/// <code>checkEveryMillis</code> ms. In the main loop, this method will log
		/// the message "waiting in main loop" for each <code>logInterval</code> times
		/// iteration to confirm the thread is alive.
		/// </remarks>
		/// <param name="check">user defined checker</param>
		/// <param name="checkEveryMillis">interval to call <code>check</code></param>
		/// <param name="logInterval">interval to log for each</param>
		/// <exception cref="System.Exception"/>
		public virtual void WaitFor(Supplier<bool> check, int checkEveryMillis, int logInterval
			)
		{
			Preconditions.CheckNotNull(check, "check should not be null");
			Preconditions.CheckArgument(checkEveryMillis >= 0, "checkEveryMillis should be positive value"
				);
			Preconditions.CheckArgument(logInterval >= 0, "logInterval should be positive value"
				);
			int loggingCounter = logInterval;
			do
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Check the condition for main loop.");
				}
				bool result = check.Get();
				if (result)
				{
					Log.Info("Exits the main loop.");
					return;
				}
				if (--loggingCounter <= 0)
				{
					Log.Info("Waiting in main loop.");
					loggingCounter = logInterval;
				}
				Sharpen.Thread.Sleep(checkEveryMillis);
			}
			while (true);
		}

		public interface CallbackHandler
		{
			/// <summary>
			/// Called when the ResourceManager responds to a heartbeat with completed
			/// containers.
			/// </summary>
			/// <remarks>
			/// Called when the ResourceManager responds to a heartbeat with completed
			/// containers. If the response contains both completed containers and
			/// allocated containers, this will be called before containersAllocated.
			/// </remarks>
			void OnContainersCompleted(IList<ContainerStatus> statuses);

			/// <summary>
			/// Called when the ResourceManager responds to a heartbeat with allocated
			/// containers.
			/// </summary>
			/// <remarks>
			/// Called when the ResourceManager responds to a heartbeat with allocated
			/// containers. If the response containers both completed containers and
			/// allocated containers, this will be called after containersCompleted.
			/// </remarks>
			void OnContainersAllocated(IList<Container> containers);

			/// <summary>
			/// Called when the ResourceManager wants the ApplicationMaster to shutdown
			/// for being out of sync etc.
			/// </summary>
			/// <remarks>
			/// Called when the ResourceManager wants the ApplicationMaster to shutdown
			/// for being out of sync etc. The ApplicationMaster should not unregister
			/// with the RM unless the ApplicationMaster wants to be the last attempt.
			/// </remarks>
			void OnShutdownRequest();

			/// <summary>
			/// Called when nodes tracked by the ResourceManager have changed in health,
			/// availability etc.
			/// </summary>
			void OnNodesUpdated(IList<NodeReport> updatedNodes);

			float GetProgress();

			/// <summary>
			/// Called when error comes from RM communications as well as from errors in
			/// the callback itself from the app.
			/// </summary>
			/// <remarks>
			/// Called when error comes from RM communications as well as from errors in
			/// the callback itself from the app. Calling
			/// stop() is the recommended action.
			/// </remarks>
			/// <param name="e"/>
			void OnError(Exception e);
		}
	}
}
