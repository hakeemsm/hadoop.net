using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	public abstract class AMRMClient<T> : AbstractService
		where T : AMRMClient.ContainerRequest
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.AMRMClient
			));

		/// <summary>Create a new instance of AMRMClient.</summary>
		/// <remarks>
		/// Create a new instance of AMRMClient.
		/// For usage:
		/// <pre>
		/// <c>AMRMClient.&lt;T&gt;createAMRMClientContainerRequest()</c>
		/// </pre>
		/// </remarks>
		/// <returns>the newly create AMRMClient instance.</returns>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.AMRMClient<T> CreateAMRMClient<T>
			()
			where T : AMRMClient.ContainerRequest
		{
			Org.Apache.Hadoop.Yarn.Client.Api.AMRMClient<T> client = new AMRMClientImpl<T>();
			return client;
		}

		private NMTokenCache nmTokenCache;

		[InterfaceAudience.Private]
		protected internal AMRMClient(string name)
			: base(name)
		{
			nmTokenCache = NMTokenCache.GetSingleton();
		}

		/// <summary>Object to represent a single container request for resources.</summary>
		/// <remarks>
		/// Object to represent a single container request for resources. Scheduler
		/// documentation should be consulted for the specifics of how the parameters
		/// are honored.
		/// By default, YARN schedulers try to allocate containers at the requested
		/// locations but they may relax the constraints in order to expedite meeting
		/// allocations limits. They first relax the constraint to the same rack as the
		/// requested node and then to anywhere in the cluster. The relaxLocality flag
		/// may be used to disable locality relaxation and request containers at only
		/// specific locations. The following conditions apply.
		/// <ul>
		/// <li>Within a priority, all container requests must have the same value for
		/// locality relaxation. Either enabled or disabled.</li>
		/// <li>If locality relaxation is disabled, then across requests, locations at
		/// different network levels may not be specified. E.g. its invalid to make a
		/// request for a specific node and another request for a specific rack.</li>
		/// <li>If locality relaxation is disabled, then only within the same request,
		/// a node and its rack may be specified together. This allows for a specific
		/// rack with a preference for a specific node within that rack.</li>
		/// <li></li>
		/// </ul>
		/// To re-enable locality relaxation at a given priority, all pending requests
		/// with locality relaxation disabled must be first removed. Then they can be
		/// added back with locality relaxation enabled.
		/// All getters return immutable values.
		/// </remarks>
		public class ContainerRequest
		{
			internal readonly Resource capability;

			internal readonly IList<string> nodes;

			internal readonly IList<string> racks;

			internal readonly Priority priority;

			internal readonly bool relaxLocality;

			internal readonly string nodeLabelsExpression;

			/// <summary>
			/// Instantiates a
			/// <see cref="ContainerRequest"/>
			/// with the given constraints and
			/// locality relaxation enabled.
			/// </summary>
			/// <param name="capability">
			/// The
			/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
			/// to be requested for each container.
			/// </param>
			/// <param name="nodes">Any hosts to request that the containers are placed on.</param>
			/// <param name="racks">
			/// Any racks to request that the containers are placed on. The
			/// racks corresponding to any hosts requested will be automatically
			/// added to this list.
			/// </param>
			/// <param name="priority">
			/// The priority at which to request the containers. Higher
			/// priorities have lower numerical values.
			/// </param>
			public ContainerRequest(Resource capability, string[] nodes, string[] racks, Priority
				 priority)
				: this(capability, nodes, racks, priority, true, null)
			{
			}

			/// <summary>
			/// Instantiates a
			/// <see cref="ContainerRequest"/>
			/// with the given constraints.
			/// </summary>
			/// <param name="capability">
			/// The
			/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
			/// to be requested for each container.
			/// </param>
			/// <param name="nodes">Any hosts to request that the containers are placed on.</param>
			/// <param name="racks">
			/// Any racks to request that the containers are placed on. The
			/// racks corresponding to any hosts requested will be automatically
			/// added to this list.
			/// </param>
			/// <param name="priority">
			/// The priority at which to request the containers. Higher
			/// priorities have lower numerical values.
			/// </param>
			/// <param name="relaxLocality">
			/// If true, containers for this request may be assigned on hosts
			/// and racks other than the ones explicitly requested.
			/// </param>
			public ContainerRequest(Resource capability, string[] nodes, string[] racks, Priority
				 priority, bool relaxLocality)
				: this(capability, nodes, racks, priority, relaxLocality, null)
			{
			}

			/// <summary>
			/// Instantiates a
			/// <see cref="ContainerRequest"/>
			/// with the given constraints.
			/// </summary>
			/// <param name="capability">
			/// The
			/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
			/// to be requested for each container.
			/// </param>
			/// <param name="nodes">Any hosts to request that the containers are placed on.</param>
			/// <param name="racks">
			/// Any racks to request that the containers are placed on. The
			/// racks corresponding to any hosts requested will be automatically
			/// added to this list.
			/// </param>
			/// <param name="priority">
			/// The priority at which to request the containers. Higher
			/// priorities have lower numerical values.
			/// </param>
			/// <param name="relaxLocality">
			/// If true, containers for this request may be assigned on hosts
			/// and racks other than the ones explicitly requested.
			/// </param>
			/// <param name="nodeLabelsExpression">
			/// Set node labels to allocate resource, now we only support
			/// asking for only a single node label
			/// </param>
			public ContainerRequest(Resource capability, string[] nodes, string[] racks, Priority
				 priority, bool relaxLocality, string nodeLabelsExpression)
			{
				// Validate request
				Preconditions.CheckArgument(capability != null, "The Resource to be requested for each container "
					 + "should not be null ");
				Preconditions.CheckArgument(priority != null, "The priority at which to request containers should not be null "
					);
				Preconditions.CheckArgument(!(!relaxLocality && (racks == null || racks.Length ==
					 0) && (nodes == null || nodes.Length == 0)), "Can't turn off locality relaxation on a "
					 + "request with no location constraints");
				this.capability = capability;
				this.nodes = (nodes != null ? ImmutableList.CopyOf(nodes) : null);
				this.racks = (racks != null ? ImmutableList.CopyOf(racks) : null);
				this.priority = priority;
				this.relaxLocality = relaxLocality;
				this.nodeLabelsExpression = nodeLabelsExpression;
			}

			public virtual Resource GetCapability()
			{
				return capability;
			}

			public virtual IList<string> GetNodes()
			{
				return nodes;
			}

			public virtual IList<string> GetRacks()
			{
				return racks;
			}

			public virtual Priority GetPriority()
			{
				return priority;
			}

			public virtual bool GetRelaxLocality()
			{
				return relaxLocality;
			}

			public virtual string GetNodeLabelExpression()
			{
				return nodeLabelsExpression;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("Capability[").Append(capability).Append("]");
				sb.Append("Priority[").Append(priority).Append("]");
				return sb.ToString();
			}
		}

		/// <summary>Register the application master.</summary>
		/// <remarks>
		/// Register the application master. This must be called before any
		/// other interaction
		/// </remarks>
		/// <param name="appHostName">Name of the host on which master is running</param>
		/// <param name="appHostPort">Port master is listening on</param>
		/// <param name="appTrackingUrl">URL at which the master info can be seen</param>
		/// <returns><code>RegisterApplicationMasterResponse</code></returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract RegisterApplicationMasterResponse RegisterApplicationMaster(string
			 appHostName, int appHostPort, string appTrackingUrl);

		/// <summary>Request additional containers and receive new container allocations.</summary>
		/// <remarks>
		/// Request additional containers and receive new container allocations.
		/// Requests made via <code>addContainerRequest</code> are sent to the
		/// <code>ResourceManager</code>. New containers assigned to the master are
		/// retrieved. Status of completed containers and node health updates are also
		/// retrieved. This also doubles up as a heartbeat to the ResourceManager and
		/// must be made periodically. The call may not always return any new
		/// allocations of containers. App should not make concurrent allocate
		/// requests. May cause request loss.
		/// <p>
		/// Note : If the user has not removed container requests that have already
		/// been satisfied, then the re-register may end up sending the entire
		/// container requests to the RM (including matched requests). Which would mean
		/// the RM could end up giving it a lot of new allocated containers.
		/// </p>
		/// </remarks>
		/// <param name="progressIndicator">Indicates progress made by the master</param>
		/// <returns>the response of the allocate request</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract AllocateResponse Allocate(float progressIndicator);

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

		/// <summary>
		/// Get outstanding <code>ContainerRequest</code>s matching the given
		/// parameters.
		/// </summary>
		/// <remarks>
		/// Get outstanding <code>ContainerRequest</code>s matching the given
		/// parameters. These ContainerRequests should have been added via
		/// <code>addContainerRequest</code> earlier in the lifecycle. For performance,
		/// the AMRMClient may return its internal collection directly without creating
		/// a copy. Users should not perform mutable operations on the return value.
		/// Each collection in the list contains requests with identical
		/// <code>Resource</code> size that fit in the given capability. In a
		/// collection, requests will be returned in the same order as they were added.
		/// </remarks>
		/// <returns>Collection of request matching the parameters</returns>
		public abstract IList<ICollection<T>> GetMatchingRequests(Priority priority, string
			 resourceName, Resource capability);

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

		/// <summary>Set the NM token cache for the <code>AMRMClient</code>.</summary>
		/// <remarks>
		/// Set the NM token cache for the <code>AMRMClient</code>. This cache must
		/// be shared with the
		/// <see cref="NMClient"/>
		/// used to manage containers for the
		/// <code>AMRMClient</code>
		/// <p>
		/// If a NM token cache is not set, the
		/// <see cref="NMTokenCache.GetSingleton()"/>
		/// singleton instance will be used.
		/// </remarks>
		/// <param name="nmTokenCache">the NM token cache to use.</param>
		public virtual void SetNMTokenCache(NMTokenCache nmTokenCache)
		{
			this.nmTokenCache = nmTokenCache;
		}

		/// <summary>Get the NM token cache of the <code>AMRMClient</code>.</summary>
		/// <remarks>
		/// Get the NM token cache of the <code>AMRMClient</code>. This cache must be
		/// shared with the
		/// <see cref="NMClient"/>
		/// used to manage containers for the
		/// <code>AMRMClient</code>.
		/// <p>
		/// If a NM token cache is not set, the
		/// <see cref="NMTokenCache.GetSingleton()"/>
		/// singleton instance will be used.
		/// </remarks>
		/// <returns>the NM token cache.</returns>
		public virtual NMTokenCache GetNMTokenCache()
		{
			return nmTokenCache;
		}

		/// <summary>Wait for <code>check</code> to return true for each 1000 ms.</summary>
		/// <remarks>
		/// Wait for <code>check</code> to return true for each 1000 ms.
		/// See also
		/// <see cref="AMRMClient{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int)"/>
		/// and
		/// <see cref="AMRMClient{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int, int)"/>
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
		/// <see cref="AMRMClient{T}.WaitFor(Com.Google.Common.Base.Supplier{T}, int, int)"/>
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
	}
}
