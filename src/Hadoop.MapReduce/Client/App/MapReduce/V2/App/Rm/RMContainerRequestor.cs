using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	/// <summary>Keeps the data structures to send container requests to RM.</summary>
	public abstract class RMContainerRequestor : RMCommunicator
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.RM.RMContainerRequestor
			));

		private static readonly ResourceRequest.ResourceRequestComparator ResourceRequestComparator
			 = new ResourceRequest.ResourceRequestComparator();

		protected internal int lastResponseID;

		private Resource availableResources;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly IDictionary<Priority, IDictionary<string, IDictionary<Resource, 
			ResourceRequest>>> remoteRequestsTable = new SortedDictionary<Priority, IDictionary
			<string, IDictionary<Resource, ResourceRequest>>>();

		private readonly ICollection<ResourceRequest> ask = new TreeSet<ResourceRequest>(
			ResourceRequestComparator);

		private readonly ICollection<ContainerId> release = new TreeSet<ContainerId>();

		protected internal ICollection<ContainerId> pendingRelease = new TreeSet<ContainerId
			>();

		private readonly IDictionary<ResourceRequest, ResourceRequest> requestLimits = new 
			SortedDictionary<ResourceRequest, ResourceRequest>(ResourceRequestComparator);

		private readonly ICollection<ResourceRequest> requestLimitsToUpdate = new TreeSet
			<ResourceRequest>(ResourceRequestComparator);

		private bool nodeBlacklistingEnabled;

		private int blacklistDisablePercent;

		private AtomicBoolean ignoreBlacklisting = new AtomicBoolean(false);

		private int blacklistedNodeCount = 0;

		private int lastClusterNmCount = 0;

		private int clusterNmCount = 0;

		private int maxTaskFailuresPerNode;

		private readonly IDictionary<string, int> nodeFailures = new Dictionary<string, int
			>();

		private readonly ICollection<string> blacklistedNodes = Sharpen.Collections.NewSetFromMap
			(new ConcurrentHashMap<string, bool>());

		private readonly ICollection<string> blacklistAdditions = Sharpen.Collections.NewSetFromMap
			(new ConcurrentHashMap<string, bool>());

		private readonly ICollection<string> blacklistRemovals = Sharpen.Collections.NewSetFromMap
			(new ConcurrentHashMap<string, bool>());

		public RMContainerRequestor(ClientService clientService, AppContext context)
			: base(clientService, context)
		{
		}

		internal class ContainerRequest
		{
			internal readonly TaskAttemptId attemptID;

			internal readonly Resource capability;

			internal readonly string[] hosts;

			internal readonly string[] racks;

			internal readonly Priority priority;

			/// <summary>
			/// the time when this request object was formed; can be used to avoid
			/// aggressive preemption for recently placed requests
			/// </summary>
			internal readonly long requestTimeMs;

			public ContainerRequest(ContainerRequestEvent @event, Priority priority)
				: this(@event.GetAttemptID(), @event.GetCapability(), @event.GetHosts(), @event.GetRacks
					(), priority)
			{
			}

			public ContainerRequest(ContainerRequestEvent @event, Priority priority, long requestTimeMs
				)
				: this(@event.GetAttemptID(), @event.GetCapability(), @event.GetHosts(), @event.GetRacks
					(), priority, requestTimeMs)
			{
			}

			public ContainerRequest(TaskAttemptId attemptID, Resource capability, string[] hosts
				, string[] racks, Priority priority)
				: this(attemptID, capability, hosts, racks, priority, Runtime.CurrentTimeMillis()
					)
			{
			}

			public ContainerRequest(TaskAttemptId attemptID, Resource capability, string[] hosts
				, string[] racks, Priority priority, long requestTimeMs)
			{
				//Key -> Priority
				//Value -> Map
				//Key->ResourceName (e.g., hostname, rackname, *)
				//Value->Map
				//Key->Resource Capability
				//Value->ResourceRequest
				// use custom comparator to make sure ResourceRequest objects differing only in 
				// numContainers dont end up as duplicates
				// pendingRelease holds history or release requests.request is removed only if
				// RM sends completedContainer.
				// How it different from release? --> release is for per allocate() request.
				//final boolean earlierAttemptFailed;
				this.attemptID = attemptID;
				this.capability = capability;
				this.hosts = hosts;
				this.racks = racks;
				this.priority = priority;
				this.requestTimeMs = requestTimeMs;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("AttemptId[").Append(attemptID).Append("]");
				sb.Append("Capability[").Append(capability).Append("]");
				sb.Append("Priority[").Append(priority).Append("]");
				return sb.ToString();
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			nodeBlacklistingEnabled = conf.GetBoolean(MRJobConfig.MrAmJobNodeBlacklistingEnable
				, true);
			Log.Info("nodeBlacklistingEnabled:" + nodeBlacklistingEnabled);
			maxTaskFailuresPerNode = conf.GetInt(MRJobConfig.MaxTaskFailuresPerTracker, 3);
			blacklistDisablePercent = conf.GetInt(MRJobConfig.MrAmIgnoreBlacklistingBlacklistedNodePerecent
				, MRJobConfig.DefaultMrAmIgnoreBlacklistingBlacklistedNodePercent);
			Log.Info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
			if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100)
			{
				throw new YarnRuntimeException("Invalid blacklistDisablePercent: " + blacklistDisablePercent
					 + ". Should be an integer between 0 and 100 or -1 to disabled");
			}
			Log.Info("blacklistDisablePercent is " + blacklistDisablePercent);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual AllocateResponse MakeRemoteRequest()
		{
			ApplyRequestLimits();
			ResourceBlacklistRequest blacklistRequest = ResourceBlacklistRequest.NewInstance(
				new AList<string>(blacklistAdditions), new AList<string>(blacklistRemovals));
			AllocateRequest allocateRequest = AllocateRequest.NewInstance(lastResponseID, base
				.GetApplicationProgress(), new AList<ResourceRequest>(ask), new AList<ContainerId
				>(release), blacklistRequest);
			AllocateResponse allocateResponse = scheduler.Allocate(allocateRequest);
			lastResponseID = allocateResponse.GetResponseId();
			availableResources = allocateResponse.GetAvailableResources();
			lastClusterNmCount = clusterNmCount;
			clusterNmCount = allocateResponse.GetNumClusterNodes();
			int numCompletedContainers = allocateResponse.GetCompletedContainersStatuses().Count;
			if (ask.Count > 0 || release.Count > 0)
			{
				Log.Info("getResources() for " + applicationId + ":" + " ask=" + ask.Count + " release= "
					 + release.Count + " newContainers=" + allocateResponse.GetAllocatedContainers()
					.Count + " finishedContainers=" + numCompletedContainers + " resourcelimit=" + availableResources
					 + " knownNMs=" + clusterNmCount);
			}
			ask.Clear();
			release.Clear();
			if (numCompletedContainers > 0)
			{
				// re-send limited requests when a container completes to trigger asking
				// for more containers
				Sharpen.Collections.AddAll(requestLimitsToUpdate, requestLimits.Keys);
			}
			if (blacklistAdditions.Count > 0 || blacklistRemovals.Count > 0)
			{
				Log.Info("Update the blacklist for " + applicationId + ": blacklistAdditions=" + 
					blacklistAdditions.Count + " blacklistRemovals=" + blacklistRemovals.Count);
			}
			blacklistAdditions.Clear();
			blacklistRemovals.Clear();
			return allocateResponse;
		}

		private void ApplyRequestLimits()
		{
			IEnumerator<ResourceRequest> iter = requestLimits.Values.GetEnumerator();
			while (iter.HasNext())
			{
				ResourceRequest reqLimit = iter.Next();
				int limit = reqLimit.GetNumContainers();
				IDictionary<string, IDictionary<Resource, ResourceRequest>> remoteRequests = remoteRequestsTable
					[reqLimit.GetPriority()];
				IDictionary<Resource, ResourceRequest> reqMap = (remoteRequests != null) ? remoteRequests
					[ResourceRequest.Any] : null;
				ResourceRequest req = (reqMap != null) ? reqMap[reqLimit.GetCapability()] : null;
				if (req == null)
				{
					continue;
				}
				// update an existing ask or send a new one if updating
				if (ask.Remove(req) || requestLimitsToUpdate.Contains(req))
				{
					ResourceRequest newReq = req.GetNumContainers() > limit ? reqLimit : req;
					ask.AddItem(newReq);
					Log.Info("Applying ask limit of " + newReq.GetNumContainers() + " for priority:" 
						+ reqLimit.GetPriority() + " and capability:" + reqLimit.GetCapability());
				}
				if (limit == int.MaxValue)
				{
					iter.Remove();
				}
			}
			requestLimitsToUpdate.Clear();
		}

		protected internal virtual void AddOutstandingRequestOnResync()
		{
			foreach (IDictionary<string, IDictionary<Resource, ResourceRequest>> rr in remoteRequestsTable
				.Values)
			{
				foreach (IDictionary<Resource, ResourceRequest> capabalities in rr.Values)
				{
					foreach (ResourceRequest request in capabalities.Values)
					{
						AddResourceRequestToAsk(request);
					}
				}
			}
			if (!ignoreBlacklisting.Get())
			{
				Sharpen.Collections.AddAll(blacklistAdditions, blacklistedNodes);
			}
			if (!pendingRelease.IsEmpty())
			{
				Sharpen.Collections.AddAll(release, pendingRelease);
			}
			Sharpen.Collections.AddAll(requestLimitsToUpdate, requestLimits.Keys);
		}

		// May be incorrect if there's multiple NodeManagers running on a single host.
		// knownNodeCount is based on node managers, not hosts. blacklisting is
		// currently based on hosts.
		protected internal virtual void ComputeIgnoreBlacklisting()
		{
			if (!nodeBlacklistingEnabled)
			{
				return;
			}
			if (blacklistDisablePercent != -1 && (blacklistedNodeCount != blacklistedNodes.Count
				 || clusterNmCount != lastClusterNmCount))
			{
				blacklistedNodeCount = blacklistedNodes.Count;
				if (clusterNmCount == 0)
				{
					Log.Info("KnownNode Count at 0. Not computing ignoreBlacklisting");
					return;
				}
				int val = (int)((float)blacklistedNodes.Count / clusterNmCount * 100);
				if (val >= blacklistDisablePercent)
				{
					if (ignoreBlacklisting.CompareAndSet(false, true))
					{
						Log.Info("Ignore blacklisting set to true. Known: " + clusterNmCount + ", Blacklisted: "
							 + blacklistedNodeCount + ", " + val + "%");
						// notify RM to ignore all the blacklisted nodes
						blacklistAdditions.Clear();
						Sharpen.Collections.AddAll(blacklistRemovals, blacklistedNodes);
					}
				}
				else
				{
					if (ignoreBlacklisting.CompareAndSet(true, false))
					{
						Log.Info("Ignore blacklisting set to false. Known: " + clusterNmCount + ", Blacklisted: "
							 + blacklistedNodeCount + ", " + val + "%");
						// notify RM of all the blacklisted nodes
						Sharpen.Collections.AddAll(blacklistAdditions, blacklistedNodes);
						blacklistRemovals.Clear();
					}
				}
			}
		}

		protected internal virtual void ContainerFailedOnHost(string hostName)
		{
			if (!nodeBlacklistingEnabled)
			{
				return;
			}
			if (blacklistedNodes.Contains(hostName))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Host " + hostName + " is already blacklisted.");
				}
				return;
			}
			//already blacklisted
			int failures = Sharpen.Collections.Remove(nodeFailures, hostName);
			failures = failures == null ? Sharpen.Extensions.ValueOf(0) : failures;
			failures++;
			Log.Info(failures + " failures on node " + hostName);
			if (failures >= maxTaskFailuresPerNode)
			{
				blacklistedNodes.AddItem(hostName);
				if (!ignoreBlacklisting.Get())
				{
					blacklistAdditions.AddItem(hostName);
				}
				//Even if blacklisting is ignored, continue to remove the host from
				// the request table. The RM may have additional nodes it can allocate on.
				Log.Info("Blacklisted host " + hostName);
				//remove all the requests corresponding to this hostname
				foreach (IDictionary<string, IDictionary<Resource, ResourceRequest>> remoteRequests
					 in remoteRequestsTable.Values)
				{
					//remove from host if no pending allocations
					bool foundAll = true;
					IDictionary<Resource, ResourceRequest> reqMap = remoteRequests[hostName];
					if (reqMap != null)
					{
						foreach (ResourceRequest req in reqMap.Values)
						{
							if (!ask.Remove(req))
							{
								foundAll = false;
								// if ask already sent to RM, we can try and overwrite it if possible.
								// send a new ask to RM with numContainers
								// specified for the blacklisted host to be 0.
								ResourceRequest zeroedRequest = ResourceRequest.NewInstance(req.GetPriority(), req
									.GetResourceName(), req.GetCapability(), req.GetNumContainers(), req.GetRelaxLocality
									());
								zeroedRequest.SetNumContainers(0);
								// to be sent to RM on next heartbeat
								AddResourceRequestToAsk(zeroedRequest);
							}
						}
						// if all requests were still in ask queue
						// we can remove this request
						if (foundAll)
						{
							Sharpen.Collections.Remove(remoteRequests, hostName);
						}
					}
				}
			}
			else
			{
				// TODO handling of rack blacklisting
				// Removing from rack should be dependent on no. of failures within the rack 
				// Blacklisting a rack on the basis of a single node's blacklisting 
				// may be overly aggressive. 
				// Node failures could be co-related with other failures on the same rack 
				// but we probably need a better approach at trying to decide how and when 
				// to blacklist a rack
				nodeFailures[hostName] = failures;
			}
		}

		protected internal virtual Resource GetAvailableResources()
		{
			return availableResources;
		}

		protected internal virtual void AddContainerReq(RMContainerRequestor.ContainerRequest
			 req)
		{
			// Create resource requests
			foreach (string host in req.hosts)
			{
				// Data-local
				if (!IsNodeBlacklisted(host))
				{
					AddResourceRequest(req.priority, host, req.capability);
				}
			}
			// Nothing Rack-local for now
			foreach (string rack in req.racks)
			{
				AddResourceRequest(req.priority, rack, req.capability);
			}
			// Off-switch
			AddResourceRequest(req.priority, ResourceRequest.Any, req.capability);
		}

		protected internal virtual void DecContainerReq(RMContainerRequestor.ContainerRequest
			 req)
		{
			// Update resource requests
			foreach (string hostName in req.hosts)
			{
				DecResourceRequest(req.priority, hostName, req.capability);
			}
			foreach (string rack in req.racks)
			{
				DecResourceRequest(req.priority, rack, req.capability);
			}
			DecResourceRequest(req.priority, ResourceRequest.Any, req.capability);
		}

		private void AddResourceRequest(Priority priority, string resourceName, Resource 
			capability)
		{
			IDictionary<string, IDictionary<Resource, ResourceRequest>> remoteRequests = this
				.remoteRequestsTable[priority];
			if (remoteRequests == null)
			{
				remoteRequests = new Dictionary<string, IDictionary<Resource, ResourceRequest>>();
				this.remoteRequestsTable[priority] = remoteRequests;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Added priority=" + priority);
				}
			}
			IDictionary<Resource, ResourceRequest> reqMap = remoteRequests[resourceName];
			if (reqMap == null)
			{
				reqMap = new Dictionary<Resource, ResourceRequest>();
				remoteRequests[resourceName] = reqMap;
			}
			ResourceRequest remoteRequest = reqMap[capability];
			if (remoteRequest == null)
			{
				remoteRequest = recordFactory.NewRecordInstance<ResourceRequest>();
				remoteRequest.SetPriority(priority);
				remoteRequest.SetResourceName(resourceName);
				remoteRequest.SetCapability(capability);
				remoteRequest.SetNumContainers(0);
				reqMap[capability] = remoteRequest;
			}
			remoteRequest.SetNumContainers(remoteRequest.GetNumContainers() + 1);
			// Note this down for next interaction with ResourceManager
			AddResourceRequestToAsk(remoteRequest);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("addResourceRequest:" + " applicationId=" + applicationId.GetId() + " priority="
					 + priority.GetPriority() + " resourceName=" + resourceName + " numContainers=" 
					+ remoteRequest.GetNumContainers() + " #asks=" + ask.Count);
			}
		}

		private void DecResourceRequest(Priority priority, string resourceName, Resource 
			capability)
		{
			IDictionary<string, IDictionary<Resource, ResourceRequest>> remoteRequests = this
				.remoteRequestsTable[priority];
			IDictionary<Resource, ResourceRequest> reqMap = remoteRequests[resourceName];
			if (reqMap == null)
			{
				// as we modify the resource requests by filtering out blacklisted hosts 
				// when they are added, this value may be null when being 
				// decremented
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Not decrementing resource as " + resourceName + " is not present in request table"
						);
				}
				return;
			}
			ResourceRequest remoteRequest = reqMap[capability];
			if (Log.IsDebugEnabled())
			{
				Log.Debug("BEFORE decResourceRequest:" + " applicationId=" + applicationId.GetId(
					) + " priority=" + priority.GetPriority() + " resourceName=" + resourceName + " numContainers="
					 + remoteRequest.GetNumContainers() + " #asks=" + ask.Count);
			}
			if (remoteRequest.GetNumContainers() > 0)
			{
				// based on blacklisting comments above we can end up decrementing more 
				// than requested. so guard for that.
				remoteRequest.SetNumContainers(remoteRequest.GetNumContainers() - 1);
			}
			if (remoteRequest.GetNumContainers() == 0)
			{
				Sharpen.Collections.Remove(reqMap, capability);
				if (reqMap.Count == 0)
				{
					Sharpen.Collections.Remove(remoteRequests, resourceName);
				}
				if (remoteRequests.Count == 0)
				{
					Sharpen.Collections.Remove(remoteRequestsTable, priority);
				}
			}
			// send the updated resource request to RM
			// send 0 container count requests also to cancel previous requests
			AddResourceRequestToAsk(remoteRequest);
			if (Log.IsDebugEnabled())
			{
				Log.Info("AFTER decResourceRequest:" + " applicationId=" + applicationId.GetId() 
					+ " priority=" + priority.GetPriority() + " resourceName=" + resourceName + " numContainers="
					 + remoteRequest.GetNumContainers() + " #asks=" + ask.Count);
			}
		}

		private void AddResourceRequestToAsk(ResourceRequest remoteRequest)
		{
			// because objects inside the resource map can be deleted ask can end up 
			// containing an object that matches new resource object but with different
			// numContainers. So existing values must be replaced explicitly
			ask.Remove(remoteRequest);
			ask.AddItem(remoteRequest);
		}

		protected internal virtual void Release(ContainerId containerId)
		{
			release.AddItem(containerId);
		}

		protected internal virtual bool IsNodeBlacklisted(string hostname)
		{
			if (!nodeBlacklistingEnabled || ignoreBlacklisting.Get())
			{
				return false;
			}
			return blacklistedNodes.Contains(hostname);
		}

		protected internal virtual RMContainerRequestor.ContainerRequest GetFilteredContainerRequest
			(RMContainerRequestor.ContainerRequest orig)
		{
			AList<string> newHosts = new AList<string>();
			foreach (string host in orig.hosts)
			{
				if (!IsNodeBlacklisted(host))
				{
					newHosts.AddItem(host);
				}
			}
			string[] hosts = Sharpen.Collections.ToArray(newHosts, new string[newHosts.Count]
				);
			RMContainerRequestor.ContainerRequest newReq = new RMContainerRequestor.ContainerRequest
				(orig.attemptID, orig.capability, hosts, orig.racks, orig.priority);
			return newReq;
		}

		protected internal virtual void SetRequestLimit(Priority priority, Resource capability
			, int limit)
		{
			if (limit < 0)
			{
				limit = int.MaxValue;
			}
			ResourceRequest newReqLimit = ResourceRequest.NewInstance(priority, ResourceRequest
				.Any, capability, limit);
			ResourceRequest oldReqLimit = requestLimits[newReqLimit] = newReqLimit;
			if (oldReqLimit == null || oldReqLimit.GetNumContainers() < limit)
			{
				requestLimitsToUpdate.AddItem(newReqLimit);
			}
		}

		public virtual ICollection<string> GetBlacklistedNodes()
		{
			return blacklistedNodes;
		}
	}
}
