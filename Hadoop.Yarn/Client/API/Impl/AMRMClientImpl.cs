using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class AMRMClientImpl<T> : AMRMClient<T>
		where T : AMRMClient.ContainerRequest
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.AMRMClientImpl
			));

		private static readonly IList<string> AnyList = Sharpen.Collections.SingletonList
			(ResourceRequest.Any);

		private int lastResponseId = 0;

		protected internal string appHostName;

		protected internal int appHostPort;

		protected internal string appTrackingUrl;

		protected internal ApplicationMasterProtocol rmClient;

		protected internal Resource clusterAvailableResources;

		protected internal int clusterNodeCount;

		protected internal readonly ICollection<string> blacklistedNodes = new HashSet<string
			>();

		protected internal readonly ICollection<string> blacklistAdditions = new HashSet<
			string>();

		protected internal readonly ICollection<string> blacklistRemovals = new HashSet<string
			>();

		internal class ResourceRequestInfo
		{
			internal ResourceRequest remoteRequest;

			internal LinkedHashSet<T> containerRequests;

			internal ResourceRequestInfo(AMRMClientImpl<T> _enclosing, Priority priority, string
				 resourceName, Resource capability, bool relaxLocality)
			{
				this._enclosing = _enclosing;
				// blacklistedNodes is required for keeping history of blacklisted nodes that
				// are sent to RM. On RESYNC command from RM, blacklistedNodes are used to get
				// current blacklisted nodes and send back to RM.
				this.remoteRequest = ResourceRequest.NewInstance(priority, resourceName, capability
					, 0);
				this.remoteRequest.SetRelaxLocality(relaxLocality);
				this.containerRequests = new LinkedHashSet<T>();
			}

			private readonly AMRMClientImpl<T> _enclosing;
		}

		/// <summary>Class compares Resource by memory then cpu in reverse order</summary>
		internal class ResourceReverseMemoryThenCpuComparator : IComparer<Resource>
		{
			public virtual int Compare(Resource arg0, Resource arg1)
			{
				int mem0 = arg0.GetMemory();
				int mem1 = arg1.GetMemory();
				int cpu0 = arg0.GetVirtualCores();
				int cpu1 = arg1.GetVirtualCores();
				if (mem0 == mem1)
				{
					if (cpu0 == cpu1)
					{
						return 0;
					}
					if (cpu0 < cpu1)
					{
						return 1;
					}
					return -1;
				}
				if (mem0 < mem1)
				{
					return 1;
				}
				return -1;
			}

			internal ResourceReverseMemoryThenCpuComparator(AMRMClientImpl<T> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly AMRMClientImpl<T> _enclosing;
		}

		internal static bool CanFit(Resource arg0, Resource arg1)
		{
			int mem0 = arg0.GetMemory();
			int mem1 = arg1.GetMemory();
			int cpu0 = arg0.GetVirtualCores();
			int cpu1 = arg1.GetVirtualCores();
			if (mem0 <= mem1 && cpu0 <= cpu1)
			{
				return true;
			}
			return false;
		}

		protected internal readonly IDictionary<Priority, IDictionary<string, SortedDictionary
			<Resource, AMRMClientImpl.ResourceRequestInfo>>> remoteRequestsTable = new SortedDictionary
			<Priority, IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
			>>>();

		protected internal readonly ICollection<ResourceRequest> ask = new TreeSet<ResourceRequest
			>(new ResourceRequest.ResourceRequestComparator());

		protected internal readonly ICollection<ContainerId> release = new TreeSet<ContainerId
			>();

		protected internal ICollection<ContainerId> pendingRelease = new TreeSet<ContainerId
			>();

		public AMRMClientImpl()
			: base(typeof(AMRMClientImpl).FullName)
		{
		}

		//Key -> Priority
		//Value -> Map
		//Key->ResourceName (e.g., nodename, rackname, *)
		//Value->Map
		//Key->Resource Capability
		//Value->ResourceRequest
		// pendingRelease holds history or release requests.request is removed only if
		// RM sends completedContainer.
		// How it different from release? --> release is for per allocate() request.
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			RackResolver.Init(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			YarnConfiguration conf = new YarnConfiguration(GetConfig());
			try
			{
				rmClient = ClientRMProxy.CreateRMProxy<ApplicationMasterProtocol>(conf);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.rmClient != null)
			{
				RPC.StopProxy(this.rmClient);
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override RegisterApplicationMasterResponse RegisterApplicationMaster(string
			 appHostName, int appHostPort, string appTrackingUrl)
		{
			this.appHostName = appHostName;
			this.appHostPort = appHostPort;
			this.appTrackingUrl = appTrackingUrl;
			Preconditions.CheckArgument(appHostName != null, "The host name should not be null"
				);
			Preconditions.CheckArgument(appHostPort >= -1, "Port number of the host" + " should be any integers larger than or equal to -1"
				);
			return RegisterApplicationMaster();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private RegisterApplicationMasterResponse RegisterApplicationMaster()
		{
			RegisterApplicationMasterRequest request = RegisterApplicationMasterRequest.NewInstance
				(this.appHostName, this.appHostPort, this.appTrackingUrl);
			RegisterApplicationMasterResponse response = rmClient.RegisterApplicationMaster(request
				);
			lock (this)
			{
				lastResponseId = 0;
				if (!response.GetNMTokensFromPreviousAttempts().IsEmpty())
				{
					PopulateNMTokens(response.GetNMTokensFromPreviousAttempts());
				}
			}
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override AllocateResponse Allocate(float progressIndicator)
		{
			Preconditions.CheckArgument(progressIndicator >= 0, "Progress indicator should not be negative"
				);
			AllocateResponse allocateResponse = null;
			IList<ResourceRequest> askList = null;
			IList<ContainerId> releaseList = null;
			AllocateRequest allocateRequest = null;
			IList<string> blacklistToAdd = new AList<string>();
			IList<string> blacklistToRemove = new AList<string>();
			try
			{
				lock (this)
				{
					askList = new AList<ResourceRequest>(ask.Count);
					foreach (ResourceRequest r in ask)
					{
						// create a copy of ResourceRequest as we might change it while the 
						// RPC layer is using it to send info across
						askList.AddItem(ResourceRequest.NewInstance(r.GetPriority(), r.GetResourceName(), 
							r.GetCapability(), r.GetNumContainers(), r.GetRelaxLocality(), r.GetNodeLabelExpression
							()));
					}
					releaseList = new AList<ContainerId>(release);
					// optimistically clear this collection assuming no RPC failure
					ask.Clear();
					release.Clear();
					Sharpen.Collections.AddAll(blacklistToAdd, blacklistAdditions);
					Sharpen.Collections.AddAll(blacklistToRemove, blacklistRemovals);
					ResourceBlacklistRequest blacklistRequest = ResourceBlacklistRequest.NewInstance(
						blacklistToAdd, blacklistToRemove);
					allocateRequest = AllocateRequest.NewInstance(lastResponseId, progressIndicator, 
						askList, releaseList, blacklistRequest);
					// clear blacklistAdditions and blacklistRemovals before 
					// unsynchronized part
					blacklistAdditions.Clear();
					blacklistRemovals.Clear();
				}
				try
				{
					allocateResponse = rmClient.Allocate(allocateRequest);
				}
				catch (ApplicationMasterNotRegisteredException)
				{
					Log.Warn("ApplicationMaster is out of sync with ResourceManager," + " hence resyncing."
						);
					lock (this)
					{
						Sharpen.Collections.AddAll(release, this.pendingRelease);
						Sharpen.Collections.AddAll(blacklistAdditions, this.blacklistedNodes);
						foreach (IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
							>> rr in remoteRequestsTable.Values)
						{
							foreach (IDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> capabalities in 
								rr.Values)
							{
								foreach (AMRMClientImpl.ResourceRequestInfo request in capabalities.Values)
								{
									AddResourceRequestToAsk(request.remoteRequest);
								}
							}
						}
					}
					// re register with RM
					RegisterApplicationMaster();
					allocateResponse = Allocate(progressIndicator);
					return allocateResponse;
				}
				lock (this)
				{
					// update these on successful RPC
					clusterNodeCount = allocateResponse.GetNumClusterNodes();
					lastResponseId = allocateResponse.GetResponseId();
					clusterAvailableResources = allocateResponse.GetAvailableResources();
					if (!allocateResponse.GetNMTokens().IsEmpty())
					{
						PopulateNMTokens(allocateResponse.GetNMTokens());
					}
					if (allocateResponse.GetAMRMToken() != null)
					{
						UpdateAMRMToken(allocateResponse.GetAMRMToken());
					}
					if (!pendingRelease.IsEmpty() && !allocateResponse.GetCompletedContainersStatuses
						().IsEmpty())
					{
						RemovePendingReleaseRequests(allocateResponse.GetCompletedContainersStatuses());
					}
				}
			}
			finally
			{
				// TODO how to differentiate remote yarn exception vs error in rpc
				if (allocateResponse == null)
				{
					// we hit an exception in allocate()
					// preserve ask and release for next call to allocate()
					lock (this)
					{
						Sharpen.Collections.AddAll(release, releaseList);
						// requests could have been added or deleted during call to allocate
						// If requests were added/removed then there is nothing to do since
						// the ResourceRequest object in ask would have the actual new value.
						// If ask does not have this ResourceRequest then it was unchanged and
						// so we can add the value back safely.
						// This assumes that there will no concurrent calls to allocate() and
						// so we dont have to worry about ask being changed in the
						// synchronized block at the beginning of this method.
						foreach (ResourceRequest oldAsk in askList)
						{
							if (!ask.Contains(oldAsk))
							{
								ask.AddItem(oldAsk);
							}
						}
						Sharpen.Collections.AddAll(blacklistAdditions, blacklistToAdd);
						Sharpen.Collections.AddAll(blacklistRemovals, blacklistToRemove);
					}
				}
			}
			return allocateResponse;
		}

		protected internal virtual void RemovePendingReleaseRequests(IList<ContainerStatus
			> completedContainersStatuses)
		{
			foreach (ContainerStatus containerStatus in completedContainersStatuses)
			{
				pendingRelease.Remove(containerStatus.GetContainerId());
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual void PopulateNMTokens(IList<NMToken> nmTokens)
		{
			foreach (NMToken token in nmTokens)
			{
				string nodeId = token.GetNodeId().ToString();
				if (GetNMTokenCache().ContainsToken(nodeId))
				{
					Log.Info("Replacing token for : " + nodeId);
				}
				else
				{
					Log.Info("Received new token for : " + nodeId);
				}
				GetNMTokenCache().SetToken(nodeId, token.GetToken());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void UnregisterApplicationMaster(FinalApplicationStatus appStatus
			, string appMessage, string appTrackingUrl)
		{
			Preconditions.CheckArgument(appStatus != null, "AppStatus should not be null.");
			FinishApplicationMasterRequest request = FinishApplicationMasterRequest.NewInstance
				(appStatus, appMessage, appTrackingUrl);
			try
			{
				while (true)
				{
					FinishApplicationMasterResponse response = rmClient.FinishApplicationMaster(request
						);
					if (response.GetIsUnregistered())
					{
						break;
					}
					Log.Info("Waiting for application to be successfully unregistered.");
					Sharpen.Thread.Sleep(100);
				}
			}
			catch (Exception)
			{
				Log.Info("Interrupted while waiting for application" + " to be removed from RMStateStore"
					);
			}
			catch (ApplicationMasterNotRegisteredException)
			{
				Log.Warn("ApplicationMaster is out of sync with ResourceManager," + " hence resyncing."
					);
				// re register with RM
				RegisterApplicationMaster();
				UnregisterApplicationMaster(appStatus, appMessage, appTrackingUrl);
			}
		}

		public override void AddContainerRequest(T req)
		{
			lock (this)
			{
				Preconditions.CheckArgument(req != null, "Resource request can not be null.");
				ICollection<string> dedupedRacks = new HashSet<string>();
				if (req.GetRacks() != null)
				{
					Sharpen.Collections.AddAll(dedupedRacks, req.GetRacks());
					if (req.GetRacks().Count != dedupedRacks.Count)
					{
						Joiner joiner = Joiner.On(',');
						Log.Warn("ContainerRequest has duplicate racks: " + joiner.Join(req.GetRacks()));
					}
				}
				ICollection<string> inferredRacks = ResolveRacks(req.GetNodes());
				inferredRacks.RemoveAll(dedupedRacks);
				// check that specific and non-specific requests cannot be mixed within a
				// priority
				CheckLocalityRelaxationConflict(req.GetPriority(), AnyList, req.GetRelaxLocality(
					));
				// check that specific rack cannot be mixed with specific node within a 
				// priority. If node and its rack are both specified then they must be 
				// in the same request.
				// For explicitly requested racks, we set locality relaxation to true
				CheckLocalityRelaxationConflict(req.GetPriority(), dedupedRacks, true);
				CheckLocalityRelaxationConflict(req.GetPriority(), inferredRacks, req.GetRelaxLocality
					());
				// check if the node label expression specified is valid
				CheckNodeLabelExpression(req);
				if (req.GetNodes() != null)
				{
					HashSet<string> dedupedNodes = new HashSet<string>(req.GetNodes());
					if (dedupedNodes.Count != req.GetNodes().Count)
					{
						Joiner joiner = Joiner.On(',');
						Log.Warn("ContainerRequest has duplicate nodes: " + joiner.Join(req.GetNodes()));
					}
					foreach (string node in dedupedNodes)
					{
						AddResourceRequest(req.GetPriority(), node, req.GetCapability(), req, true, req.GetNodeLabelExpression
							());
					}
				}
				foreach (string rack in dedupedRacks)
				{
					AddResourceRequest(req.GetPriority(), rack, req.GetCapability(), req, true, req.GetNodeLabelExpression
						());
				}
				// Ensure node requests are accompanied by requests for
				// corresponding rack
				foreach (string rack_1 in inferredRacks)
				{
					AddResourceRequest(req.GetPriority(), rack_1, req.GetCapability(), req, req.GetRelaxLocality
						(), req.GetNodeLabelExpression());
				}
				// Off-switch
				AddResourceRequest(req.GetPriority(), ResourceRequest.Any, req.GetCapability(), req
					, req.GetRelaxLocality(), req.GetNodeLabelExpression());
			}
		}

		public override void RemoveContainerRequest(T req)
		{
			lock (this)
			{
				Preconditions.CheckArgument(req != null, "Resource request can not be null.");
				ICollection<string> allRacks = new HashSet<string>();
				if (req.GetRacks() != null)
				{
					Sharpen.Collections.AddAll(allRacks, req.GetRacks());
				}
				Sharpen.Collections.AddAll(allRacks, ResolveRacks(req.GetNodes()));
				// Update resource requests
				if (req.GetNodes() != null)
				{
					foreach (string node in new HashSet<string>(req.GetNodes()))
					{
						DecResourceRequest(req.GetPriority(), node, req.GetCapability(), req);
					}
				}
				foreach (string rack in allRacks)
				{
					DecResourceRequest(req.GetPriority(), rack, req.GetCapability(), req);
				}
				DecResourceRequest(req.GetPriority(), ResourceRequest.Any, req.GetCapability(), req
					);
			}
		}

		public override void ReleaseAssignedContainer(ContainerId containerId)
		{
			lock (this)
			{
				Preconditions.CheckArgument(containerId != null, "ContainerId can not be null.");
				pendingRelease.AddItem(containerId);
				release.AddItem(containerId);
			}
		}

		public override Resource GetAvailableResources()
		{
			lock (this)
			{
				return clusterAvailableResources;
			}
		}

		public override int GetClusterNodeCount()
		{
			lock (this)
			{
				return clusterNodeCount;
			}
		}

		public override IList<ICollection<T>> GetMatchingRequests(Priority priority, string
			 resourceName, Resource capability)
		{
			lock (this)
			{
				Preconditions.CheckArgument(capability != null, "The Resource to be requested should not be null "
					);
				Preconditions.CheckArgument(priority != null, "The priority at which to request containers should not be null "
					);
				IList<LinkedHashSet<T>> list = new List<LinkedHashSet<T>>();
				IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
					>> remoteRequests = this.remoteRequestsTable[priority];
				if (remoteRequests == null)
				{
					return list;
				}
				SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> reqMap = remoteRequests
					[resourceName];
				if (reqMap == null)
				{
					return list;
				}
				AMRMClientImpl.ResourceRequestInfo resourceRequestInfo = reqMap[capability];
				if (resourceRequestInfo != null && !resourceRequestInfo.containerRequests.IsEmpty
					())
				{
					list.AddItem(resourceRequestInfo.containerRequests);
					return list;
				}
				// no exact match. Container may be larger than what was requested.
				// get all resources <= capability. map is reverse sorted. 
				SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> tailMap = reqMap.TailMap
					(capability);
				foreach (KeyValuePair<Resource, AMRMClientImpl.ResourceRequestInfo> entry in tailMap)
				{
					if (CanFit(entry.Key, capability) && !entry.Value.containerRequests.IsEmpty())
					{
						// match found that fits in the larger resource
						list.AddItem(entry.Value.containerRequests);
					}
				}
				// no match found
				return list;
			}
		}

		private ICollection<string> ResolveRacks(IList<string> nodes)
		{
			ICollection<string> racks = new HashSet<string>();
			if (nodes != null)
			{
				foreach (string node in nodes)
				{
					// Ensure node requests are accompanied by requests for
					// corresponding rack
					string rack = RackResolver.Resolve(node).GetNetworkLocation();
					if (rack == null)
					{
						Log.Warn("Failed to resolve rack for node " + node + ".");
					}
					else
					{
						racks.AddItem(rack);
					}
				}
			}
			return racks;
		}

		/// <summary>
		/// ContainerRequests with locality relaxation cannot be made at the same
		/// priority as ContainerRequests without locality relaxation.
		/// </summary>
		private void CheckLocalityRelaxationConflict(Priority priority, ICollection<string
			> locations, bool relaxLocality)
		{
			IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
				>> remoteRequests = this.remoteRequestsTable[priority];
			if (remoteRequests == null)
			{
				return;
			}
			// Locality relaxation will be set to relaxLocality for all implicitly
			// requested racks. Make sure that existing rack requests match this.
			foreach (string location in locations)
			{
				SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> reqs = remoteRequests
					[location];
				if (reqs != null && !reqs.IsEmpty())
				{
					bool existingRelaxLocality = reqs.Values.GetEnumerator().Next().remoteRequest.GetRelaxLocality
						();
					if (relaxLocality != existingRelaxLocality)
					{
						throw new InvalidContainerRequestException("Cannot submit a " + "ContainerRequest asking for location "
							 + location + " with locality relaxation " + relaxLocality + " when it has " + "already been requested with locality relaxation "
							 + existingRelaxLocality);
					}
				}
			}
		}

		/// <summary>
		/// Valid if a node label expression specified on container request is valid or
		/// not
		/// </summary>
		/// <param name="containerRequest"/>
		private void CheckNodeLabelExpression(T containerRequest)
		{
			string exp = containerRequest.GetNodeLabelExpression();
			if (null == exp || exp.IsEmpty())
			{
				return;
			}
			// Don't support specifying >= 2 node labels in a node label expression now
			if (exp.Contains("&&") || exp.Contains("||"))
			{
				throw new InvalidContainerRequestException("Cannot specify more than two node labels"
					 + " in a single node label expression");
			}
			// Don't allow specify node label against ANY request
			if ((containerRequest.GetRacks() != null && (!containerRequest.GetRacks().IsEmpty
				())) || (containerRequest.GetNodes() != null && (!containerRequest.GetNodes().IsEmpty
				())))
			{
				throw new InvalidContainerRequestException("Cannot specify node label with rack and node"
					);
			}
		}

		private void AddResourceRequestToAsk(ResourceRequest remoteRequest)
		{
			// This code looks weird but is needed because of the following scenario.
			// A ResourceRequest is removed from the remoteRequestTable. A 0 container 
			// request is added to 'ask' to notify the RM about not needing it any more.
			// Before the call to allocate, the user now requests more containers. If 
			// the locations of the 0 size request and the new request are the same
			// (with the difference being only container count), then the set comparator
			// will consider both to be the same and not add the new request to ask. So 
			// we need to check for the "same" request being present and remove it and 
			// then add it back. The comparator is container count agnostic.
			// This should happen only rarely but we do need to guard against it.
			if (ask.Contains(remoteRequest))
			{
				ask.Remove(remoteRequest);
			}
			ask.AddItem(remoteRequest);
		}

		private void AddResourceRequest(Priority priority, string resourceName, Resource 
			capability, T req, bool relaxLocality, string labelExpression)
		{
			IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
				>> remoteRequests = this.remoteRequestsTable[priority];
			if (remoteRequests == null)
			{
				remoteRequests = new Dictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
					>>();
				this.remoteRequestsTable[priority] = remoteRequests;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Added priority=" + priority);
				}
			}
			SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> reqMap = remoteRequests
				[resourceName];
			if (reqMap == null)
			{
				// capabilities are stored in reverse sorted order. smallest last.
				reqMap = new SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo>(new AMRMClientImpl.ResourceReverseMemoryThenCpuComparator
					(this));
				remoteRequests[resourceName] = reqMap;
			}
			AMRMClientImpl.ResourceRequestInfo resourceRequestInfo = reqMap[capability];
			if (resourceRequestInfo == null)
			{
				resourceRequestInfo = new AMRMClientImpl.ResourceRequestInfo(this, priority, resourceName
					, capability, relaxLocality);
				reqMap[capability] = resourceRequestInfo;
			}
			resourceRequestInfo.remoteRequest.SetNumContainers(resourceRequestInfo.remoteRequest
				.GetNumContainers() + 1);
			if (relaxLocality)
			{
				resourceRequestInfo.containerRequests.AddItem(req);
			}
			if (ResourceRequest.Any.Equals(resourceName))
			{
				resourceRequestInfo.remoteRequest.SetNodeLabelExpression(labelExpression);
			}
			// Note this down for next interaction with ResourceManager
			AddResourceRequestToAsk(resourceRequestInfo.remoteRequest);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("addResourceRequest:" + " applicationId=" + " priority=" + priority.GetPriority
					() + " resourceName=" + resourceName + " numContainers=" + resourceRequestInfo.remoteRequest
					.GetNumContainers() + " #asks=" + ask.Count);
			}
		}

		private void DecResourceRequest(Priority priority, string resourceName, Resource 
			capability, T req)
		{
			IDictionary<string, SortedDictionary<Resource, AMRMClientImpl.ResourceRequestInfo
				>> remoteRequests = this.remoteRequestsTable[priority];
			if (remoteRequests == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Not decrementing resource as priority " + priority + " is not present in request table"
						);
				}
				return;
			}
			IDictionary<Resource, AMRMClientImpl.ResourceRequestInfo> reqMap = remoteRequests
				[resourceName];
			if (reqMap == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Not decrementing resource as " + resourceName + " is not present in request table"
						);
				}
				return;
			}
			AMRMClientImpl.ResourceRequestInfo resourceRequestInfo = reqMap[capability];
			if (Log.IsDebugEnabled())
			{
				Log.Debug("BEFORE decResourceRequest:" + " applicationId=" + " priority=" + priority
					.GetPriority() + " resourceName=" + resourceName + " numContainers=" + resourceRequestInfo
					.remoteRequest.GetNumContainers() + " #asks=" + ask.Count);
			}
			resourceRequestInfo.remoteRequest.SetNumContainers(resourceRequestInfo.remoteRequest
				.GetNumContainers() - 1);
			resourceRequestInfo.containerRequests.Remove(req);
			if (resourceRequestInfo.remoteRequest.GetNumContainers() < 0)
			{
				// guard against spurious removals
				resourceRequestInfo.remoteRequest.SetNumContainers(0);
			}
			// send the ResourceRequest to RM even if is 0 because it needs to override
			// a previously sent value. If ResourceRequest was not sent previously then
			// sending 0 aught to be a no-op on RM
			AddResourceRequestToAsk(resourceRequestInfo.remoteRequest);
			// delete entries from map if no longer needed
			if (resourceRequestInfo.remoteRequest.GetNumContainers() == 0)
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
			if (Log.IsDebugEnabled())
			{
				Log.Info("AFTER decResourceRequest:" + " applicationId=" + " priority=" + priority
					.GetPriority() + " resourceName=" + resourceName + " numContainers=" + resourceRequestInfo
					.remoteRequest.GetNumContainers() + " #asks=" + ask.Count);
			}
		}

		public override void UpdateBlacklist(IList<string> blacklistAdditions, IList<string
			> blacklistRemovals)
		{
			lock (this)
			{
				if (blacklistAdditions != null)
				{
					Sharpen.Collections.AddAll(this.blacklistAdditions, blacklistAdditions);
					Sharpen.Collections.AddAll(this.blacklistedNodes, blacklistAdditions);
					// if some resources are also in blacklistRemovals updated before, we 
					// should remove them here.
					this.blacklistRemovals.RemoveAll(blacklistAdditions);
				}
				if (blacklistRemovals != null)
				{
					Sharpen.Collections.AddAll(this.blacklistRemovals, blacklistRemovals);
					this.blacklistedNodes.RemoveAll(blacklistRemovals);
					// if some resources are in blacklistAdditions before, we should remove
					// them here.
					this.blacklistAdditions.RemoveAll(blacklistRemovals);
				}
				if (blacklistAdditions != null && blacklistRemovals != null && blacklistAdditions
					.RemoveAll(blacklistRemovals))
				{
					// we allow resources to appear in addition list and removal list in the
					// same invocation of updateBlacklist(), but should get a warn here.
					Log.Warn("The same resources appear in both blacklistAdditions and " + "blacklistRemovals in updateBlacklist."
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateAMRMToken(Token token)
		{
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(((byte[])token.GetIdentifier().Array()), ((byte[])token.GetPassword
				().Array()), new Text(token.GetKind()), new Text(token.GetService()));
			// Preserve the token service sent by the RM when adding the token
			// to ensure we replace the previous token setup by the RM.
			// Afterwards we can update the service address for the RPC layer.
			UserGroupInformation currentUGI = UserGroupInformation.GetCurrentUser();
			currentUGI.AddToken(amrmToken);
			amrmToken.SetService(ClientRMProxy.GetAMRMTokenService(GetConfig()));
		}
	}
}
