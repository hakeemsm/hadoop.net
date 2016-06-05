using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	/// <summary>Allocates the container from the ResourceManager scheduler.</summary>
	public class RMContainerAllocator : RMContainerRequestor, ContainerAllocator
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.RM.RMContainerAllocator
			));

		public const float DefaultCompletedMapsPercentForReduceSlowstart = 0.05f;

		internal static readonly Priority PriorityFastFailMap;

		internal static readonly Priority PriorityReduce;

		internal static readonly Priority PriorityMap;

		[VisibleForTesting]
		public const string RampdownDiagnostic = "Reducer preempted " + "to make room for pending map attempts";

		private Sharpen.Thread eventHandlingThread;

		private readonly AtomicBoolean stopped;

		static RMContainerAllocator()
		{
			assignedRequests = new RMContainerAllocator.AssignedRequests(this);
			scheduledRequests = new RMContainerAllocator.ScheduledRequests(this);
			scheduleStats = new RMContainerAllocator.ScheduleStats(this);
			PriorityFastFailMap = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance
				<Priority>();
			PriorityFastFailMap.SetPriority(5);
			PriorityReduce = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<Priority
				>();
			PriorityReduce.SetPriority(10);
			PriorityMap = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<Priority
				>();
			PriorityMap.SetPriority(20);
		}

		private readonly List<RMContainerRequestor.ContainerRequest> pendingReduces = new 
			List<RMContainerRequestor.ContainerRequest>();

		private readonly RMContainerAllocator.AssignedRequests assignedRequests;

		private readonly RMContainerAllocator.ScheduledRequests scheduledRequests;

		private int containersAllocated = 0;

		private int containersReleased = 0;

		private int hostLocalAssigned = 0;

		private int rackLocalAssigned = 0;

		private int lastCompletedTasks = 0;

		private bool recalculateReduceSchedule = false;

		private Resource mapResourceRequest = Resources.None();

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource reduceResourceRequest = Resources
			.None();

		private bool reduceStarted = false;

		private float maxReduceRampupLimit = 0;

		private float maxReducePreemptionLimit = 0;

		/// <summary>
		/// after this threshold, if the container request is not allocated, it is
		/// considered delayed.
		/// </summary>
		private long allocationDelayThresholdMs = 0;

		private float reduceSlowStart = 0;

		private int maxRunningMaps = 0;

		private int maxRunningReduces = 0;

		private long retryInterval;

		private long retrystartTime;

		private Clock clock;

		[VisibleForTesting]
		protected internal BlockingQueue<ContainerAllocatorEvent> eventQueue = new LinkedBlockingQueue
			<ContainerAllocatorEvent>();

		private RMContainerAllocator.ScheduleStats scheduleStats;

		public RMContainerAllocator(ClientService clientService, AppContext context)
			: base(clientService, context)
		{
			assignedRequests = new RMContainerAllocator.AssignedRequests(this);
			scheduledRequests = new RMContainerAllocator.ScheduledRequests(this);
			scheduleStats = new RMContainerAllocator.ScheduleStats(this);
			/*
			Vocabulary Used:
			pending -> requests which are NOT yet sent to RM
			scheduled -> requests which are sent to RM but not yet assigned
			assigned -> requests which are assigned to a container
			completed -> request corresponding to which container has completed
			
			Lifecycle of map
			scheduled->assigned->completed
			
			Lifecycle of reduce
			pending->scheduled->assigned->completed
			
			Maps are scheduled as soon as their requests are received. Reduces are
			added to the pending and are ramped up (added to scheduled) based
			on completed maps and current availability in the cluster.
			*/
			//reduces which are not yet scheduled
			//holds information about the assigned containers to task attempts
			//holds scheduled requests to be fulfilled by RM
			this.stopped = new AtomicBoolean(false);
			this.clock = context.GetClock();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			reduceSlowStart = conf.GetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, DefaultCompletedMapsPercentForReduceSlowstart
				);
			maxReduceRampupLimit = conf.GetFloat(MRJobConfig.MrAmJobReduceRampupUpLimit, MRJobConfig
				.DefaultMrAmJobReduceRampUpLimit);
			maxReducePreemptionLimit = conf.GetFloat(MRJobConfig.MrAmJobReducePreemptionLimit
				, MRJobConfig.DefaultMrAmJobReducePreemptionLimit);
			allocationDelayThresholdMs = conf.GetInt(MRJobConfig.MrJobReducerPreemptDelaySec, 
				MRJobConfig.DefaultMrJobReducerPreemptDelaySec) * 1000;
			//sec -> ms
			maxRunningMaps = conf.GetInt(MRJobConfig.JobRunningMapLimit, MRJobConfig.DefaultJobRunningMapLimit
				);
			maxRunningReduces = conf.GetInt(MRJobConfig.JobRunningReduceLimit, MRJobConfig.DefaultJobRunningReduceLimit
				);
			RackResolver.Init(conf);
			retryInterval = GetConfig().GetLong(MRJobConfig.MrAmToRmWaitIntervalMs, MRJobConfig
				.DefaultMrAmToRmWaitIntervalMs);
			// Init startTime to current time. If all goes well, it will be reset after
			// first attempt to contact RM.
			retrystartTime = Runtime.CurrentTimeMillis();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			this.eventHandlingThread = new _Thread_214(this);
			// Kill the AM
			this.eventHandlingThread.Start();
			base.ServiceStart();
		}

		private sealed class _Thread_214 : Sharpen.Thread
		{
			public _Thread_214(RMContainerAllocator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				ContainerAllocatorEvent @event;
				while (!this._enclosing.stopped.Get() && !Sharpen.Thread.CurrentThread().IsInterrupted
					())
				{
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception e)
					{
						if (!this._enclosing.stopped.Get())
						{
							Org.Apache.Hadoop.Mapreduce.V2.App.RM.RMContainerAllocator.Log.Error("Returning, interrupted : "
								 + e);
						}
						return;
					}
					try
					{
						this._enclosing.HandleEvent(@event);
					}
					catch (Exception t)
					{
						Org.Apache.Hadoop.Mapreduce.V2.App.RM.RMContainerAllocator.Log.Error("Error in handling event type "
							 + @event.GetType() + " to the ContainreAllocator", t);
						this._enclosing.eventHandler.Handle(new JobEvent(this._enclosing.GetJob().GetID()
							, JobEventType.InternalError));
						return;
					}
				}
			}

			private readonly RMContainerAllocator _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void Heartbeat()
		{
			lock (this)
			{
				scheduleStats.UpdateAndLogIfChanged("Before Scheduling: ");
				IList<Container> allocatedContainers = GetResources();
				if (allocatedContainers != null && allocatedContainers.Count > 0)
				{
					scheduledRequests.Assign(allocatedContainers);
				}
				int completedMaps = GetJob().GetCompletedMaps();
				int completedTasks = completedMaps + GetJob().GetCompletedReduces();
				if ((lastCompletedTasks != completedTasks) || (scheduledRequests.maps.Count > 0))
				{
					lastCompletedTasks = completedTasks;
					recalculateReduceSchedule = true;
				}
				if (recalculateReduceSchedule)
				{
					PreemptReducesIfNeeded();
					ScheduleReduces(GetJob().GetTotalMaps(), completedMaps, scheduledRequests.maps.Count
						, scheduledRequests.reduces.Count, assignedRequests.maps.Count, assignedRequests
						.reduces.Count, mapResourceRequest, reduceResourceRequest, pendingReduces.Count, 
						maxReduceRampupLimit, reduceSlowStart);
					recalculateReduceSchedule = false;
				}
				scheduleStats.UpdateAndLogIfChanged("After Scheduling: ");
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stopped.GetAndSet(true))
			{
				// return if already stopped
				return;
			}
			if (eventHandlingThread != null)
			{
				eventHandlingThread.Interrupt();
			}
			base.ServiceStop();
			scheduleStats.Log("Final Stats: ");
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual RMContainerAllocator.AssignedRequests GetAssignedRequests()
		{
			return assignedRequests;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual RMContainerAllocator.ScheduledRequests GetScheduledRequests()
		{
			return scheduledRequests;
		}

		public virtual bool GetIsReduceStarted()
		{
			return reduceStarted;
		}

		public virtual void SetIsReduceStarted(bool reduceStarted)
		{
			this.reduceStarted = reduceStarted;
		}

		public virtual void Handle(ContainerAllocatorEvent @event)
		{
			int qSize = eventQueue.Count;
			if (qSize != 0 && qSize % 1000 == 0)
			{
				Log.Info("Size of event-queue in RMContainerAllocator is " + qSize);
			}
			int remCapacity = eventQueue.RemainingCapacity();
			if (remCapacity < 1000)
			{
				Log.Warn("Very low remaining capacity in the event-queue " + "of RMContainerAllocator: "
					 + remCapacity);
			}
			try
			{
				eventQueue.Put(@event);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		protected internal virtual void HandleEvent(ContainerAllocatorEvent @event)
		{
			lock (this)
			{
				recalculateReduceSchedule = true;
				if (@event.GetType() == ContainerAllocator.EventType.ContainerReq)
				{
					ContainerRequestEvent reqEvent = (ContainerRequestEvent)@event;
					JobId jobId = GetJob().GetID();
					Org.Apache.Hadoop.Yarn.Api.Records.Resource supportedMaxContainerCapability = GetMaxContainerCapability
						();
					if (reqEvent.GetAttemptID().GetTaskId().GetTaskType().Equals(TaskType.Map))
					{
						if (mapResourceRequest.Equals(Resources.None()))
						{
							mapResourceRequest = reqEvent.GetCapability();
							eventHandler.Handle(new JobHistoryEvent(jobId, new NormalizedResourceEvent(TaskType
								.Map, mapResourceRequest.GetMemory())));
							Log.Info("mapResourceRequest:" + mapResourceRequest);
							if (mapResourceRequest.GetMemory() > supportedMaxContainerCapability.GetMemory() 
								|| mapResourceRequest.GetVirtualCores() > supportedMaxContainerCapability.GetVirtualCores
								())
							{
								string diagMsg = "MAP capability required is more than the supported " + "max container capability in the cluster. Killing the Job. mapResourceRequest: "
									 + mapResourceRequest + " maxContainerCapability:" + supportedMaxContainerCapability;
								Log.Info(diagMsg);
								eventHandler.Handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
								eventHandler.Handle(new JobEvent(jobId, JobEventType.JobKill));
							}
						}
						// set the resources
						reqEvent.GetCapability().SetMemory(mapResourceRequest.GetMemory());
						reqEvent.GetCapability().SetVirtualCores(mapResourceRequest.GetVirtualCores());
						scheduledRequests.AddMap(reqEvent);
					}
					else
					{
						//maps are immediately scheduled
						if (reduceResourceRequest.Equals(Resources.None()))
						{
							reduceResourceRequest = reqEvent.GetCapability();
							eventHandler.Handle(new JobHistoryEvent(jobId, new NormalizedResourceEvent(TaskType
								.Reduce, reduceResourceRequest.GetMemory())));
							Log.Info("reduceResourceRequest:" + reduceResourceRequest);
							if (reduceResourceRequest.GetMemory() > supportedMaxContainerCapability.GetMemory
								() || reduceResourceRequest.GetVirtualCores() > supportedMaxContainerCapability.
								GetVirtualCores())
							{
								string diagMsg = "REDUCE capability required is more than the " + "supported max container capability in the cluster. Killing the "
									 + "Job. reduceResourceRequest: " + reduceResourceRequest + " maxContainerCapability:"
									 + supportedMaxContainerCapability;
								Log.Info(diagMsg);
								eventHandler.Handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
								eventHandler.Handle(new JobEvent(jobId, JobEventType.JobKill));
							}
						}
						// set the resources
						reqEvent.GetCapability().SetMemory(reduceResourceRequest.GetMemory());
						reqEvent.GetCapability().SetVirtualCores(reduceResourceRequest.GetVirtualCores());
						if (reqEvent.GetEarlierAttemptFailed())
						{
							//add to the front of queue for fail fast
							pendingReduces.AddFirst(new RMContainerRequestor.ContainerRequest(reqEvent, PriorityReduce
								));
						}
						else
						{
							pendingReduces.AddItem(new RMContainerRequestor.ContainerRequest(reqEvent, PriorityReduce
								));
						}
					}
				}
				else
				{
					//reduces are added to pending and are slowly ramped up
					if (@event.GetType() == ContainerAllocator.EventType.ContainerDeallocate)
					{
						Log.Info("Processing the event " + @event.ToString());
						TaskAttemptId aId = @event.GetAttemptID();
						bool removed = scheduledRequests.Remove(aId);
						if (!removed)
						{
							ContainerId containerId = assignedRequests.Get(aId);
							if (containerId != null)
							{
								removed = true;
								assignedRequests.Remove(aId);
								containersReleased++;
								pendingRelease.AddItem(containerId);
								Release(containerId);
							}
						}
						if (!removed)
						{
							Log.Error("Could not deallocate container for task attemptId " + aId);
						}
					}
					else
					{
						if (@event.GetType() == ContainerAllocator.EventType.ContainerFailed)
						{
							ContainerFailedEvent fEv = (ContainerFailedEvent)@event;
							string host = GetHost(fEv.GetContMgrAddress());
							ContainerFailedOnHost(host);
						}
					}
				}
			}
		}

		private static string GetHost(string contMgrAddress)
		{
			string host = contMgrAddress;
			string[] hostport = host.Split(":");
			if (hostport.Length == 2)
			{
				host = hostport[0];
			}
			return host;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual void SetReduceResourceRequest(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			lock (this)
			{
				this.reduceResourceRequest = res;
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual void SetMapResourceRequest(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			lock (this)
			{
				this.mapResourceRequest = res;
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual void PreemptReducesIfNeeded()
		{
			if (reduceResourceRequest.Equals(Resources.None()))
			{
				return;
			}
			// no reduces
			//check if reduces have taken over the whole cluster and there are 
			//unassigned maps
			if (scheduledRequests.maps.Count > 0)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resourceLimit = GetResourceLimit();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource availableResourceForMap = Resources.Subtract
					(resourceLimit, Resources.Multiply(reduceResourceRequest, assignedRequests.reduces
					.Count - assignedRequests.preemptionWaitingReduces.Count));
				// availableMemForMap must be sufficient to run at least 1 map
				if (ResourceCalculatorUtils.ComputeAvailableContainers(availableResourceForMap, mapResourceRequest
					, GetSchedulerResourceTypes()) <= 0)
				{
					// to make sure new containers are given to maps and not reduces
					// ramp down all scheduled reduces if any
					// (since reduces are scheduled at higher priority than maps)
					Log.Info("Ramping down all scheduled reduces:" + scheduledRequests.reduces.Count);
					foreach (RMContainerRequestor.ContainerRequest req in scheduledRequests.reduces.Values)
					{
						pendingReduces.AddItem(req);
					}
					scheduledRequests.reduces.Clear();
					//do further checking to find the number of map requests that were
					//hanging around for a while
					int hangingMapRequests = GetNumOfHangingRequests(scheduledRequests.maps);
					if (hangingMapRequests > 0)
					{
						// preempt for making space for at least one map
						int preemptionReduceNumForOneMap = ResourceCalculatorUtils.DivideAndCeilContainers
							(mapResourceRequest, reduceResourceRequest, GetSchedulerResourceTypes());
						int preemptionReduceNumForPreemptionLimit = ResourceCalculatorUtils.DivideAndCeilContainers
							(Resources.Multiply(resourceLimit, maxReducePreemptionLimit), reduceResourceRequest
							, GetSchedulerResourceTypes());
						int preemptionReduceNumForAllMaps = ResourceCalculatorUtils.DivideAndCeilContainers
							(Resources.Multiply(mapResourceRequest, hangingMapRequests), reduceResourceRequest
							, GetSchedulerResourceTypes());
						int toPreempt = Math.Min(Math.Max(preemptionReduceNumForOneMap, preemptionReduceNumForPreemptionLimit
							), preemptionReduceNumForAllMaps);
						Log.Info("Going to preempt " + toPreempt + " due to lack of space for maps");
						assignedRequests.PreemptReduce(toPreempt);
					}
				}
			}
		}

		private int GetNumOfHangingRequests(IDictionary<TaskAttemptId, RMContainerRequestor.ContainerRequest
			> requestMap)
		{
			if (allocationDelayThresholdMs <= 0)
			{
				return requestMap.Count;
			}
			int hangingRequests = 0;
			long currTime = clock.GetTime();
			foreach (RMContainerRequestor.ContainerRequest request in requestMap.Values)
			{
				long delay = currTime - request.requestTimeMs;
				if (delay > allocationDelayThresholdMs)
				{
					hangingRequests++;
				}
			}
			return hangingRequests;
		}

		[InterfaceAudience.Private]
		public virtual void ScheduleReduces(int totalMaps, int completedMaps, int scheduledMaps
			, int scheduledReduces, int assignedMaps, int assignedReduces, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 mapResourceReqt, Org.Apache.Hadoop.Yarn.Api.Records.Resource reduceResourceReqt
			, int numPendingReduces, float maxReduceRampupLimit, float reduceSlowStart)
		{
			if (numPendingReduces == 0)
			{
				return;
			}
			// get available resources for this job
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headRoom = GetAvailableResources();
			if (headRoom == null)
			{
				headRoom = Resources.None();
			}
			Log.Info("Recalculating schedule, headroom=" + headRoom);
			//check for slow start
			if (!GetIsReduceStarted())
			{
				//not set yet
				int completedMapsForReduceSlowstart = (int)Math.Ceil(reduceSlowStart * totalMaps);
				if (completedMaps < completedMapsForReduceSlowstart)
				{
					Log.Info("Reduce slow start threshold not met. " + "completedMapsForReduceSlowstart "
						 + completedMapsForReduceSlowstart);
					return;
				}
				else
				{
					Log.Info("Reduce slow start threshold reached. Scheduling reduces.");
					SetIsReduceStarted(true);
				}
			}
			//if all maps are assigned, then ramp up all reduces irrespective of the
			//headroom
			if (scheduledMaps == 0 && numPendingReduces > 0)
			{
				Log.Info("All maps assigned. " + "Ramping up all remaining reduces:" + numPendingReduces
					);
				ScheduleAllReduces();
				return;
			}
			float completedMapPercent = 0f;
			if (totalMaps != 0)
			{
				//support for 0 maps
				completedMapPercent = (float)completedMaps / totalMaps;
			}
			else
			{
				completedMapPercent = 1;
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource netScheduledMapResource = Resources.Multiply
				(mapResourceReqt, (scheduledMaps + assignedMaps));
			Org.Apache.Hadoop.Yarn.Api.Records.Resource netScheduledReduceResource = Resources
				.Multiply(reduceResourceReqt, (scheduledReduces + assignedReduces));
			Org.Apache.Hadoop.Yarn.Api.Records.Resource finalMapResourceLimit;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource finalReduceResourceLimit;
			// ramp up the reduces based on completed map percentage
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalResourceLimit = GetResourceLimit
				();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource idealReduceResourceLimit = Resources.
				Multiply(totalResourceLimit, Math.Min(completedMapPercent, maxReduceRampupLimit)
				);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource ideaMapResourceLimit = Resources.Subtract
				(totalResourceLimit, idealReduceResourceLimit);
			// check if there aren't enough maps scheduled, give the free map capacity
			// to reduce.
			// Even when container number equals, there may be unused resources in one
			// dimension
			if (ResourceCalculatorUtils.ComputeAvailableContainers(ideaMapResourceLimit, mapResourceReqt
				, GetSchedulerResourceTypes()) >= (scheduledMaps + assignedMaps))
			{
				// enough resource given to maps, given the remaining to reduces
				Org.Apache.Hadoop.Yarn.Api.Records.Resource unusedMapResourceLimit = Resources.Subtract
					(ideaMapResourceLimit, netScheduledMapResource);
				finalReduceResourceLimit = Resources.Add(idealReduceResourceLimit, unusedMapResourceLimit
					);
				finalMapResourceLimit = Resources.Subtract(totalResourceLimit, finalReduceResourceLimit
					);
			}
			else
			{
				finalMapResourceLimit = ideaMapResourceLimit;
				finalReduceResourceLimit = idealReduceResourceLimit;
			}
			Log.Info("completedMapPercent " + completedMapPercent + " totalResourceLimit:" + 
				totalResourceLimit + " finalMapResourceLimit:" + finalMapResourceLimit + " finalReduceResourceLimit:"
				 + finalReduceResourceLimit + " netScheduledMapResource:" + netScheduledMapResource
				 + " netScheduledReduceResource:" + netScheduledReduceResource);
			int rampUp = ResourceCalculatorUtils.ComputeAvailableContainers(Resources.Subtract
				(finalReduceResourceLimit, netScheduledReduceResource), reduceResourceReqt, GetSchedulerResourceTypes
				());
			if (rampUp > 0)
			{
				rampUp = Math.Min(rampUp, numPendingReduces);
				Log.Info("Ramping up " + rampUp);
				RampUpReduces(rampUp);
			}
			else
			{
				if (rampUp < 0)
				{
					int rampDown = -1 * rampUp;
					rampDown = Math.Min(rampDown, scheduledReduces);
					Log.Info("Ramping down " + rampDown);
					RampDownReduces(rampDown);
				}
			}
		}

		[InterfaceAudience.Private]
		public virtual void ScheduleAllReduces()
		{
			foreach (RMContainerRequestor.ContainerRequest req in pendingReduces)
			{
				scheduledRequests.AddReduce(req);
			}
			pendingReduces.Clear();
		}

		[InterfaceAudience.Private]
		public virtual void RampUpReduces(int rampUp)
		{
			//more reduce to be scheduled
			for (int i = 0; i < rampUp; i++)
			{
				RMContainerRequestor.ContainerRequest request = pendingReduces.RemoveFirst();
				scheduledRequests.AddReduce(request);
			}
		}

		[InterfaceAudience.Private]
		public virtual void RampDownReduces(int rampDown)
		{
			//remove from the scheduled and move back to pending
			for (int i = 0; i < rampDown; i++)
			{
				RMContainerRequestor.ContainerRequest request = scheduledRequests.RemoveReduce();
				pendingReduces.AddItem(request);
			}
		}

		/// <exception cref="System.Exception"/>
		private IList<Container> GetResources()
		{
			ApplyConcurrentTaskLimits();
			// will be null the first time
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headRoom = GetAvailableResources() ==
				 null ? Resources.None() : Resources.Clone(GetAvailableResources());
			AllocateResponse response;
			/*
			* If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
			* milliseconds before aborting. During this interval, AM will still try
			* to contact the RM.
			*/
			try
			{
				response = MakeRemoteRequest();
				// Reset retry count if no exception occurred.
				retrystartTime = Runtime.CurrentTimeMillis();
			}
			catch (ApplicationAttemptNotFoundException e)
			{
				// This can happen if the RM has been restarted. If it is in that state,
				// this application must clean itself up.
				eventHandler.Handle(new JobEvent(this.GetJob().GetID(), JobEventType.JobAmReboot)
					);
				throw new RMContainerAllocationException("Resource Manager doesn't recognize AttemptId: "
					 + this.GetContext().GetApplicationAttemptId(), e);
			}
			catch (ApplicationMasterNotRegisteredException)
			{
				Log.Info("ApplicationMaster is out of sync with ResourceManager," + " hence resync and send outstanding requests."
					);
				// RM may have restarted, re-register with RM.
				lastResponseID = 0;
				Register();
				AddOutstandingRequestOnResync();
				return null;
			}
			catch (Exception e)
			{
				// This can happen when the connection to the RM has gone down. Keep
				// re-trying until the retryInterval has expired.
				if (Runtime.CurrentTimeMillis() - retrystartTime >= retryInterval)
				{
					Log.Error("Could not contact RM after " + retryInterval + " milliseconds.");
					eventHandler.Handle(new JobEvent(this.GetJob().GetID(), JobEventType.JobAmReboot)
						);
					throw new RMContainerAllocationException("Could not contact RM after " + retryInterval
						 + " milliseconds.");
				}
				// Throw this up to the caller, which may decide to ignore it and
				// continue to attempt to contact the RM.
				throw;
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newHeadRoom = GetAvailableResources()
				 == null ? Resources.None() : GetAvailableResources();
			IList<Container> newContainers = response.GetAllocatedContainers();
			// Setting NMTokens
			if (response.GetNMTokens() != null)
			{
				foreach (NMToken nmToken in response.GetNMTokens())
				{
					NMTokenCache.SetNMToken(nmToken.GetNodeId().ToString(), nmToken.GetToken());
				}
			}
			// Setting AMRMToken
			if (response.GetAMRMToken() != null)
			{
				UpdateAMRMToken(response.GetAMRMToken());
			}
			IList<ContainerStatus> finishedContainers = response.GetCompletedContainersStatuses
				();
			if (newContainers.Count + finishedContainers.Count > 0 || !headRoom.Equals(newHeadRoom
				))
			{
				//something changed
				recalculateReduceSchedule = true;
				if (Log.IsDebugEnabled() && !headRoom.Equals(newHeadRoom))
				{
					Log.Debug("headroom=" + newHeadRoom);
				}
			}
			if (Log.IsDebugEnabled())
			{
				foreach (Container cont in newContainers)
				{
					Log.Debug("Received new Container :" + cont);
				}
			}
			//Called on each allocation. Will know about newly blacklisted/added hosts.
			ComputeIgnoreBlacklisting();
			HandleUpdatedNodes(response);
			foreach (ContainerStatus cont_1 in finishedContainers)
			{
				Log.Info("Received completed container " + cont_1.GetContainerId());
				TaskAttemptId attemptID = assignedRequests.Get(cont_1.GetContainerId());
				if (attemptID == null)
				{
					Log.Error("Container complete event for unknown container id " + cont_1.GetContainerId
						());
				}
				else
				{
					pendingRelease.Remove(cont_1.GetContainerId());
					assignedRequests.Remove(attemptID);
					// send the container completed event to Task attempt
					eventHandler.Handle(CreateContainerFinishedEvent(cont_1, attemptID));
					// Send the diagnostics
					string diagnostics = StringInterner.WeakIntern(cont_1.GetDiagnostics());
					eventHandler.Handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID, diagnostics)
						);
				}
			}
			return newContainers;
		}

		private void ApplyConcurrentTaskLimits()
		{
			int numScheduledMaps = scheduledRequests.maps.Count;
			if (maxRunningMaps > 0 && numScheduledMaps > 0)
			{
				int maxRequestedMaps = Math.Max(0, maxRunningMaps - assignedRequests.maps.Count);
				int numScheduledFailMaps = scheduledRequests.earlierFailedMaps.Count;
				int failedMapRequestLimit = Math.Min(maxRequestedMaps, numScheduledFailMaps);
				int normalMapRequestLimit = Math.Min(maxRequestedMaps - failedMapRequestLimit, numScheduledMaps
					 - numScheduledFailMaps);
				SetRequestLimit(PriorityFastFailMap, mapResourceRequest, failedMapRequestLimit);
				SetRequestLimit(PriorityMap, mapResourceRequest, normalMapRequestLimit);
			}
			int numScheduledReduces = scheduledRequests.reduces.Count;
			if (maxRunningReduces > 0 && numScheduledReduces > 0)
			{
				int maxRequestedReduces = Math.Max(0, maxRunningReduces - assignedRequests.reduces
					.Count);
				int reduceRequestLimit = Math.Min(maxRequestedReduces, numScheduledReduces);
				SetRequestLimit(PriorityReduce, reduceResourceRequest, reduceRequestLimit);
			}
		}

		private bool CanAssignMaps()
		{
			return (maxRunningMaps <= 0 || assignedRequests.maps.Count < maxRunningMaps);
		}

		private bool CanAssignReduces()
		{
			return (maxRunningReduces <= 0 || assignedRequests.reduces.Count < maxRunningReduces
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateAMRMToken(Token token)
		{
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(((byte[])token.GetIdentifier().Array()), ((byte[])token.GetPassword
				().Array()), new Text(token.GetKind()), new Text(token.GetService()));
			UserGroupInformation currentUGI = UserGroupInformation.GetCurrentUser();
			currentUGI.AddToken(amrmToken);
			amrmToken.SetService(ClientRMProxy.GetAMRMTokenService(GetConfig()));
		}

		[VisibleForTesting]
		public virtual TaskAttemptEvent CreateContainerFinishedEvent(ContainerStatus cont
			, TaskAttemptId attemptID)
		{
			if (cont.GetExitStatus() == ContainerExitStatus.Aborted || cont.GetExitStatus() ==
				 ContainerExitStatus.Preempted)
			{
				// killed by framework
				return new TaskAttemptEvent(attemptID, TaskAttemptEventType.TaKill);
			}
			else
			{
				return new TaskAttemptEvent(attemptID, TaskAttemptEventType.TaContainerCompleted);
			}
		}

		private void HandleUpdatedNodes(AllocateResponse response)
		{
			// send event to the job about on updated nodes
			IList<NodeReport> updatedNodes = response.GetUpdatedNodes();
			if (!updatedNodes.IsEmpty())
			{
				// send event to the job to act upon completed tasks
				eventHandler.Handle(new JobUpdatedNodesEvent(GetJob().GetID(), updatedNodes));
				// act upon running tasks
				HashSet<NodeId> unusableNodes = new HashSet<NodeId>();
				foreach (NodeReport nr in updatedNodes)
				{
					NodeState nodeState = nr.GetNodeState();
					if (nodeState.IsUnusable())
					{
						unusableNodes.AddItem(nr.GetNodeId());
					}
				}
				for (int i = 0; i < 2; ++i)
				{
					Dictionary<TaskAttemptId, Container> taskSet = i == 0 ? assignedRequests.maps : assignedRequests
						.reduces;
					// kill running containers
					foreach (KeyValuePair<TaskAttemptId, Container> entry in taskSet)
					{
						TaskAttemptId tid = entry.Key;
						NodeId taskAttemptNodeId = entry.Value.GetNodeId();
						if (unusableNodes.Contains(taskAttemptNodeId))
						{
							Log.Info("Killing taskAttempt:" + tid + " because it is running on unusable node:"
								 + taskAttemptNodeId);
							eventHandler.Handle(new TaskAttemptKillEvent(tid, "TaskAttempt killed because it ran on unusable node"
								 + taskAttemptNodeId));
						}
					}
				}
			}
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceLimit()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headRoom = GetAvailableResources();
			if (headRoom == null)
			{
				headRoom = Resources.None();
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assignedMapResource = Resources.Multiply
				(mapResourceRequest, assignedRequests.maps.Count);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assignedReduceResource = Resources.Multiply
				(reduceResourceRequest, assignedRequests.reduces.Count);
			return Resources.Add(headRoom, Resources.Add(assignedMapResource, assignedReduceResource
				));
		}

		internal class ScheduledRequests
		{
			private readonly List<TaskAttemptId> earlierFailedMaps = new List<TaskAttemptId>(
				);

			/// <summary>Maps from a host to a list of Map tasks with data on the host</summary>
			private readonly IDictionary<string, List<TaskAttemptId>> mapsHostMapping = new Dictionary
				<string, List<TaskAttemptId>>();

			private readonly IDictionary<string, List<TaskAttemptId>> mapsRackMapping = new Dictionary
				<string, List<TaskAttemptId>>();

			[VisibleForTesting]
			internal readonly IDictionary<TaskAttemptId, RMContainerRequestor.ContainerRequest
				> maps = new LinkedHashMap<TaskAttemptId, RMContainerRequestor.ContainerRequest>
				();

			private readonly LinkedHashMap<TaskAttemptId, RMContainerRequestor.ContainerRequest
				> reduces = new LinkedHashMap<TaskAttemptId, RMContainerRequestor.ContainerRequest
				>();

			internal virtual bool Remove(TaskAttemptId tId)
			{
				RMContainerRequestor.ContainerRequest req = null;
				if (tId.GetTaskId().GetTaskType().Equals(TaskType.Map))
				{
					req = Sharpen.Collections.Remove(this.maps, tId);
				}
				else
				{
					req = Sharpen.Collections.Remove(this.reduces, tId);
				}
				if (req == null)
				{
					return false;
				}
				else
				{
					this._enclosing.DecContainerReq(req);
					return true;
				}
			}

			internal virtual RMContainerRequestor.ContainerRequest RemoveReduce()
			{
				IEnumerator<KeyValuePair<TaskAttemptId, RMContainerRequestor.ContainerRequest>> it
					 = this.reduces.GetEnumerator();
				if (it.HasNext())
				{
					KeyValuePair<TaskAttemptId, RMContainerRequestor.ContainerRequest> entry = it.Next
						();
					it.Remove();
					this._enclosing.DecContainerReq(entry.Value);
					return entry.Value;
				}
				return null;
			}

			internal virtual void AddMap(ContainerRequestEvent @event)
			{
				RMContainerRequestor.ContainerRequest request = null;
				if (@event.GetEarlierAttemptFailed())
				{
					this.earlierFailedMaps.AddItem(@event.GetAttemptID());
					request = new RMContainerRequestor.ContainerRequest(@event, RMContainerAllocator.
						PriorityFastFailMap);
					RMContainerAllocator.Log.Info("Added " + @event.GetAttemptID() + " to list of failed maps"
						);
				}
				else
				{
					foreach (string host in @event.GetHosts())
					{
						List<TaskAttemptId> list = this.mapsHostMapping[host];
						if (list == null)
						{
							list = new List<TaskAttemptId>();
							this.mapsHostMapping[host] = list;
						}
						list.AddItem(@event.GetAttemptID());
						if (RMContainerAllocator.Log.IsDebugEnabled())
						{
							RMContainerAllocator.Log.Debug("Added attempt req to host " + host);
						}
					}
					foreach (string rack in @event.GetRacks())
					{
						List<TaskAttemptId> list = this.mapsRackMapping[rack];
						if (list == null)
						{
							list = new List<TaskAttemptId>();
							this.mapsRackMapping[rack] = list;
						}
						list.AddItem(@event.GetAttemptID());
						if (RMContainerAllocator.Log.IsDebugEnabled())
						{
							RMContainerAllocator.Log.Debug("Added attempt req to rack " + rack);
						}
					}
					request = new RMContainerRequestor.ContainerRequest(@event, RMContainerAllocator.
						PriorityMap);
				}
				this.maps[@event.GetAttemptID()] = request;
				this._enclosing.AddContainerReq(request);
			}

			internal virtual void AddReduce(RMContainerRequestor.ContainerRequest req)
			{
				this.reduces[req.attemptID] = req;
				this._enclosing.AddContainerReq(req);
			}

			// this method will change the list of allocatedContainers.
			private void Assign(IList<Container> allocatedContainers)
			{
				IEnumerator<Container> it = allocatedContainers.GetEnumerator();
				RMContainerAllocator.Log.Info("Got allocated containers " + allocatedContainers.Count
					);
				this._enclosing.containersAllocated += allocatedContainers.Count;
				while (it.HasNext())
				{
					Container allocated = it.Next();
					if (RMContainerAllocator.Log.IsDebugEnabled())
					{
						RMContainerAllocator.Log.Debug("Assigning container " + allocated.GetId() + " with priority "
							 + allocated.GetPriority() + " to NM " + allocated.GetNodeId());
					}
					// check if allocated container meets memory requirements 
					// and whether we have any scheduled tasks that need 
					// a container to be assigned
					bool isAssignable = true;
					Priority priority = allocated.GetPriority();
					Org.Apache.Hadoop.Yarn.Api.Records.Resource allocatedResource = allocated.GetResource
						();
					if (RMContainerAllocator.PriorityFastFailMap.Equals(priority) || RMContainerAllocator
						.PriorityMap.Equals(priority))
					{
						if (ResourceCalculatorUtils.ComputeAvailableContainers(allocatedResource, this._enclosing
							.mapResourceRequest, this._enclosing.GetSchedulerResourceTypes()) <= 0 || this.maps
							.IsEmpty())
						{
							RMContainerAllocator.Log.Info("Cannot assign container " + allocated + " for a map as either "
								 + " container memory less than required " + this._enclosing.mapResourceRequest 
								+ " or no pending map tasks - maps.isEmpty=" + this.maps.IsEmpty());
							isAssignable = false;
						}
					}
					else
					{
						if (RMContainerAllocator.PriorityReduce.Equals(priority))
						{
							if (ResourceCalculatorUtils.ComputeAvailableContainers(allocatedResource, this._enclosing
								.reduceResourceRequest, this._enclosing.GetSchedulerResourceTypes()) <= 0 || this
								.reduces.IsEmpty())
							{
								RMContainerAllocator.Log.Info("Cannot assign container " + allocated + " for a reduce as either "
									 + " container memory less than required " + this._enclosing.reduceResourceRequest
									 + " or no pending reduce tasks - reduces.isEmpty=" + this.reduces.IsEmpty());
								isAssignable = false;
							}
						}
						else
						{
							RMContainerAllocator.Log.Warn("Container allocated at unwanted priority: " + priority
								 + ". Returning to RM...");
							isAssignable = false;
						}
					}
					if (!isAssignable)
					{
						// release container if we could not assign it 
						this.ContainerNotAssigned(allocated);
						it.Remove();
						continue;
					}
					// do not assign if allocated container is on a  
					// blacklisted host
					string allocatedHost = allocated.GetNodeId().GetHost();
					if (this._enclosing.IsNodeBlacklisted(allocatedHost))
					{
						// we need to request for a new container 
						// and release the current one
						RMContainerAllocator.Log.Info("Got allocated container on a blacklisted " + " host "
							 + allocatedHost + ". Releasing container " + allocated);
						// find the request matching this allocated container 
						// and replace it with a new one 
						RMContainerRequestor.ContainerRequest toBeReplacedReq = this.GetContainerReqToReplace
							(allocated);
						if (toBeReplacedReq != null)
						{
							RMContainerAllocator.Log.Info("Placing a new container request for task attempt "
								 + toBeReplacedReq.attemptID);
							RMContainerRequestor.ContainerRequest newReq = this._enclosing.GetFilteredContainerRequest
								(toBeReplacedReq);
							this._enclosing.DecContainerReq(toBeReplacedReq);
							if (toBeReplacedReq.attemptID.GetTaskId().GetTaskType() == TaskType.Map)
							{
								this.maps[newReq.attemptID] = newReq;
							}
							else
							{
								this.reduces[newReq.attemptID] = newReq;
							}
							this._enclosing.AddContainerReq(newReq);
						}
						else
						{
							RMContainerAllocator.Log.Info("Could not map allocated container to a valid request."
								 + " Releasing allocated container " + allocated);
						}
						// release container if we could not assign it 
						this.ContainerNotAssigned(allocated);
						it.Remove();
						continue;
					}
				}
				this.AssignContainers(allocatedContainers);
				// release container if we could not assign it 
				it = allocatedContainers.GetEnumerator();
				while (it.HasNext())
				{
					Container allocated = it.Next();
					RMContainerAllocator.Log.Info("Releasing unassigned container " + allocated);
					this.ContainerNotAssigned(allocated);
				}
			}

			private void ContainerAssigned(Container allocated, RMContainerRequestor.ContainerRequest
				 assigned)
			{
				// Update resource requests
				this._enclosing.DecContainerReq(assigned);
				// send the container-assigned event to task attempt
				this._enclosing.eventHandler.Handle(new TaskAttemptContainerAssignedEvent(assigned
					.attemptID, allocated, this._enclosing.applicationACLs));
				this._enclosing.assignedRequests.Add(allocated, assigned.attemptID);
				if (RMContainerAllocator.Log.IsDebugEnabled())
				{
					RMContainerAllocator.Log.Info("Assigned container (" + allocated + ") " + " to task "
						 + assigned.attemptID + " on node " + allocated.GetNodeId().ToString());
				}
			}

			private void ContainerNotAssigned(Container allocated)
			{
				this._enclosing.containersReleased++;
				this._enclosing.pendingRelease.AddItem(allocated.GetId());
				this._enclosing.Release(allocated.GetId());
			}

			private RMContainerRequestor.ContainerRequest AssignWithoutLocality(Container allocated
				)
			{
				RMContainerRequestor.ContainerRequest assigned = null;
				Priority priority = allocated.GetPriority();
				if (RMContainerAllocator.PriorityFastFailMap.Equals(priority))
				{
					RMContainerAllocator.Log.Info("Assigning container " + allocated + " to fast fail map"
						);
					assigned = this.AssignToFailedMap(allocated);
				}
				else
				{
					if (RMContainerAllocator.PriorityReduce.Equals(priority))
					{
						if (RMContainerAllocator.Log.IsDebugEnabled())
						{
							RMContainerAllocator.Log.Debug("Assigning container " + allocated + " to reduce");
						}
						assigned = this.AssignToReduce(allocated);
					}
				}
				return assigned;
			}

			private void AssignContainers(IList<Container> allocatedContainers)
			{
				IEnumerator<Container> it = allocatedContainers.GetEnumerator();
				while (it.HasNext())
				{
					Container allocated = it.Next();
					RMContainerRequestor.ContainerRequest assigned = this.AssignWithoutLocality(allocated
						);
					if (assigned != null)
					{
						this.ContainerAssigned(allocated, assigned);
						it.Remove();
					}
				}
				this.AssignMapsWithLocality(allocatedContainers);
			}

			private RMContainerRequestor.ContainerRequest GetContainerReqToReplace(Container 
				allocated)
			{
				RMContainerAllocator.Log.Info("Finding containerReq for allocated container: " + 
					allocated);
				Priority priority = allocated.GetPriority();
				RMContainerRequestor.ContainerRequest toBeReplaced = null;
				if (RMContainerAllocator.PriorityFastFailMap.Equals(priority))
				{
					RMContainerAllocator.Log.Info("Replacing FAST_FAIL_MAP container " + allocated.GetId
						());
					IEnumerator<TaskAttemptId> iter = this.earlierFailedMaps.GetEnumerator();
					while (toBeReplaced == null && iter.HasNext())
					{
						toBeReplaced = this.maps[iter.Next()];
					}
					RMContainerAllocator.Log.Info("Found replacement: " + toBeReplaced);
					return toBeReplaced;
				}
				else
				{
					if (RMContainerAllocator.PriorityMap.Equals(priority))
					{
						RMContainerAllocator.Log.Info("Replacing MAP container " + allocated.GetId());
						// allocated container was for a map
						string host = allocated.GetNodeId().GetHost();
						List<TaskAttemptId> list = this.mapsHostMapping[host];
						if (list != null && list.Count > 0)
						{
							TaskAttemptId tId = list.RemoveLast();
							if (this.maps.Contains(tId))
							{
								toBeReplaced = Sharpen.Collections.Remove(this.maps, tId);
							}
						}
						else
						{
							TaskAttemptId tId = this.maps.Keys.GetEnumerator().Next();
							toBeReplaced = Sharpen.Collections.Remove(this.maps, tId);
						}
					}
					else
					{
						if (RMContainerAllocator.PriorityReduce.Equals(priority))
						{
							TaskAttemptId tId = this.reduces.Keys.GetEnumerator().Next();
							toBeReplaced = Sharpen.Collections.Remove(this.reduces, tId);
						}
					}
				}
				RMContainerAllocator.Log.Info("Found replacement: " + toBeReplaced);
				return toBeReplaced;
			}

			private RMContainerRequestor.ContainerRequest AssignToFailedMap(Container allocated
				)
			{
				//try to assign to earlierFailedMaps if present
				RMContainerRequestor.ContainerRequest assigned = null;
				while (assigned == null && this.earlierFailedMaps.Count > 0 && this._enclosing.CanAssignMaps
					())
				{
					TaskAttemptId tId = this.earlierFailedMaps.RemoveFirst();
					if (this.maps.Contains(tId))
					{
						assigned = Sharpen.Collections.Remove(this.maps, tId);
						JobCounterUpdateEvent jce = new JobCounterUpdateEvent(assigned.attemptID.GetTaskId
							().GetJobId());
						jce.AddCounterUpdate(JobCounter.OtherLocalMaps, 1);
						this._enclosing.eventHandler.Handle(jce);
						RMContainerAllocator.Log.Info("Assigned from earlierFailedMaps");
						break;
					}
				}
				return assigned;
			}

			private RMContainerRequestor.ContainerRequest AssignToReduce(Container allocated)
			{
				RMContainerRequestor.ContainerRequest assigned = null;
				//try to assign to reduces if present
				if (assigned == null && this.reduces.Count > 0 && this._enclosing.CanAssignReduces
					())
				{
					TaskAttemptId tId = this.reduces.Keys.GetEnumerator().Next();
					assigned = Sharpen.Collections.Remove(this.reduces, tId);
					RMContainerAllocator.Log.Info("Assigned to reduce");
				}
				return assigned;
			}

			private void AssignMapsWithLocality(IList<Container> allocatedContainers)
			{
				// try to assign to all nodes first to match node local
				IEnumerator<Container> it = allocatedContainers.GetEnumerator();
				while (it.HasNext() && this.maps.Count > 0 && this._enclosing.CanAssignMaps())
				{
					Container allocated = it.Next();
					Priority priority = allocated.GetPriority();
					System.Diagnostics.Debug.Assert(RMContainerAllocator.PriorityMap.Equals(priority)
						);
					// "if (maps.containsKey(tId))" below should be almost always true.
					// hence this while loop would almost always have O(1) complexity
					string host = allocated.GetNodeId().GetHost();
					List<TaskAttemptId> list = this.mapsHostMapping[host];
					while (list != null && list.Count > 0)
					{
						if (RMContainerAllocator.Log.IsDebugEnabled())
						{
							RMContainerAllocator.Log.Debug("Host matched to the request list " + host);
						}
						TaskAttemptId tId = list.RemoveFirst();
						if (this.maps.Contains(tId))
						{
							RMContainerRequestor.ContainerRequest assigned = Sharpen.Collections.Remove(this.
								maps, tId);
							this.ContainerAssigned(allocated, assigned);
							it.Remove();
							JobCounterUpdateEvent jce = new JobCounterUpdateEvent(assigned.attemptID.GetTaskId
								().GetJobId());
							jce.AddCounterUpdate(JobCounter.DataLocalMaps, 1);
							this._enclosing.eventHandler.Handle(jce);
							this._enclosing.hostLocalAssigned++;
							if (RMContainerAllocator.Log.IsDebugEnabled())
							{
								RMContainerAllocator.Log.Debug("Assigned based on host match " + host);
							}
							break;
						}
					}
				}
				// try to match all rack local
				it = allocatedContainers.GetEnumerator();
				while (it.HasNext() && this.maps.Count > 0 && this._enclosing.CanAssignMaps())
				{
					Container allocated = it.Next();
					Priority priority = allocated.GetPriority();
					System.Diagnostics.Debug.Assert(RMContainerAllocator.PriorityMap.Equals(priority)
						);
					// "if (maps.containsKey(tId))" below should be almost always true.
					// hence this while loop would almost always have O(1) complexity
					string host = allocated.GetNodeId().GetHost();
					string rack = RackResolver.Resolve(host).GetNetworkLocation();
					List<TaskAttemptId> list = this.mapsRackMapping[rack];
					while (list != null && list.Count > 0)
					{
						TaskAttemptId tId = list.RemoveFirst();
						if (this.maps.Contains(tId))
						{
							RMContainerRequestor.ContainerRequest assigned = Sharpen.Collections.Remove(this.
								maps, tId);
							this.ContainerAssigned(allocated, assigned);
							it.Remove();
							JobCounterUpdateEvent jce = new JobCounterUpdateEvent(assigned.attemptID.GetTaskId
								().GetJobId());
							jce.AddCounterUpdate(JobCounter.RackLocalMaps, 1);
							this._enclosing.eventHandler.Handle(jce);
							this._enclosing.rackLocalAssigned++;
							if (RMContainerAllocator.Log.IsDebugEnabled())
							{
								RMContainerAllocator.Log.Debug("Assigned based on rack match " + rack);
							}
							break;
						}
					}
				}
				// assign remaining
				it = allocatedContainers.GetEnumerator();
				while (it.HasNext() && this.maps.Count > 0 && this._enclosing.CanAssignMaps())
				{
					Container allocated = it.Next();
					Priority priority = allocated.GetPriority();
					System.Diagnostics.Debug.Assert(RMContainerAllocator.PriorityMap.Equals(priority)
						);
					TaskAttemptId tId = this.maps.Keys.GetEnumerator().Next();
					RMContainerRequestor.ContainerRequest assigned = Sharpen.Collections.Remove(this.
						maps, tId);
					this.ContainerAssigned(allocated, assigned);
					it.Remove();
					JobCounterUpdateEvent jce = new JobCounterUpdateEvent(assigned.attemptID.GetTaskId
						().GetJobId());
					jce.AddCounterUpdate(JobCounter.OtherLocalMaps, 1);
					this._enclosing.eventHandler.Handle(jce);
					if (RMContainerAllocator.Log.IsDebugEnabled())
					{
						RMContainerAllocator.Log.Debug("Assigned based on * match");
					}
				}
			}

			internal ScheduledRequests(RMContainerAllocator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMContainerAllocator _enclosing;
		}

		internal class AssignedRequests
		{
			private readonly IDictionary<ContainerId, TaskAttemptId> containerToAttemptMap = 
				new Dictionary<ContainerId, TaskAttemptId>();

			private readonly LinkedHashMap<TaskAttemptId, Container> maps = new LinkedHashMap
				<TaskAttemptId, Container>();

			[VisibleForTesting]
			internal readonly LinkedHashMap<TaskAttemptId, Container> reduces = new LinkedHashMap
				<TaskAttemptId, Container>();

			[VisibleForTesting]
			internal readonly ICollection<TaskAttemptId> preemptionWaitingReduces = new HashSet
				<TaskAttemptId>();

			internal virtual void Add(Container container, TaskAttemptId tId)
			{
				RMContainerAllocator.Log.Info("Assigned container " + container.GetId().ToString(
					) + " to " + tId);
				this.containerToAttemptMap[container.GetId()] = tId;
				if (tId.GetTaskId().GetTaskType().Equals(TaskType.Map))
				{
					this.maps[tId] = container;
				}
				else
				{
					this.reduces[tId] = container;
				}
			}

			internal virtual void PreemptReduce(int toPreempt)
			{
				IList<TaskAttemptId> reduceList = new AList<TaskAttemptId>(this.reduces.Keys);
				//sort reduces on progress
				reduceList.Sort(new _IComparer_1319(this));
				for (int i = 0; i < toPreempt && reduceList.Count > 0; i++)
				{
					TaskAttemptId id = reduceList.Remove(0);
					//remove the one on top
					RMContainerAllocator.Log.Info("Preempting " + id);
					this.preemptionWaitingReduces.AddItem(id);
					this._enclosing.eventHandler.Handle(new TaskAttemptKillEvent(id, RMContainerAllocator
						.RampdownDiagnostic));
				}
			}

			private sealed class _IComparer_1319 : IComparer<TaskAttemptId>
			{
				public _IComparer_1319(AssignedRequests _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public int Compare(TaskAttemptId o1, TaskAttemptId o2)
				{
					return float.Compare(this._enclosing._enclosing.GetJob().GetTask(o1.GetTaskId()).
						GetAttempt(o1).GetProgress(), this._enclosing._enclosing.GetJob().GetTask(o2.GetTaskId
						()).GetAttempt(o2).GetProgress());
				}

				private readonly AssignedRequests _enclosing;
			}

			internal virtual bool Remove(TaskAttemptId tId)
			{
				ContainerId containerId = null;
				if (tId.GetTaskId().GetTaskType().Equals(TaskType.Map))
				{
					containerId = Sharpen.Collections.Remove(this.maps, tId).GetId();
				}
				else
				{
					containerId = Sharpen.Collections.Remove(this.reduces, tId).GetId();
					if (containerId != null)
					{
						bool preempted = this.preemptionWaitingReduces.Remove(tId);
						if (preempted)
						{
							RMContainerAllocator.Log.Info("Reduce preemption successful " + tId);
						}
					}
				}
				if (containerId != null)
				{
					Sharpen.Collections.Remove(this.containerToAttemptMap, containerId);
					return true;
				}
				return false;
			}

			internal virtual TaskAttemptId Get(ContainerId cId)
			{
				return this.containerToAttemptMap[cId];
			}

			internal virtual ContainerId Get(TaskAttemptId tId)
			{
				Container taskContainer;
				if (tId.GetTaskId().GetTaskType().Equals(TaskType.Map))
				{
					taskContainer = this.maps[tId];
				}
				else
				{
					taskContainer = this.reduces[tId];
				}
				if (taskContainer == null)
				{
					return null;
				}
				else
				{
					return taskContainer.GetId();
				}
			}

			internal AssignedRequests(RMContainerAllocator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMContainerAllocator _enclosing;
		}

		private class ScheduleStats
		{
			internal int numPendingReduces;

			internal int numScheduledMaps;

			internal int numScheduledReduces;

			internal int numAssignedMaps;

			internal int numAssignedReduces;

			internal int numCompletedMaps;

			internal int numCompletedReduces;

			internal int numContainersAllocated;

			internal int numContainersReleased;

			public virtual void UpdateAndLogIfChanged(string msgPrefix)
			{
				bool changed = false;
				// synchronized to fix findbug warnings
				lock (this._enclosing)
				{
					changed |= (this.numPendingReduces != this._enclosing.pendingReduces.Count);
					this.numPendingReduces = this._enclosing.pendingReduces.Count;
					changed |= (this.numScheduledMaps != this._enclosing.scheduledRequests.maps.Count
						);
					this.numScheduledMaps = this._enclosing.scheduledRequests.maps.Count;
					changed |= (this.numScheduledReduces != this._enclosing.scheduledRequests.reduces
						.Count);
					this.numScheduledReduces = this._enclosing.scheduledRequests.reduces.Count;
					changed |= (this.numAssignedMaps != this._enclosing.assignedRequests.maps.Count);
					this.numAssignedMaps = this._enclosing.assignedRequests.maps.Count;
					changed |= (this.numAssignedReduces != this._enclosing.assignedRequests.reduces.Count
						);
					this.numAssignedReduces = this._enclosing.assignedRequests.reduces.Count;
					changed |= (this.numCompletedMaps != this._enclosing.GetJob().GetCompletedMaps());
					this.numCompletedMaps = this._enclosing.GetJob().GetCompletedMaps();
					changed |= (this.numCompletedReduces != this._enclosing.GetJob().GetCompletedReduces
						());
					this.numCompletedReduces = this._enclosing.GetJob().GetCompletedReduces();
					changed |= (this.numContainersAllocated != this._enclosing.containersAllocated);
					this.numContainersAllocated = this._enclosing.containersAllocated;
					changed |= (this.numContainersReleased != this._enclosing.containersReleased);
					this.numContainersReleased = this._enclosing.containersReleased;
				}
				if (changed)
				{
					this.Log(msgPrefix);
				}
			}

			public virtual void Log(string msgPrefix)
			{
				RMContainerAllocator.Log.Info(msgPrefix + "PendingReds:" + this.numPendingReduces
					 + " ScheduledMaps:" + this.numScheduledMaps + " ScheduledReds:" + this.numScheduledReduces
					 + " AssignedMaps:" + this.numAssignedMaps + " AssignedReds:" + this.numAssignedReduces
					 + " CompletedMaps:" + this.numCompletedMaps + " CompletedReds:" + this.numCompletedReduces
					 + " ContAlloc:" + this.numContainersAllocated + " ContRel:" + this.numContainersReleased
					 + " HostLocal:" + this._enclosing.hostLocalAssigned + " RackLocal:" + this._enclosing
					.rackLocalAssigned);
			}

			internal ScheduleStats(RMContainerAllocator _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMContainerAllocator _enclosing;
		}
	}
}
