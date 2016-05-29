using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.Capacity
{
	/// <summary>
	/// This class implement a
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.SchedulingEditPolicy
	/// 	"/>
	/// that is designed to be
	/// paired with the
	/// <c>CapacityScheduler</c>
	/// . At every invocation of
	/// <c>editSchedule()</c>
	/// it computes the ideal amount of resources assigned to each
	/// queue (for each queue in the hierarchy), and determines whether preemption
	/// is needed. Overcapacity is distributed among queues in a weighted fair manner,
	/// where the weight is the amount of guaranteed capacity for the queue.
	/// Based on this ideal assignment it determines whether preemption is required
	/// and select a set of containers from each application that would be killed if
	/// the corresponding amount of resources is not freed up by the application.
	/// If not in
	/// <c>observeOnly</c>
	/// mode, it triggers preemption requests via a
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ContainerPreemptEvent
	/// 	"/>
	/// that the
	/// <c>ResourceManager</c>
	/// will ensure
	/// to deliver to the application (or to execute).
	/// If the deficit of resources is persistent over a long enough period of time
	/// this policy will trigger forced termination of containers (again by generating
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ContainerPreemptEvent
	/// 	"/>
	/// ).
	/// </summary>
	public class ProportionalCapacityPreemptionPolicy : SchedulingEditPolicy
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.Capacity.ProportionalCapacityPreemptionPolicy
			));

		/// <summary>
		/// If true, run the policy but do not affect the cluster with preemption and
		/// kill events.
		/// </summary>
		public const string ObserveOnly = "yarn.resourcemanager.monitor.capacity.preemption.observe_only";

		/// <summary>Time in milliseconds between invocations of this policy</summary>
		public const string MonitoringInterval = "yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval";

		/// <summary>
		/// Time in milliseconds between requesting a preemption from an application
		/// and killing the container.
		/// </summary>
		public const string WaitTimeBeforeKill = "yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill";

		/// <summary>Maximum percentage of resources preempted in a single round.</summary>
		/// <remarks>
		/// Maximum percentage of resources preempted in a single round. By
		/// controlling this value one can throttle the pace at which containers are
		/// reclaimed from the cluster. After computing the total desired preemption,
		/// the policy scales it back within this limit.
		/// </remarks>
		public const string TotalPreemptionPerRound = "yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round";

		/// <summary>
		/// Maximum amount of resources above the target capacity ignored for
		/// preemption.
		/// </summary>
		/// <remarks>
		/// Maximum amount of resources above the target capacity ignored for
		/// preemption. This defines a deadzone around the target capacity that helps
		/// prevent thrashing and oscillations around the computed target balance.
		/// High values would slow the time to capacity and (absent natural
		/// completions) it might prevent convergence to guaranteed capacity.
		/// </remarks>
		public const string MaxIgnoredOverCapacity = "yarn.resourcemanager.monitor.capacity.preemption.max_ignored_over_capacity";

		/// <summary>
		/// Given a computed preemption target, account for containers naturally
		/// expiring and preempt only this percentage of the delta.
		/// </summary>
		/// <remarks>
		/// Given a computed preemption target, account for containers naturally
		/// expiring and preempt only this percentage of the delta. This determines
		/// the rate of geometric convergence into the deadzone (
		/// <see cref="MaxIgnoredOverCapacity"/>
		/// ). For example, a termination factor of 0.5
		/// will reclaim almost 95% of resources within 5 *
		/// <see cref="WaitTimeBeforeKill"/>
		/// , even absent natural termination.
		/// </remarks>
		public const string NaturalTerminationFactor = "yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor";

		private RMContext rmContext;

		private readonly Clock clock;

		private double maxIgnoredOverCapacity;

		private long maxWaitTime;

		private CapacityScheduler scheduler;

		private long monitoringInterval;

		private readonly IDictionary<RMContainer, long> preempted = new Dictionary<RMContainer
			, long>();

		private ResourceCalculator rc;

		private float percentageClusterPreemptionAllowed;

		private double naturalTerminationFactor;

		private bool observeOnly;

		private IDictionary<NodeId, ICollection<string>> labels;

		public ProportionalCapacityPreemptionPolicy()
		{
			clock = new SystemClock();
		}

		public ProportionalCapacityPreemptionPolicy(Configuration config, RMContext context
			, CapacityScheduler scheduler)
			: this(config, context, scheduler, new SystemClock())
		{
		}

		public ProportionalCapacityPreemptionPolicy(Configuration config, RMContext context
			, CapacityScheduler scheduler, Clock clock)
		{
			Init(config, context, scheduler);
			this.clock = clock;
		}

		public virtual void Init(Configuration config, RMContext context, PreemptableResourceScheduler
			 sched)
		{
			Log.Info("Preemption monitor:" + this.GetType().GetCanonicalName());
			System.Diagnostics.Debug.Assert(null == scheduler, "Unexpected duplicate call to init"
				);
			if (!(sched is CapacityScheduler))
			{
				throw new YarnRuntimeException("Class " + sched.GetType().GetCanonicalName() + " not instance of "
					 + typeof(CapacityScheduler).GetCanonicalName());
			}
			rmContext = context;
			scheduler = (CapacityScheduler)sched;
			maxIgnoredOverCapacity = config.GetDouble(MaxIgnoredOverCapacity, 0.1);
			naturalTerminationFactor = config.GetDouble(NaturalTerminationFactor, 0.2);
			maxWaitTime = config.GetLong(WaitTimeBeforeKill, 15000);
			monitoringInterval = config.GetLong(MonitoringInterval, 3000);
			percentageClusterPreemptionAllowed = config.GetFloat(TotalPreemptionPerRound, (float
				)0.1);
			observeOnly = config.GetBoolean(ObserveOnly, false);
			rc = scheduler.GetResourceCalculator();
			labels = null;
		}

		[VisibleForTesting]
		public virtual ResourceCalculator GetResourceCalculator()
		{
			return rc;
		}

		public virtual void EditSchedule()
		{
			CSQueue root = scheduler.GetRootQueue();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources = Resources.Clone(scheduler
				.GetClusterResource());
			clusterResources = GetNonLabeledResources(clusterResources);
			SetNodeLabels(scheduler.GetRMContext().GetNodeLabelManager().GetNodeLabels());
			ContainerBasedPreemptOrKill(root, clusterResources);
		}

		/// <summary>Setting Node Labels</summary>
		/// <param name="nodelabels"/>
		public virtual void SetNodeLabels(IDictionary<NodeId, ICollection<string>> nodelabels
			)
		{
			labels = nodelabels;
		}

		/// <summary>This method returns all non labeled resources.</summary>
		/// <param name="clusterResources"/>
		/// <returns>Resources</returns>
		private Org.Apache.Hadoop.Yarn.Api.Records.Resource GetNonLabeledResources(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources)
		{
			RMContext rmcontext = scheduler.GetRMContext();
			RMNodeLabelsManager lm = rmcontext.GetNodeLabelManager();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource res = lm.GetResourceByLabel(RMNodeLabelsManager
				.NoLabel, clusterResources);
			return res == null ? clusterResources : res;
		}

		/// <summary>This method selects and tracks containers to be preempted.</summary>
		/// <remarks>
		/// This method selects and tracks containers to be preempted. If a container
		/// is in the target list for more than maxWaitTime it is killed.
		/// </remarks>
		/// <param name="root">the root of the CapacityScheduler queue hierarchy</param>
		/// <param name="clusterResources">the total amount of resources in the cluster</param>
		private void ContainerBasedPreemptOrKill(CSQueue root, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources)
		{
			// extract a summary of the queues from scheduler
			ProportionalCapacityPreemptionPolicy.TempQueue tRoot;
			lock (scheduler)
			{
				tRoot = CloneQueues(root, clusterResources);
			}
			// compute the ideal distribution of resources among queues
			// updates cloned queues state accordingly
			tRoot.idealAssigned = tRoot.guaranteed;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalPreemptionAllowed = Resources.Multiply
				(clusterResources, percentageClusterPreemptionAllowed);
			IList<ProportionalCapacityPreemptionPolicy.TempQueue> queues = RecursivelyComputeIdealAssignment
				(tRoot, totalPreemptionAllowed);
			// based on ideal allocation select containers to be preempted from each
			// queue and each application
			IDictionary<ApplicationAttemptId, ICollection<RMContainer>> toPreempt = GetContainersToPreempt
				(queues, clusterResources);
			if (Log.IsDebugEnabled())
			{
				LogToCSV(queues);
			}
			// if we are in observeOnly mode return before any action is taken
			if (observeOnly)
			{
				return;
			}
			// preempt (or kill) the selected containers
			foreach (KeyValuePair<ApplicationAttemptId, ICollection<RMContainer>> e in toPreempt)
			{
				ApplicationAttemptId appAttemptId = e.Key;
				foreach (RMContainer container in e.Value)
				{
					// if we tried to preempt this for more than maxWaitTime
					if (preempted[container] != null && preempted[container] + maxWaitTime < clock.GetTime
						())
					{
						// kill it
						rmContext.GetDispatcher().GetEventHandler().Handle(new ContainerPreemptEvent(appAttemptId
							, container, SchedulerEventType.KillContainer));
						Sharpen.Collections.Remove(preempted, container);
					}
					else
					{
						//otherwise just send preemption events
						rmContext.GetDispatcher().GetEventHandler().Handle(new ContainerPreemptEvent(appAttemptId
							, container, SchedulerEventType.PreemptContainer));
						if (preempted[container] == null)
						{
							preempted[container] = clock.GetTime();
						}
					}
				}
			}
			// Keep the preempted list clean
			for (IEnumerator<RMContainer> i = preempted.Keys.GetEnumerator(); i.HasNext(); )
			{
				RMContainer id = i.Next();
				// garbage collect containers that are irrelevant for preemption
				if (preempted[id] + 2 * maxWaitTime < clock.GetTime())
				{
					i.Remove();
				}
			}
		}

		/// <summary>
		/// This method recursively computes the ideal assignment of resources to each
		/// level of the hierarchy.
		/// </summary>
		/// <remarks>
		/// This method recursively computes the ideal assignment of resources to each
		/// level of the hierarchy. This ensures that leafs that are over-capacity but
		/// with parents within capacity will not be preempted. Preemptions are allowed
		/// within each subtree according to local over/under capacity.
		/// </remarks>
		/// <param name="root">the root of the cloned queue hierachy</param>
		/// <param name="totalPreemptionAllowed">maximum amount of preemption allowed</param>
		/// <returns>a list of leaf queues updated with preemption targets</returns>
		private IList<ProportionalCapacityPreemptionPolicy.TempQueue> RecursivelyComputeIdealAssignment
			(ProportionalCapacityPreemptionPolicy.TempQueue root, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalPreemptionAllowed)
		{
			IList<ProportionalCapacityPreemptionPolicy.TempQueue> leafs = new AList<ProportionalCapacityPreemptionPolicy.TempQueue
				>();
			if (root.GetChildren() != null && root.GetChildren().Count > 0)
			{
				// compute ideal distribution at this level
				ComputeIdealResourceDistribution(rc, root.GetChildren(), totalPreemptionAllowed, 
					root.idealAssigned);
				// compute recursively for lower levels and build list of leafs
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue t in root.GetChildren())
				{
					Sharpen.Collections.AddAll(leafs, RecursivelyComputeIdealAssignment(t, totalPreemptionAllowed
						));
				}
			}
			else
			{
				// we are in a leaf nothing to do, just return yourself
				return Sharpen.Collections.SingletonList(root);
			}
			return leafs;
		}

		/// <summary>
		/// This method computes (for a single level in the tree, passed as a
		/// <c>List&lt;TempQueue&gt;</c>
		/// ) the ideal assignment of resources. This is done
		/// recursively to allocate capacity fairly across all queues with pending
		/// demands. It terminates when no resources are left to assign, or when all
		/// demand is satisfied.
		/// </summary>
		/// <param name="rc">resource calculator</param>
		/// <param name="queues">
		/// a list of cloned queues to be assigned capacity to (this is
		/// an out param)
		/// </param>
		/// <param name="totalPreemptionAllowed">total amount of preemption we allow</param>
		/// <param name="tot_guarant">the amount of capacity assigned to this pool of queues</param>
		private void ComputeIdealResourceDistribution(ResourceCalculator rc, IList<ProportionalCapacityPreemptionPolicy.TempQueue
			> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource totalPreemptionAllowed, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 tot_guarant)
		{
			// qAlloc tracks currently active queues (will decrease progressively as
			// demand is met)
			IList<ProportionalCapacityPreemptionPolicy.TempQueue> qAlloc = new AList<ProportionalCapacityPreemptionPolicy.TempQueue
				>(queues);
			// unassigned tracks how much resources are still to assign, initialized
			// with the total capacity for this set of queues
			Org.Apache.Hadoop.Yarn.Api.Records.Resource unassigned = Resources.Clone(tot_guarant
				);
			// group queues based on whether they have non-zero guaranteed capacity
			ICollection<ProportionalCapacityPreemptionPolicy.TempQueue> nonZeroGuarQueues = new 
				HashSet<ProportionalCapacityPreemptionPolicy.TempQueue>();
			ICollection<ProportionalCapacityPreemptionPolicy.TempQueue> zeroGuarQueues = new 
				HashSet<ProportionalCapacityPreemptionPolicy.TempQueue>();
			foreach (ProportionalCapacityPreemptionPolicy.TempQueue q in qAlloc)
			{
				if (Resources.GreaterThan(rc, tot_guarant, q.guaranteed, Resources.None()))
				{
					nonZeroGuarQueues.AddItem(q);
				}
				else
				{
					zeroGuarQueues.AddItem(q);
				}
			}
			// first compute the allocation as a fixpoint based on guaranteed capacity
			ComputeFixpointAllocation(rc, tot_guarant, nonZeroGuarQueues, unassigned, false);
			// if any capacity is left unassigned, distributed among zero-guarantee 
			// queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
			if (!zeroGuarQueues.IsEmpty() && Resources.GreaterThan(rc, tot_guarant, unassigned
				, Resources.None()))
			{
				ComputeFixpointAllocation(rc, tot_guarant, zeroGuarQueues, unassigned, true);
			}
			// based on ideal assignment computed above and current assignment we derive
			// how much preemption is required overall
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totPreemptionNeeded = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			foreach (ProportionalCapacityPreemptionPolicy.TempQueue t in queues)
			{
				if (Resources.GreaterThan(rc, tot_guarant, t.current, t.idealAssigned))
				{
					Resources.AddTo(totPreemptionNeeded, Resources.Subtract(t.current, t.idealAssigned
						));
				}
			}
			// if we need to preempt more than is allowed, compute a factor (0<f<1)
			// that is used to scale down how much we ask back from each queue
			float scalingFactor = 1.0F;
			if (Resources.GreaterThan(rc, tot_guarant, totPreemptionNeeded, totalPreemptionAllowed
				))
			{
				scalingFactor = Resources.Divide(rc, tot_guarant, totalPreemptionAllowed, totPreemptionNeeded
					);
			}
			// assign to each queue the amount of actual preemption based on local
			// information of ideal preemption and scaling factor
			foreach (ProportionalCapacityPreemptionPolicy.TempQueue t_1 in queues)
			{
				t_1.AssignPreemption(scalingFactor, rc, tot_guarant);
			}
			if (Log.IsDebugEnabled())
			{
				long time = clock.GetTime();
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue t_2 in queues)
				{
					Log.Debug(time + ": " + t_2);
				}
			}
		}

		/// <summary>
		/// Given a set of queues compute the fix-point distribution of unassigned
		/// resources among them.
		/// </summary>
		/// <remarks>
		/// Given a set of queues compute the fix-point distribution of unassigned
		/// resources among them. As pending request of a queue are exhausted, the
		/// queue is removed from the set and remaining capacity redistributed among
		/// remaining queues. The distribution is weighted based on guaranteed
		/// capacity, unless asked to ignoreGuarantee, in which case resources are
		/// distributed uniformly.
		/// </remarks>
		private void ComputeFixpointAllocation(ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 tot_guarant, ICollection<ProportionalCapacityPreemptionPolicy.TempQueue> qAlloc
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource unassigned, bool ignoreGuarantee)
		{
			// Prior to assigning the unused resources, process each queue as follows:
			// If current > guaranteed, idealAssigned = guaranteed + untouchable extra
			// Else idealAssigned = current;
			// Subtract idealAssigned resources from unassigned.
			// If the queue has all of its needs met (that is, if 
			// idealAssigned >= current + pending), remove it from consideration.
			// Sort queues from most under-guaranteed to most over-guaranteed.
			ProportionalCapacityPreemptionPolicy.TQComparator tqComparator = new ProportionalCapacityPreemptionPolicy.TQComparator
				(rc, tot_guarant);
			PriorityQueue<ProportionalCapacityPreemptionPolicy.TempQueue> orderedByNeed = new 
				PriorityQueue<ProportionalCapacityPreemptionPolicy.TempQueue>(10, tqComparator);
			for (IEnumerator<ProportionalCapacityPreemptionPolicy.TempQueue> i = qAlloc.GetEnumerator
				(); i.HasNext(); )
			{
				ProportionalCapacityPreemptionPolicy.TempQueue q = i.Next();
				if (Resources.GreaterThan(rc, tot_guarant, q.current, q.guaranteed))
				{
					q.idealAssigned = Resources.Add(q.guaranteed, q.untouchableExtra);
				}
				else
				{
					q.idealAssigned = Resources.Clone(q.current);
				}
				Resources.SubtractFrom(unassigned, q.idealAssigned);
				// If idealAssigned < (current + pending), q needs more resources, so
				// add it to the list of underserved queues, ordered by need.
				Org.Apache.Hadoop.Yarn.Api.Records.Resource curPlusPend = Resources.Add(q.current
					, q.pending);
				if (Resources.LessThan(rc, tot_guarant, q.idealAssigned, curPlusPend))
				{
					orderedByNeed.AddItem(q);
				}
			}
			//assign all cluster resources until no more demand, or no resources are left
			while (!orderedByNeed.IsEmpty() && Resources.GreaterThan(rc, tot_guarant, unassigned
				, Resources.None()))
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource wQassigned = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0);
				// we compute normalizedGuarantees capacity based on currently active
				// queues
				ResetCapacity(rc, unassigned, orderedByNeed, ignoreGuarantee);
				// For each underserved queue (or set of queues if multiple are equally
				// underserved), offer its share of the unassigned resources based on its
				// normalized guarantee. After the offer, if the queue is not satisfied,
				// place it back in the ordered list of queues, recalculating its place
				// in the order of most under-guaranteed to most over-guaranteed. In this
				// way, the most underserved queue(s) are always given resources first.
				ICollection<ProportionalCapacityPreemptionPolicy.TempQueue> underserved = GetMostUnderservedQueues
					(orderedByNeed, tqComparator);
				for (IEnumerator<ProportionalCapacityPreemptionPolicy.TempQueue> i_1 = underserved
					.GetEnumerator(); i_1.HasNext(); )
				{
					ProportionalCapacityPreemptionPolicy.TempQueue sub = i_1.Next();
					Org.Apache.Hadoop.Yarn.Api.Records.Resource wQavail = Resources.MultiplyAndNormalizeUp
						(rc, unassigned, sub.normalizedGuarantee, Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(1, 1));
					Org.Apache.Hadoop.Yarn.Api.Records.Resource wQidle = sub.Offer(wQavail, rc, tot_guarant
						);
					Org.Apache.Hadoop.Yarn.Api.Records.Resource wQdone = Resources.Subtract(wQavail, 
						wQidle);
					if (Resources.GreaterThan(rc, tot_guarant, wQdone, Resources.None()))
					{
						// The queue is still asking for more. Put it back in the priority
						// queue, recalculating its order based on need.
						orderedByNeed.AddItem(sub);
					}
					Resources.AddTo(wQassigned, wQdone);
				}
				Resources.SubtractFrom(unassigned, wQassigned);
			}
		}

		// Take the most underserved TempQueue (the one on the head). Collect and
		// return the list of all queues that have the same idealAssigned
		// percentage of guaranteed.
		protected internal virtual ICollection<ProportionalCapacityPreemptionPolicy.TempQueue
			> GetMostUnderservedQueues(PriorityQueue<ProportionalCapacityPreemptionPolicy.TempQueue
			> orderedByNeed, ProportionalCapacityPreemptionPolicy.TQComparator tqComparator)
		{
			AList<ProportionalCapacityPreemptionPolicy.TempQueue> underserved = new AList<ProportionalCapacityPreemptionPolicy.TempQueue
				>();
			while (!orderedByNeed.IsEmpty())
			{
				ProportionalCapacityPreemptionPolicy.TempQueue q1 = orderedByNeed.Remove();
				underserved.AddItem(q1);
				ProportionalCapacityPreemptionPolicy.TempQueue q2 = orderedByNeed.Peek();
				// q1's pct of guaranteed won't be larger than q2's. If it's less, then
				// return what has already been collected. Otherwise, q1's pct of
				// guaranteed == that of q2, so add q2 to underserved list during the
				// next pass.
				if (q2 == null || tqComparator.Compare(q1, q2) < 0)
				{
					return underserved;
				}
			}
			return underserved;
		}

		/// <summary>Computes a normalizedGuaranteed capacity based on active queues</summary>
		/// <param name="rc">resource calculator</param>
		/// <param name="clusterResource">the total amount of resources in the cluster</param>
		/// <param name="queues">the list of queues to consider</param>
		private void ResetCapacity(ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ICollection<ProportionalCapacityPreemptionPolicy.TempQueue> queues
			, bool ignoreGuar)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource activeCap = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			if (ignoreGuar)
			{
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue q in queues)
				{
					q.normalizedGuarantee = (float)1.0f / ((float)queues.Count);
				}
			}
			else
			{
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue q in queues)
				{
					Resources.AddTo(activeCap, q.guaranteed);
				}
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue q_1 in queues)
				{
					q_1.normalizedGuarantee = Resources.Divide(rc, clusterResource, q_1.guaranteed, activeCap
						);
				}
			}
		}

		/// <summary>
		/// Based a resource preemption target drop reservations of containers and
		/// if necessary select containers for preemption from applications in each
		/// over-capacity queue.
		/// </summary>
		/// <remarks>
		/// Based a resource preemption target drop reservations of containers and
		/// if necessary select containers for preemption from applications in each
		/// over-capacity queue. It uses
		/// <see cref="NaturalTerminationFactor"/>
		/// to
		/// account for containers that will naturally complete.
		/// </remarks>
		/// <param name="queues">set of leaf queues to preempt from</param>
		/// <param name="clusterResource">total amount of cluster resources</param>
		/// <returns>a map of applciationID to set of containers to preempt</returns>
		private IDictionary<ApplicationAttemptId, ICollection<RMContainer>> GetContainersToPreempt
			(IList<ProportionalCapacityPreemptionPolicy.TempQueue> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			IDictionary<ApplicationAttemptId, ICollection<RMContainer>> preemptMap = new Dictionary
				<ApplicationAttemptId, ICollection<RMContainer>>();
			IList<RMContainer> skippedAMContainerlist = new AList<RMContainer>();
			foreach (ProportionalCapacityPreemptionPolicy.TempQueue qT in queues)
			{
				if (qT.preemptionDisabled && qT.leafQueue != null)
				{
					if (Log.IsDebugEnabled())
					{
						if (Resources.GreaterThan(rc, clusterResource, qT.toBePreempted, Org.Apache.Hadoop.Yarn.Api.Records.Resource
							.NewInstance(0, 0)))
						{
							Log.Debug("Tried to preempt the following " + "resources from non-preemptable queue: "
								 + qT.queueName + " - Resources: " + qT.toBePreempted);
						}
					}
					continue;
				}
				// we act only if we are violating balance by more than
				// maxIgnoredOverCapacity
				if (Resources.GreaterThan(rc, clusterResource, qT.current, Resources.Multiply(qT.
					guaranteed, 1.0 + maxIgnoredOverCapacity)))
				{
					// we introduce a dampening factor naturalTerminationFactor that
					// accounts for natural termination of containers
					Org.Apache.Hadoop.Yarn.Api.Records.Resource resToObtain = Resources.Multiply(qT.toBePreempted
						, naturalTerminationFactor);
					Org.Apache.Hadoop.Yarn.Api.Records.Resource skippedAMSize = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					// lock the leafqueue while we scan applications and unreserve
					lock (qT.leafQueue)
					{
						NavigableSet<FiCaSchedulerApp> ns = (NavigableSet<FiCaSchedulerApp>)qT.leafQueue.
							GetApplications();
						IEnumerator<FiCaSchedulerApp> desc = ns.DescendingIterator();
						qT.actuallyPreempted = Resources.Clone(resToObtain);
						while (desc.HasNext())
						{
							FiCaSchedulerApp fc = desc.Next();
							if (Resources.LessThanOrEqual(rc, clusterResource, resToObtain, Resources.None()))
							{
								break;
							}
							preemptMap[fc.GetApplicationAttemptId()] = PreemptFrom(fc, clusterResource, resToObtain
								, skippedAMContainerlist, skippedAMSize);
						}
						Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAMCapacityForThisQueue = Resources
							.Multiply(Resources.Multiply(clusterResource, qT.leafQueue.GetAbsoluteCapacity()
							), qT.leafQueue.GetMaxAMResourcePerQueuePercent());
						// Can try preempting AMContainers (still saving atmost
						// maxAMCapacityForThisQueue AMResource's) if more resources are
						// required to be preempted from this Queue.
						PreemptAMContainers(clusterResource, preemptMap, skippedAMContainerlist, resToObtain
							, skippedAMSize, maxAMCapacityForThisQueue);
					}
				}
			}
			return preemptMap;
		}

		/// <summary>
		/// As more resources are needed for preemption, saved AMContainers has to be
		/// rescanned.
		/// </summary>
		/// <remarks>
		/// As more resources are needed for preemption, saved AMContainers has to be
		/// rescanned. Such AMContainers can be preempted based on resToObtain, but
		/// maxAMCapacityForThisQueue resources will be still retained.
		/// </remarks>
		/// <param name="clusterResource"/>
		/// <param name="preemptMap"/>
		/// <param name="skippedAMContainerlist"/>
		/// <param name="resToObtain"/>
		/// <param name="skippedAMSize"/>
		/// <param name="maxAMCapacityForThisQueue"/>
		private void PreemptAMContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, IDictionary<ApplicationAttemptId, ICollection<RMContainer>> preemptMap, IList<
			RMContainer> skippedAMContainerlist, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resToObtain, Org.Apache.Hadoop.Yarn.Api.Records.Resource skippedAMSize, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAMCapacityForThisQueue)
		{
			foreach (RMContainer c in skippedAMContainerlist)
			{
				// Got required amount of resources for preemption, can stop now
				if (Resources.LessThanOrEqual(rc, clusterResource, resToObtain, Resources.None()))
				{
					break;
				}
				// Once skippedAMSize reaches down to maxAMCapacityForThisQueue,
				// container selection iteration for preemption will be stopped. 
				if (Resources.LessThanOrEqual(rc, clusterResource, skippedAMSize, maxAMCapacityForThisQueue
					))
				{
					break;
				}
				ICollection<RMContainer> contToPrempt = preemptMap[c.GetApplicationAttemptId()];
				if (null == contToPrempt)
				{
					contToPrempt = new HashSet<RMContainer>();
					preemptMap[c.GetApplicationAttemptId()] = contToPrempt;
				}
				contToPrempt.AddItem(c);
				Resources.SubtractFrom(resToObtain, c.GetContainer().GetResource());
				Resources.SubtractFrom(skippedAMSize, c.GetContainer().GetResource());
			}
			skippedAMContainerlist.Clear();
		}

		/// <summary>
		/// Given a target preemption for a specific application, select containers
		/// to preempt (after unreserving all reservation for that app).
		/// </summary>
		/// <param name="app"/>
		/// <param name="clusterResource"/>
		/// <param name="rsrcPreempt"/>
		/// <returns>Set<RMContainer> Set of RMContainers</returns>
		private ICollection<RMContainer> PreemptFrom(FiCaSchedulerApp app, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource rsrcPreempt, IList
			<RMContainer> skippedAMContainerlist, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 skippedAMSize)
		{
			ICollection<RMContainer> ret = new HashSet<RMContainer>();
			ApplicationAttemptId appId = app.GetApplicationAttemptId();
			// first drop reserved containers towards rsrcPreempt
			IList<RMContainer> reservations = new AList<RMContainer>(app.GetReservedContainers
				());
			foreach (RMContainer c in reservations)
			{
				if (Resources.LessThanOrEqual(rc, clusterResource, rsrcPreempt, Resources.None()))
				{
					return ret;
				}
				if (!observeOnly)
				{
					rmContext.GetDispatcher().GetEventHandler().Handle(new ContainerPreemptEvent(appId
						, c, SchedulerEventType.DropReservation));
				}
				Resources.SubtractFrom(rsrcPreempt, c.GetContainer().GetResource());
			}
			// if more resources are to be freed go through all live containers in
			// reverse priority and reverse allocation order and mark them for
			// preemption
			IList<RMContainer> containers = new AList<RMContainer>(app.GetLiveContainers());
			SortContainers(containers);
			foreach (RMContainer c_1 in containers)
			{
				if (Resources.LessThanOrEqual(rc, clusterResource, rsrcPreempt, Resources.None()))
				{
					return ret;
				}
				// Skip AM Container from preemption for now.
				if (c_1.IsAMContainer())
				{
					skippedAMContainerlist.AddItem(c_1);
					Resources.AddTo(skippedAMSize, c_1.GetContainer().GetResource());
					continue;
				}
				// skip Labeled resource
				if (IsLabeledContainer(c_1))
				{
					continue;
				}
				ret.AddItem(c_1);
				Resources.SubtractFrom(rsrcPreempt, c_1.GetContainer().GetResource());
			}
			return ret;
		}

		/// <summary>Checking if given container is a labeled container</summary>
		/// <param name="c"/>
		/// <returns>true/false</returns>
		private bool IsLabeledContainer(RMContainer c)
		{
			return labels.Contains(c.GetAllocatedNode());
		}

		/// <summary>
		/// Compare by reversed priority order first, and then reversed containerId
		/// order
		/// </summary>
		/// <param name="containers"/>
		[VisibleForTesting]
		internal static void SortContainers(IList<RMContainer> containers)
		{
			containers.Sort(new _IComparer_705());
		}

		private sealed class _IComparer_705 : IComparer<RMContainer>
		{
			public _IComparer_705()
			{
			}

			public int Compare(RMContainer a, RMContainer b)
			{
				IComparer<Priority> c = new Priority.Comparator();
				int priorityComp = c.Compare(b.GetContainer().GetPriority(), a.GetContainer().GetPriority
					());
				if (priorityComp != 0)
				{
					return priorityComp;
				}
				return b.GetContainerId().CompareTo(a.GetContainerId());
			}
		}

		public virtual long GetMonitoringInterval()
		{
			return monitoringInterval;
		}

		public virtual string GetPolicyName()
		{
			return "ProportionalCapacityPreemptionPolicy";
		}

		/// <summary>
		/// This method walks a tree of CSQueue and clones the portion of the state
		/// relevant for preemption in TempQueue(s).
		/// </summary>
		/// <remarks>
		/// This method walks a tree of CSQueue and clones the portion of the state
		/// relevant for preemption in TempQueue(s). It also maintains a pointer to
		/// the leaves. Finally it aggregates pending resources in each queue and rolls
		/// it up to higher levels.
		/// </remarks>
		/// <param name="root">the root of the CapacityScheduler queue hierarchy</param>
		/// <param name="clusterResources">the total amount of resources in the cluster</param>
		/// <returns>the root of the cloned queue hierarchy</returns>
		private ProportionalCapacityPreemptionPolicy.TempQueue CloneQueues(CSQueue root, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources)
		{
			ProportionalCapacityPreemptionPolicy.TempQueue ret;
			lock (root)
			{
				string queueName = root.GetQueueName();
				float absUsed = root.GetAbsoluteUsedCapacity();
				float absCap = root.GetAbsoluteCapacity();
				float absMaxCap = root.GetAbsoluteMaximumCapacity();
				bool preemptionDisabled = root.GetPreemptionDisabled();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource current = Resources.Multiply(clusterResources
					, absUsed);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource guaranteed = Resources.Multiply(clusterResources
					, absCap);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource maxCapacity = Resources.Multiply(clusterResources
					, absMaxCap);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource extra = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0);
				if (Resources.GreaterThan(rc, clusterResources, current, guaranteed))
				{
					extra = Resources.Subtract(current, guaranteed);
				}
				if (root is LeafQueue)
				{
					LeafQueue l = (LeafQueue)root;
					Org.Apache.Hadoop.Yarn.Api.Records.Resource pending = l.GetTotalResourcePending();
					ret = new ProportionalCapacityPreemptionPolicy.TempQueue(queueName, current, pending
						, guaranteed, maxCapacity, preemptionDisabled);
					if (preemptionDisabled)
					{
						ret.untouchableExtra = extra;
					}
					else
					{
						ret.preemptableExtra = extra;
					}
					ret.SetLeafQueue(l);
				}
				else
				{
					Org.Apache.Hadoop.Yarn.Api.Records.Resource pending = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					ret = new ProportionalCapacityPreemptionPolicy.TempQueue(root.GetQueueName(), current
						, pending, guaranteed, maxCapacity, false);
					Org.Apache.Hadoop.Yarn.Api.Records.Resource childrensPreemptable = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					foreach (CSQueue c in root.GetChildQueues())
					{
						ProportionalCapacityPreemptionPolicy.TempQueue subq = CloneQueues(c, clusterResources
							);
						Resources.AddTo(childrensPreemptable, subq.preemptableExtra);
						ret.AddChild(subq);
					}
					// untouchableExtra = max(extra - childrenPreemptable, 0)
					if (Resources.GreaterThanOrEqual(rc, clusterResources, childrensPreemptable, extra
						))
					{
						ret.untouchableExtra = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 
							0);
					}
					else
					{
						ret.untouchableExtra = Resources.SubtractFrom(extra, childrensPreemptable);
					}
				}
			}
			return ret;
		}

		// simple printout function that reports internal queue state (useful for
		// plotting)
		private void LogToCSV(IList<ProportionalCapacityPreemptionPolicy.TempQueue> unorderedqueues
			)
		{
			IList<ProportionalCapacityPreemptionPolicy.TempQueue> queues = new AList<ProportionalCapacityPreemptionPolicy.TempQueue
				>(unorderedqueues);
			queues.Sort(new _IComparer_796());
			string queueState = " QUEUESTATE: " + clock.GetTime();
			StringBuilder sb = new StringBuilder();
			sb.Append(queueState);
			foreach (ProportionalCapacityPreemptionPolicy.TempQueue tq in queues)
			{
				sb.Append(", ");
				tq.AppendLogString(sb);
			}
			Log.Debug(sb.ToString());
		}

		private sealed class _IComparer_796 : IComparer<ProportionalCapacityPreemptionPolicy.TempQueue
			>
		{
			public _IComparer_796()
			{
			}

			public int Compare(ProportionalCapacityPreemptionPolicy.TempQueue o1, ProportionalCapacityPreemptionPolicy.TempQueue
				 o2)
			{
				return string.CompareOrdinal(o1.queueName, o2.queueName);
			}
		}

		/// <summary>
		/// Temporary data-structure tracking resource availability, pending resource
		/// need, current utilization.
		/// </summary>
		/// <remarks>
		/// Temporary data-structure tracking resource availability, pending resource
		/// need, current utilization. Used to clone
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CSQueue
		/// 	"/>
		/// .
		/// </remarks>
		internal class TempQueue
		{
			internal readonly string queueName;

			internal readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource current;

			internal readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource pending;

			internal readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource guaranteed;

			internal readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource maxCapacity;

			internal Org.Apache.Hadoop.Yarn.Api.Records.Resource idealAssigned;

			internal Org.Apache.Hadoop.Yarn.Api.Records.Resource toBePreempted;

			internal Org.Apache.Hadoop.Yarn.Api.Records.Resource actuallyPreempted;

			internal Org.Apache.Hadoop.Yarn.Api.Records.Resource untouchableExtra;

			internal Org.Apache.Hadoop.Yarn.Api.Records.Resource preemptableExtra;

			internal double normalizedGuarantee;

			internal readonly AList<ProportionalCapacityPreemptionPolicy.TempQueue> children;

			internal LeafQueue leafQueue;

			internal bool preemptionDisabled;

			internal TempQueue(string queueName, Org.Apache.Hadoop.Yarn.Api.Records.Resource 
				current, Org.Apache.Hadoop.Yarn.Api.Records.Resource pending, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 guaranteed, Org.Apache.Hadoop.Yarn.Api.Records.Resource maxCapacity, bool preemptionDisabled
				)
			{
				this.queueName = queueName;
				this.current = current;
				this.pending = pending;
				this.guaranteed = guaranteed;
				this.maxCapacity = maxCapacity;
				this.idealAssigned = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0
					);
				this.actuallyPreempted = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(
					0, 0);
				this.toBePreempted = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0
					);
				this.normalizedGuarantee = float.NaN;
				this.children = new AList<ProportionalCapacityPreemptionPolicy.TempQueue>();
				this.untouchableExtra = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0
					, 0);
				this.preemptableExtra = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0
					, 0);
				this.preemptionDisabled = preemptionDisabled;
			}

			public virtual void SetLeafQueue(LeafQueue l)
			{
				System.Diagnostics.Debug.Assert(children.Count == 0);
				this.leafQueue = l;
			}

			/// <summary>When adding a child we also aggregate its pending resource needs.</summary>
			/// <param name="q">the child queue to add to this queue</param>
			public virtual void AddChild(ProportionalCapacityPreemptionPolicy.TempQueue q)
			{
				System.Diagnostics.Debug.Assert(leafQueue == null);
				children.AddItem(q);
				Resources.AddTo(pending, q.pending);
			}

			public virtual void AddChildren(AList<ProportionalCapacityPreemptionPolicy.TempQueue
				> queues)
			{
				System.Diagnostics.Debug.Assert(leafQueue == null);
				Sharpen.Collections.AddAll(children, queues);
			}

			public virtual AList<ProportionalCapacityPreemptionPolicy.TempQueue> GetChildren(
				)
			{
				return children;
			}

			// This function "accepts" all the resources it can (pending) and return
			// the unused ones
			internal virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource Offer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 avail, ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
				)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource absMaxCapIdealAssignedDelta = Resources
					.ComponentwiseMax(Resources.Subtract(maxCapacity, idealAssigned), Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0));
				// remain = avail - min(avail, (max - assigned), (current + pending - assigned))
				Org.Apache.Hadoop.Yarn.Api.Records.Resource accepted = Resources.Min(rc, clusterResource
					, absMaxCapIdealAssignedDelta, Resources.Min(rc, clusterResource, avail, Resources
					.Subtract(Resources.Add(current, pending), idealAssigned)));
				Org.Apache.Hadoop.Yarn.Api.Records.Resource remain = Resources.Subtract(avail, accepted
					);
				Resources.AddTo(idealAssigned, accepted);
				return remain;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(" NAME: " + queueName).Append(" CUR: ").Append(current).Append(" PEN: "
					).Append(pending).Append(" GAR: ").Append(guaranteed).Append(" NORM: ").Append(normalizedGuarantee
					).Append(" IDEAL_ASSIGNED: ").Append(idealAssigned).Append(" IDEAL_PREEMPT: ").Append
					(toBePreempted).Append(" ACTUAL_PREEMPT: ").Append(actuallyPreempted).Append(" UNTOUCHABLE: "
					).Append(untouchableExtra).Append(" PREEMPTABLE: ").Append(preemptableExtra).Append
					("\n");
				return sb.ToString();
			}

			public virtual void PrintAll()
			{
				Log.Info(this.ToString());
				foreach (ProportionalCapacityPreemptionPolicy.TempQueue sub in this.GetChildren())
				{
					sub.PrintAll();
				}
			}

			public virtual void AssignPreemption(float scalingFactor, ResourceCalculator rc, 
				Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource)
			{
				if (Resources.GreaterThan(rc, clusterResource, current, idealAssigned))
				{
					toBePreempted = Resources.Multiply(Resources.Subtract(current, idealAssigned), scalingFactor
						);
				}
				else
				{
					toBePreempted = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);
				}
			}

			internal virtual void AppendLogString(StringBuilder sb)
			{
				sb.Append(queueName).Append(", ").Append(current.GetMemory()).Append(", ").Append
					(current.GetVirtualCores()).Append(", ").Append(pending.GetMemory()).Append(", "
					).Append(pending.GetVirtualCores()).Append(", ").Append(guaranteed.GetMemory()).
					Append(", ").Append(guaranteed.GetVirtualCores()).Append(", ").Append(idealAssigned
					.GetMemory()).Append(", ").Append(idealAssigned.GetVirtualCores()).Append(", ").
					Append(toBePreempted.GetMemory()).Append(", ").Append(toBePreempted.GetVirtualCores
					()).Append(", ").Append(actuallyPreempted.GetMemory()).Append(", ").Append(actuallyPreempted
					.GetVirtualCores());
			}
		}

		internal class TQComparator : IComparer<ProportionalCapacityPreemptionPolicy.TempQueue
			>
		{
			private ResourceCalculator rc;

			private Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterRes;

			internal TQComparator(ResourceCalculator rc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 clusterRes)
			{
				this.rc = rc;
				this.clusterRes = clusterRes;
			}

			public virtual int Compare(ProportionalCapacityPreemptionPolicy.TempQueue tq1, ProportionalCapacityPreemptionPolicy.TempQueue
				 tq2)
			{
				if (GetIdealPctOfGuaranteed(tq1) < GetIdealPctOfGuaranteed(tq2))
				{
					return -1;
				}
				if (GetIdealPctOfGuaranteed(tq1) > GetIdealPctOfGuaranteed(tq2))
				{
					return 1;
				}
				return 0;
			}

			// Calculates idealAssigned / guaranteed
			// TempQueues with 0 guarantees are always considered the most over
			// capacity and therefore considered last for resources.
			private double GetIdealPctOfGuaranteed(ProportionalCapacityPreemptionPolicy.TempQueue
				 q)
			{
				double pctOver = int.MaxValue;
				if (q != null && Resources.GreaterThan(rc, clusterRes, q.guaranteed, Resources.None
					()))
				{
					pctOver = Resources.Divide(rc, clusterRes, q.idealAssigned, q.guaranteed);
				}
				return (pctOver);
			}
		}
	}
}
