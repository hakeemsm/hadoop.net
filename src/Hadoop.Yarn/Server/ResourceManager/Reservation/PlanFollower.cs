using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// A PlanFollower is a component that runs on a timer, and synchronizes the
	/// underlying
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
	/// 	"/>
	/// with the
	/// <see cref="Plan"/>
	/// (s) and viceversa.
	/// While different implementations might operate differently, the key idea is to
	/// map the current allocation of resources for each active reservation in the
	/// plan(s), to a corresponding notion in the underlying scheduler (e.g., tuning
	/// capacity of queues, set pool weights, or tweak application priorities). The
	/// goal is to affect the dynamic allocation of resources done by the scheduler
	/// so that the jobs obtain access to resources in a way that is consistent with
	/// the reservations in the plan. A key conceptual step here is to convert the
	/// absolute-valued promises made in the reservations to appropriate relative
	/// priorities/queue sizes etc.
	/// Symmetrically the PlanFollower exposes changes in cluster conditions (as
	/// tracked by the scheduler) to the plan, e.g., the overall amount of physical
	/// resources available. The Plan in turn can react by replanning its allocations
	/// if appropriate.
	/// The implementation can assume that is run frequently enough to be able to
	/// observe and react to normal operational changes in cluster conditions on the
	/// fly (e.g., if cluster resources drop, we can update the relative weights of a
	/// queue so that the absolute promises made to the job at reservation time are
	/// respected).
	/// However, due to RM restarts and the related downtime, it is advisable for
	/// implementations to operate in a stateless way, and be able to synchronize the
	/// state of plans/scheduler regardless of how big is the time gap between
	/// executions.
	/// </summary>
	public interface PlanFollower : Runnable
	{
		/// <summary>Init function that configures the PlanFollower, by providing:</summary>
		/// <param name="clock">a reference to the system clock.</param>
		/// <param name="sched">a reference to the underlying scheduler</param>
		/// <param name="plans">
		/// references to the plans we should keep synchronized at every
		/// time tick.
		/// </param>
		void Init(Clock clock, ResourceScheduler sched, ICollection<Plan> plans);

		/// <summary>
		/// The function performing the actual synchronization operation for a given
		/// Plan.
		/// </summary>
		/// <remarks>
		/// The function performing the actual synchronization operation for a given
		/// Plan. This is normally invoked by the run method, but it can be invoked
		/// synchronously to avoid race conditions when a user's reservation request
		/// start time is imminent.
		/// </remarks>
		/// <param name="plan">the Plan to synchronize</param>
		void SynchronizePlan(Plan plan);

		/// <summary>Setter for the list of plans.</summary>
		/// <param name="plans">the collection of Plans we operate on at every time tick.</param>
		void SetPlans(ICollection<Plan> plans);
	}
}
