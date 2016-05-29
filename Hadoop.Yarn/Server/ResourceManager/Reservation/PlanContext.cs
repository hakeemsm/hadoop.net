using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This interface provides read-only access to configuration-type parameter for
	/// a plan.
	/// </summary>
	public interface PlanContext
	{
		/// <summary>Returns the configured "step" or granularity of time of the plan in millis.
		/// 	</summary>
		/// <returns>plan step in millis</returns>
		long GetStep();

		/// <summary>
		/// Return the
		/// <see cref="ReservationAgent"/>
		/// configured for this plan that is
		/// responsible for optimally placing various reservation requests
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="ReservationAgent"/>
		/// configured for this plan
		/// </returns>
		ReservationAgent GetReservationAgent();

		/// <summary>
		/// Return an instance of a
		/// <see cref="Planner"/>
		/// , which will be invoked in response
		/// to unexpected reduction in the resources of this plan
		/// </summary>
		/// <returns>
		/// an instance of a
		/// <see cref="Planner"/>
		/// , which will be invoked in response
		/// to unexpected reduction in the resources of this plan
		/// </returns>
		Planner GetReplanner();

		/// <summary>
		/// Return the configured
		/// <see cref="SharingPolicy"/>
		/// that governs the sharing of the
		/// resources of the plan between its various users
		/// </summary>
		/// <returns>
		/// the configured
		/// <see cref="SharingPolicy"/>
		/// that governs the sharing of
		/// the resources of the plan between its various users
		/// </returns>
		SharingPolicy GetSharingPolicy();

		/// <summary>
		/// Returns the system
		/// <see cref="Org.Apache.Hadoop.Yarn.Util.Resource.ResourceCalculator"/>
		/// </summary>
		/// <returns>
		/// the system
		/// <see cref="Org.Apache.Hadoop.Yarn.Util.Resource.ResourceCalculator"/>
		/// </returns>
		ResourceCalculator GetResourceCalculator();

		/// <summary>
		/// Returns the single smallest
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocation that can be
		/// reserved in this plan
		/// </summary>
		/// <returns>
		/// the single smallest
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocation that can be
		/// reserved in this plan
		/// </returns>
		Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinimumAllocation();

		/// <summary>
		/// Returns the single largest
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocation that can be reserved
		/// in this plan
		/// </summary>
		/// <returns>
		/// the single largest
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocation that can be reserved
		/// in this plan
		/// </returns>
		Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocation();

		/// <summary>
		/// Return the name of the queue in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
		/// 	"/>
		/// corresponding
		/// to this plan
		/// </summary>
		/// <returns>
		/// the name of the queue in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
		/// 	"/>
		/// corresponding to this plan
		/// </returns>
		string GetQueueName();

		/// <summary>
		/// Return the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics"/
		/// 	>
		/// for the queue in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
		/// 	"/>
		/// corresponding to this plan
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics"/
		/// 	>
		/// for the queue in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
		/// 	"/>
		/// corresponding to this plan
		/// </returns>
		QueueMetrics GetQueueMetrics();

		/// <summary>
		/// Instructs the
		/// <see cref="PlanFollower"/>
		/// on what to do for applications
		/// which are still running when the reservation is expiring (move-to-default
		/// vs kill)
		/// </summary>
		/// <returns>
		/// true if remaining applications have to be killed, false if they
		/// have to migrated
		/// </returns>
		bool GetMoveOnExpiry();
	}
}
