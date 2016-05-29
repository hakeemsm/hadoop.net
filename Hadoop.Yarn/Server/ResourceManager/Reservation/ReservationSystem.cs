using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This interface is the one implemented by any system that wants to support
	/// Reservations i.e.
	/// </summary>
	/// <remarks>
	/// This interface is the one implemented by any system that wants to support
	/// Reservations i.e. make
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
	/// allocations in future. Implementors
	/// need to bootstrap all configured
	/// <see cref="Plan"/>
	/// s in the active
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
	/// 	"/>
	/// along with their corresponding
	/// <see cref="ReservationAgent"/>
	/// and
	/// <see cref="SharingPolicy"/>
	/// . It is also responsible
	/// for managing the
	/// <see cref="PlanFollower"/>
	/// to ensure the
	/// <see cref="Plan"/>
	/// s are in sync
	/// with the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
	/// 	"/>
	/// .
	/// </remarks>
	public interface ReservationSystem
	{
		/// <summary>
		/// Set RMContext for
		/// <see cref="ReservationSystem"/>
		/// . This method should be called
		/// immediately after instantiating a reservation system once.
		/// </summary>
		/// <param name="rmContext">
		/// created by
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceManager"/>
		/// </param>
		void SetRMContext(RMContext rmContext);

		/// <summary>
		/// Re-initialize the
		/// <see cref="ReservationSystem"/>
		/// .
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="rmContext">
		/// current context of the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceManager"/>
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void Reinitialize(Configuration conf, RMContext rmContext);

		/// <summary>
		/// Get an existing
		/// <see cref="Plan"/>
		/// that has been initialized.
		/// </summary>
		/// <param name="planName">
		/// the name of the
		/// <see cref="Plan"/>
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Plan"/>
		/// identified by name
		/// </returns>
		Plan GetPlan(string planName);

		/// <summary>
		/// Return a map containing all the plans known to this ReservationSystem
		/// (useful for UI)
		/// </summary>
		/// <returns>a Map of Plan names and Plan objects</returns>
		IDictionary<string, Plan> GetAllPlans();

		/// <summary>
		/// Invokes
		/// <see cref="PlanFollower"/>
		/// to synchronize the specified
		/// <see cref="Plan"/>
		/// with
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ResourceScheduler
		/// 	"/>
		/// </summary>
		/// <param name="planName">
		/// the name of the
		/// <see cref="Plan"/>
		/// to be synchronized
		/// </param>
		void SynchronizePlan(string planName);

		/// <summary>
		/// Return the time step (ms) at which the
		/// <see cref="PlanFollower"/>
		/// is invoked
		/// </summary>
		/// <returns>
		/// the time step (ms) at which the
		/// <see cref="PlanFollower"/>
		/// is invoked
		/// </returns>
		long GetPlanFollowerTimeStep();

		/// <summary>
		/// Get a new unique
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// .
		/// </summary>
		/// <returns>
		/// a new unique
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// </returns>
		ReservationId GetNewReservationId();

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Queue"/>
		/// that an existing
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// is associated
		/// with.
		/// </summary>
		/// <param name="reservationId">the unique id of the reservation</param>
		/// <returns>the name of the associated Queue</returns>
		string GetQueueForReservation(ReservationId reservationId);

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Queue"/>
		/// that an existing
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// should be
		/// associated with.
		/// </summary>
		/// <param name="reservationId">the unique id of the reservation</param>
		/// <param name="queueName">the name of Queue to associate the reservation with</param>
		void SetQueueForReservation(ReservationId reservationId, string queueName);
	}
}
