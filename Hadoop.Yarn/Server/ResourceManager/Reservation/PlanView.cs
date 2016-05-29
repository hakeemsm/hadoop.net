using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This interface provides a read-only view on the allocations made in this
	/// plan.
	/// </summary>
	/// <remarks>
	/// This interface provides a read-only view on the allocations made in this
	/// plan. This methods are used for example by
	/// <see cref="ReservationAgent"/>
	/// s to
	/// determine the free resources in a certain point in time, and by
	/// PlanFollowerPolicy to publish this plan to the scheduler.
	/// </remarks>
	public interface PlanView : PlanContext
	{
		/// <summary>
		/// Return a
		/// <see cref="ReservationAllocation"/>
		/// identified by its
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// </summary>
		/// <param name="reservationID">
		/// the unique id to identify the
		/// <see cref="ReservationAllocation"/>
		/// </param>
		/// <returns>
		/// 
		/// <see cref="ReservationAllocation"/>
		/// identified by the specified id
		/// </returns>
		ReservationAllocation GetReservationById(ReservationId reservationID);

		/// <summary>Gets all the active reservations at the specified point of time</summary>
		/// <param name="tick">
		/// the time (UTC in ms) for which the active reservations are
		/// requested
		/// </param>
		/// <returns>set of active reservations at the specified time</returns>
		ICollection<ReservationAllocation> GetReservationsAtTime(long tick);

		/// <summary>Gets all the reservations in the plan</summary>
		/// <returns>set of all reservations handled by this Plan</returns>
		ICollection<ReservationAllocation> GetAllReservations();

		/// <summary>
		/// Returns the total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// reserved for all users at the specified
		/// time
		/// </summary>
		/// <param name="tick">
		/// the time (UTC in ms) for which the reserved resources are
		/// requested
		/// </param>
		/// <returns>
		/// the total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// reserved for all users at the specified
		/// time
		/// </returns>
		Resource GetTotalCommittedResources(long tick);

		/// <summary>
		/// Returns the total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// reserved for a given user at the
		/// specified time
		/// </summary>
		/// <param name="user">the user who made the reservation(s)</param>
		/// <param name="tick">
		/// the time (UTC in ms) for which the reserved resources are
		/// requested
		/// </param>
		/// <returns>
		/// the total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// reserved for a given user at the
		/// specified time
		/// </returns>
		Resource GetConsumptionForUser(string user, long tick);

		/// <summary>
		/// Returns the overall capacity in terms of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// assigned to this
		/// plan (typically will correspond to the absolute capacity of the
		/// corresponding queue).
		/// </summary>
		/// <returns>
		/// the overall capacity in terms of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// assigned to this
		/// plan
		/// </returns>
		Resource GetTotalCapacity();

		/// <summary>Gets the time (UTC in ms) at which the first reservation starts</summary>
		/// <returns>the time (UTC in ms) at which the first reservation starts</returns>
		long GetEarliestStartTime();

		/// <summary>Returns the time (UTC in ms) at which the last reservation terminates</summary>
		/// <returns>the time (UTC in ms) at which the last reservation terminates</returns>
		long GetLastEndTime();
	}
}
