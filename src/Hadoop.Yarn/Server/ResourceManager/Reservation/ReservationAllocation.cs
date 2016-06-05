using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// A ReservationAllocation represents a concrete allocation of resources over
	/// time that satisfy a certain
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	/// . This is used
	/// internally by a
	/// <see cref="Plan"/>
	/// to store information about how each of the
	/// accepted
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	/// have been allocated.
	/// </summary>
	public interface ReservationAllocation : Comparable<ReservationAllocation>
	{
		/// <summary>
		/// Returns the unique identifier
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// that represents the
		/// reservation
		/// </summary>
		/// <returns>
		/// reservationId the unique identifier
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// that
		/// represents the reservation
		/// </returns>
		ReservationId GetReservationId();

		/// <summary>
		/// Returns the original
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// submitted by the client
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// submitted by the client
		/// </returns>
		ReservationDefinition GetReservationDefinition();

		/// <summary>Returns the time at which the reservation is activated</summary>
		/// <returns>the time at which the reservation is activated</returns>
		long GetStartTime();

		/// <summary>Returns the time at which the reservation terminates</summary>
		/// <returns>the time at which the reservation terminates</returns>
		long GetEndTime();

		/// <summary>
		/// Returns the map of resources requested against the time interval for which
		/// they were
		/// </summary>
		/// <returns>
		/// the allocationRequests the map of resources requested against the
		/// time interval for which they were
		/// </returns>
		IDictionary<ReservationInterval, ReservationRequest> GetAllocationRequests();

		/// <summary>Return a string identifying the plan to which the reservation belongs</summary>
		/// <returns>the plan to which the reservation belongs</returns>
		string GetPlanName();

		/// <summary>Returns the user who requested the reservation</summary>
		/// <returns>the user who requested the reservation</returns>
		string GetUser();

		/// <summary>Returns whether the reservation has gang semantics or not</summary>
		/// <returns>true if there is a gang request, false otherwise</returns>
		bool ContainsGangs();

		/// <summary>Sets the time at which the reservation was accepted by the system</summary>
		/// <param name="acceptedAt">
		/// the time at which the reservation was accepted by the
		/// system
		/// </param>
		void SetAcceptanceTimestamp(long acceptedAt);

		/// <summary>Returns the time at which the reservation was accepted by the system</summary>
		/// <returns>the time at which the reservation was accepted by the system</returns>
		long GetAcceptanceTime();

		/// <summary>
		/// Returns the capacity represented by cumulative resources reserved by the
		/// reservation at the specified point of time
		/// </summary>
		/// <param name="tick">
		/// the time (UTC in ms) for which the reserved resources are
		/// requested
		/// </param>
		/// <returns>the resources reserved at the specified time</returns>
		Resource GetResourcesAtTime(long tick);
	}
}
