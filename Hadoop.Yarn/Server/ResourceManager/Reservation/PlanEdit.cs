using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>This interface groups the methods used to modify the state of a Plan.</summary>
	public interface PlanEdit : PlanContext, PlanView
	{
		/// <summary>
		/// Add a new
		/// <see cref="ReservationAllocation"/>
		/// to the plan
		/// </summary>
		/// <param name="reservation">
		/// the
		/// <see cref="ReservationAllocation"/>
		/// to be added to the
		/// plan
		/// </param>
		/// <returns>true if addition is successful, false otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		bool AddReservation(ReservationAllocation reservation);

		/// <summary>
		/// Updates an existing
		/// <see cref="ReservationAllocation"/>
		/// in the plan. This is
		/// required for re-negotiation
		/// </summary>
		/// <param name="reservation">
		/// the
		/// <see cref="ReservationAllocation"/>
		/// to be updated the plan
		/// </param>
		/// <returns>true if update is successful, false otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		bool UpdateReservation(ReservationAllocation reservation);

		/// <summary>
		/// Delete an existing
		/// <see cref="ReservationAllocation"/>
		/// from the plan identified
		/// uniquely by its
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// . This will generally be used for
		/// garbage collection
		/// </summary>
		/// <param name="reservationID">
		/// the
		/// <see cref="ReservationAllocation"/>
		/// to be deleted from
		/// the plan identified uniquely by its
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// </param>
		/// <returns>true if delete is successful, false otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		bool DeleteReservation(ReservationId reservationID);

		/// <summary>Method invoked to garbage collect old reservations.</summary>
		/// <remarks>
		/// Method invoked to garbage collect old reservations. It cleans up expired
		/// reservations that have fallen out of the sliding archival window
		/// </remarks>
		/// <param name="tick">the current time from which the archival window is computed</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		void ArchiveCompletedReservations(long tick);

		/// <summary>
		/// Sets the overall capacity in terms of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// assigned to this
		/// plan
		/// </summary>
		/// <param name="capacity">
		/// the overall capacity in terms of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// assigned
		/// to this plan
		/// </param>
		void SetTotalCapacity(Resource capacity);
	}
}
