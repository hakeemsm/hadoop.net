using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>An entity that seeks to acquire resources to satisfy an user's contract</summary>
	public interface ReservationAgent
	{
		/// <summary>Create a reservation for the user that abides by the specified contract</summary>
		/// <param name="reservationId">the identifier of the reservation to be created.</param>
		/// <param name="user">the user who wants to create the reservation</param>
		/// <param name="plan">the Plan to which the reservation must be fitted</param>
		/// <param name="contract">
		/// encapsulates the resources the user requires for his
		/// session
		/// </param>
		/// <returns>whether the create operation was successful or not</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	">if the session cannot be fitted into the plan</exception>
		bool CreateReservation(ReservationId reservationId, string user, Plan plan, ReservationDefinition
			 contract);

		/// <summary>Update a reservation for the user that abides by the specified contract</summary>
		/// <param name="reservationId">the identifier of the reservation to be updated</param>
		/// <param name="user">the user who wants to create the session</param>
		/// <param name="plan">the Plan to which the reservation must be fitted</param>
		/// <param name="contract">
		/// encapsulates the resources the user requires for his
		/// reservation
		/// </param>
		/// <returns>whether the update operation was successful or not</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	">if the reservation cannot be fitted into the plan</exception>
		bool UpdateReservation(ReservationId reservationId, string user, Plan plan, ReservationDefinition
			 contract);

		/// <summary>Delete an user reservation</summary>
		/// <param name="reservationId">the identifier of the reservation to be deleted</param>
		/// <param name="user">the user who wants to create the reservation</param>
		/// <param name="plan">the Plan to which the session must be fitted</param>
		/// <returns>whether the delete operation was successful or not</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	">if the reservation cannot be fitted into the plan</exception>
		bool DeleteReservation(ReservationId reservationId, string user, Plan plan);
	}
}
