using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationUpdateRequest"/>
	/// captures the set of requirements the user
	/// has to update an existing reservation.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationUpdateRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationUpdateRequest NewInstance(ReservationDefinition reservationDefinition
			, ReservationId reservationId)
		{
			ReservationUpdateRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				ReservationUpdateRequest>();
			request.SetReservationDefinition(reservationDefinition);
			request.SetReservationId(reservationId);
			return request;
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// representing the updated user
		/// constraints for this reservation
		/// </summary>
		/// <returns>the reservation definition representing user constraints</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationDefinition GetReservationDefinition();

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// representing the updated user
		/// constraints for this reservation
		/// </summary>
		/// <param name="reservationDefinition">
		/// the reservation request representing the
		/// reservation
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetReservationDefinition(ReservationDefinition reservationDefinition
			);

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// , that corresponds to a valid resource
		/// allocation in the scheduler (between start and end time of this
		/// reservation)
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// representing the unique id of the
		/// corresponding reserved resource allocation in the scheduler
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationId GetReservationId();

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// , that correspond to a valid resource
		/// allocation in the scheduler (between start and end time of this
		/// reservation)
		/// </summary>
		/// <param name="reservationId">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// representing the the unique
		/// id of the corresponding reserved resource allocation in the
		/// scheduler
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetReservationId(ReservationId reservationId);
	}
}
