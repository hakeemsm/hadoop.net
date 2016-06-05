using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationDeleteRequest"/>
	/// captures the set of requirements the user
	/// has to delete an existing reservation.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationDeleteRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationDeleteRequest NewInstance(ReservationId reservationId)
		{
			ReservationDeleteRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				ReservationDeleteRequest>();
			request.SetReservationId(reservationId);
			return request;
		}

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
