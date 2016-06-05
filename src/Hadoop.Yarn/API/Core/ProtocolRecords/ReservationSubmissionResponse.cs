using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationSubmissionResponse"/>
	/// contains the answer of the admission
	/// control system in the
	/// <c>ResourceManager</c>
	/// to a reservation create
	/// operation. Response contains a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
	/// if the operation was
	/// successful, if not an exception reporting reason for a failure.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationSubmissionResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ReservationSubmissionResponse NewInstance(ReservationId reservationId
			)
		{
			ReservationSubmissionResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReservationSubmissionResponse>();
			response.SetReservationId(reservationId);
			return response;
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
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetReservationId(ReservationId reservationId);
	}
}
