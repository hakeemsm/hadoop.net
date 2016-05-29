using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationSubmissionRequest"/>
	/// captures the set of requirements the
	/// user has to create a reservation.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationSubmissionRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationSubmissionRequest NewInstance(ReservationDefinition reservationDefinition
			, string queueName)
		{
			ReservationSubmissionRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReservationSubmissionRequest>();
			request.SetReservationDefinition(reservationDefinition);
			request.SetQueue(queueName);
			return request;
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// representing the user constraints for
		/// this reservation
		/// </summary>
		/// <returns>the reservation definition representing user constraints</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationDefinition GetReservationDefinition();

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// representing the user constraints for
		/// this reservation
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
		/// Get the name of the
		/// <c>Plan</c>
		/// that corresponds to the name of the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// in the scheduler to which the reservation will be
		/// submitted to.
		/// </summary>
		/// <returns>
		/// the name of the
		/// <c>Plan</c>
		/// that corresponds to the name of the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// in the scheduler to which the reservation will be
		/// submitted to
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetQueue();

		/// <summary>
		/// Set the name of the
		/// <c>Plan</c>
		/// that corresponds to the name of the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// in the scheduler to which the reservation will be
		/// submitted to
		/// </summary>
		/// <param name="queueName">
		/// the name of the parent
		/// <c>Plan</c>
		/// that corresponds to
		/// the name of the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// in the scheduler to which the
		/// reservation will be submitted to
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetQueue(string queueName);
	}
}
