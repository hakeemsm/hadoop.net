using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <see cref="ReservationDefinition"/>
	/// captures the set of resource and time
	/// constraints the user cares about regarding a reservation.
	/// </summary>
	/// <seealso cref="ResourceRequest"/>
	public abstract class ReservationDefinition
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationDefinition NewInstance(long arrival, long deadline, ReservationRequests
			 reservationRequests, string name)
		{
			ReservationDefinition rDefinition = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReservationDefinition>();
			rDefinition.SetArrival(arrival);
			rDefinition.SetDeadline(deadline);
			rDefinition.SetReservationRequests(reservationRequests);
			rDefinition.SetReservationName(name);
			return rDefinition;
		}

		/// <summary>
		/// Get the arrival time or the earliest time from which the resource(s) can be
		/// allocated.
		/// </summary>
		/// <remarks>
		/// Get the arrival time or the earliest time from which the resource(s) can be
		/// allocated. Time expressed as UTC.
		/// </remarks>
		/// <returns>the earliest valid time for this reservation</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetArrival();

		/// <summary>
		/// Set the arrival time or the earliest time from which the resource(s) can be
		/// allocated.
		/// </summary>
		/// <remarks>
		/// Set the arrival time or the earliest time from which the resource(s) can be
		/// allocated. Time expressed as UTC.
		/// </remarks>
		/// <param name="earliestStartTime">the earliest valid time for this reservation</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetArrival(long earliestStartTime);

		/// <summary>
		/// Get the deadline or the latest time by when the resource(s) must be
		/// allocated.
		/// </summary>
		/// <remarks>
		/// Get the deadline or the latest time by when the resource(s) must be
		/// allocated. Time expressed as UTC.
		/// </remarks>
		/// <returns>
		/// the deadline or the latest time by when the resource(s) must be
		/// allocated
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetDeadline();

		/// <summary>
		/// Set the deadline or the latest time by when the resource(s) must be
		/// allocated.
		/// </summary>
		/// <remarks>
		/// Set the deadline or the latest time by when the resource(s) must be
		/// allocated. Time expressed as UTC.
		/// </remarks>
		/// <param name="latestEndTime">
		/// the deadline or the latest time by when the
		/// resource(s) should be allocated
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetDeadline(long latestEndTime);

		/// <summary>
		/// Get the list of
		/// <see cref="ReservationRequests"/>
		/// representing the resources
		/// required by the application
		/// </summary>
		/// <returns>
		/// the list of
		/// <see cref="ReservationRequests"/>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationRequests GetReservationRequests();

		/// <summary>
		/// Set the list of
		/// <see cref="ReservationRequests"/>
		/// representing the resources
		/// required by the application
		/// </summary>
		/// <param name="reservationRequests">
		/// the list of
		/// <see cref="ReservationRequests"/>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetReservationRequests(ReservationRequests reservationRequests
			);

		/// <summary>Get the name for this reservation.</summary>
		/// <remarks>
		/// Get the name for this reservation. The name need-not be unique, and it is
		/// just a mnemonic for the user (akin to job names). Accepted reservations are
		/// uniquely identified by a system-generated ReservationId.
		/// </remarks>
		/// <returns>
		/// string representing the name of the corresponding reserved resource
		/// allocation in the scheduler
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract string GetReservationName();

		/// <summary>Set the name for this reservation.</summary>
		/// <remarks>
		/// Set the name for this reservation. The name need-not be unique, and it is
		/// just a mnemonic for the user (akin to job names). Accepted reservations are
		/// uniquely identified by a system-generated ReservationId.
		/// </remarks>
		/// <param name="name">
		/// representing the name of the corresponding reserved resource
		/// allocation in the scheduler
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract void SetReservationName(string name);
	}
}
