using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <see cref="ReservationRequests"/>
	/// captures the set of resource and constraints the
	/// user cares about regarding a reservation.
	/// </summary>
	/// <seealso cref="ReservationRequest"/>
	public abstract class ReservationRequests
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ReservationRequests NewInstance(IList<ReservationRequest> reservationResources
			, ReservationRequestInterpreter type)
		{
			ReservationRequests reservationRequests = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReservationRequests>();
			reservationRequests.SetReservationResources(reservationResources);
			reservationRequests.SetInterpreter(type);
			return reservationRequests;
		}

		/// <summary>
		/// Get the list of
		/// <see cref="ReservationRequest"/>
		/// representing the resources
		/// required by the application
		/// </summary>
		/// <returns>
		/// the list of
		/// <see cref="ReservationRequest"/>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IList<ReservationRequest> GetReservationResources();

		/// <summary>
		/// Set the list of
		/// <see cref="ReservationRequest"/>
		/// representing the resources
		/// required by the application
		/// </summary>
		/// <param name="reservationResources">
		/// the list of
		/// <see cref="ReservationRequest"/>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetReservationResources(IList<ReservationRequest> reservationResources
			);

		/// <summary>
		/// Get the
		/// <see cref="ReservationRequestInterpreter"/>
		/// , representing how the list of
		/// resources should be allocated, this captures temporal ordering and other
		/// constraints.
		/// </summary>
		/// <returns>
		/// the list of
		/// <see cref="ReservationRequestInterpreter"/>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationRequestInterpreter GetInterpreter();

		/// <summary>
		/// Set the
		/// <see cref="ReservationRequestInterpreter"/>
		/// , representing how the list of
		/// resources should be allocated, this captures temporal ordering and other
		/// constraints.
		/// </summary>
		/// <param name="interpreter">
		/// the
		/// <see cref="ReservationRequestInterpreter"/>
		/// for this
		/// reservation
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetInterpreter(ReservationRequestInterpreter interpreter);
	}
}
