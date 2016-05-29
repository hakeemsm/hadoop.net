using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions
{
	/// <summary>
	/// Exception thrown by the admission control subsystem when there is a problem
	/// in trying to find an allocation for a user
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionRequest
	/// 	"/>
	/// .
	/// </summary>
	[System.Serializable]
	public class PlanningException : Exception
	{
		private const long serialVersionUID = -684069387367879218L;

		public PlanningException(string message)
			: base(message)
		{
		}

		public PlanningException(Exception cause)
			: base(cause)
		{
		}

		public PlanningException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
