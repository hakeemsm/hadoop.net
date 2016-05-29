using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions
{
	/// <summary>
	/// Exception thrown when an update to an existing reservation is performed
	/// by a user that is not the reservation owner.
	/// </summary>
	[System.Serializable]
	public class MismatchedUserException : PlanningException
	{
		private const long serialVersionUID = 8313222590561668413L;

		public MismatchedUserException(string message)
			: base(message)
		{
		}

		public MismatchedUserException(Exception cause)
			: base(cause)
		{
		}

		public MismatchedUserException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
