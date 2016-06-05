using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions
{
	/// <summary>
	/// This exception indicate that the reservation that has been attempted, would
	/// exceed the physical resources available in the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Plan"/>
	/// at the moment.
	/// </summary>
	[System.Serializable]
	public class ResourceOverCommitException : PlanningException
	{
		private const long serialVersionUID = 7070699407526521032L;

		public ResourceOverCommitException(string message)
			: base(message)
		{
		}

		public ResourceOverCommitException(Exception cause)
			: base(cause)
		{
		}

		public ResourceOverCommitException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
