using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions
{
	/// <summary>
	/// This exception is thrown if the user quota is exceed while accepting or
	/// updating a reservation.
	/// </summary>
	[System.Serializable]
	public class PlanningQuotaException : PlanningException
	{
		private const long serialVersionUID = 8206629288380246166L;

		public PlanningQuotaException(string message)
			: base(message)
		{
		}

		public PlanningQuotaException(Exception cause)
			: base(cause)
		{
		}

		public PlanningQuotaException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
