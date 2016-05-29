using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions
{
	/// <summary>This exception is thrown if the request made is not syntactically valid.
	/// 	</summary>
	[System.Serializable]
	public class ContractValidationException : PlanningException
	{
		private const long serialVersionUID = 1L;

		public ContractValidationException(string message)
			: base(message)
		{
		}
	}
}
