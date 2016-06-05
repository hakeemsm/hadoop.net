using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationDeleteResponse"/>
	/// contains the answer of the admission
	/// control system in the
	/// <c>ResourceManager</c>
	/// to a reservation delete
	/// operation. Currently response is empty if the operation was successful, if
	/// not an exception reporting reason for a failure.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationDeleteResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ReservationDeleteResponse NewInstance()
		{
			ReservationDeleteResponse response = Records.NewRecord<ReservationDeleteResponse>
				();
			return response;
		}
	}
}
