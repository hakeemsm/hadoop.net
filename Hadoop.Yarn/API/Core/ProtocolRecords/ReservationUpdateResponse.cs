using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <see cref="ReservationUpdateResponse"/>
	/// contains the answer of the admission
	/// control system in the
	/// <c>ResourceManager</c>
	/// to a reservation update
	/// operation. Currently response is empty if the operation was successful, if
	/// not an exception reporting reason for a failure.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	public abstract class ReservationUpdateResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ReservationUpdateResponse NewInstance()
		{
			ReservationUpdateResponse response = Records.NewRecord<ReservationUpdateResponse>
				();
			return response;
		}
	}
}
