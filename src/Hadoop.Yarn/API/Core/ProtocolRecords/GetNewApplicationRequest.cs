using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by clients to get a new
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// for
	/// submitting an application.</p>
	/// <p>Currently, this is empty.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetNewApplication(GetNewApplicationRequest)
	/// 	"/>
	public abstract class GetNewApplicationRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetNewApplicationRequest NewInstance()
		{
			GetNewApplicationRequest request = Records.NewRecord<GetNewApplicationRequest>();
			return request;
		}
	}
}
