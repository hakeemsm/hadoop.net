using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to a client on
	/// application submission.</p>
	/// <p>Currently, this is empty.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.SubmitApplication(SubmitApplicationRequest)
	/// 	"/>
	public abstract class SubmitApplicationResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static SubmitApplicationResponse NewInstance()
		{
			SubmitApplicationResponse response = Records.NewRecord<SubmitApplicationResponse>
				();
			return response;
		}
	}
}
