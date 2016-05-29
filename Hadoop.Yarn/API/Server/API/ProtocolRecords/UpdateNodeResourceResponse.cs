using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to Admin client on
	/// node resource change.</p>
	/// <p>Currently, this is empty.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Server.Api.ResourceManagerAdministrationProtocol.UpdateNodeResource(UpdateNodeResourceRequest)
	/// 	"/>
	public abstract class UpdateNodeResourceResponse
	{
		public static UpdateNodeResourceResponse NewInstance()
		{
			UpdateNodeResourceResponse response = Records.NewRecord<UpdateNodeResourceResponse
				>();
			return response;
		}
	}
}
