using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to the client moving
	/// a submitted application to a different queue.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to the client moving
	/// a submitted application to a different queue.
	/// </p>
	/// <p>
	/// A response without exception means that the move has completed successfully.
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest)
	/// 	"/>
	public class MoveApplicationAcrossQueuesResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual MoveApplicationAcrossQueuesResponse NewInstance()
		{
			MoveApplicationAcrossQueuesResponse response = Records.NewRecord<MoveApplicationAcrossQueuesResponse
				>();
			return response;
		}
	}
}
