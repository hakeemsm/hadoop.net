using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by clients to the <code>ResourceManager</code> to
	/// get queue acls for the <em>current user</em>.</p>
	/// <p>Currently, this is empty.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueUserAcls(GetQueueUserAclsInfoRequest)
	/// 	"/>
	public abstract class GetQueueUserAclsInfoRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetQueueUserAclsInfoRequest NewInstance()
		{
			GetQueueUserAclsInfoRequest request = Records.NewRecord<GetQueueUserAclsInfoRequest
				>();
			return request;
		}
	}
}
