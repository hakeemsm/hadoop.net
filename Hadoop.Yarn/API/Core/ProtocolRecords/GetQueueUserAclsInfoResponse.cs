using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to clients
	/// seeking queue acls for the user.</p>
	/// <p>The response contains a list of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueUserACLInfo"/>
	/// which
	/// provides information about
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueACL"/>
	/// per queue.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueACL"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueUserACLInfo"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueUserAcls(GetQueueUserAclsInfoRequest)
	/// 	"/>
	public abstract class GetQueueUserAclsInfoResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetQueueUserAclsInfoResponse NewInstance(IList<QueueUserACLInfo> queueUserAclsList
			)
		{
			GetQueueUserAclsInfoResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetQueueUserAclsInfoResponse>();
			response.SetUserAclsInfoList(queueUserAclsList);
			return response;
		}

		/// <summary>Get the <code>QueueUserACLInfo</code> per queue for the user.</summary>
		/// <returns><code>QueueUserACLInfo</code> per queue for the user</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<QueueUserACLInfo> GetUserAclsInfoList();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUserAclsInfoList(IList<QueueUserACLInfo> queueUserAclsList
			);
	}
}
