using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	public interface ResourceManagerAdministrationProtocol : GetUserMappingsProtocol
	{
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshQueuesResponse RefreshQueues(RefreshQueuesRequest request);

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshNodesResponse RefreshNodes(RefreshNodesRequest request);

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshSuperUserGroupsConfigurationResponse RefreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest
			 request);

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshUserToGroupsMappingsResponse RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest
			 request);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshAdminAclsResponse RefreshAdminAcls(RefreshAdminAclsRequest request);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		RefreshServiceAclsResponse RefreshServiceAcls(RefreshServiceAclsRequest request);

		/// <summary>
		/// <p>The interface used by admin to update nodes' resources to the
		/// <code>ResourceManager</code> </p>.
		/// </summary>
		/// <remarks>
		/// <p>The interface used by admin to update nodes' resources to the
		/// <code>ResourceManager</code> </p>.
		/// <p>The admin client is required to provide details such as a map from
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeId"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceOption"/>
		/// required to update resources on
		/// a list of <code>RMNode</code> in <code>ResourceManager</code> etc.
		/// via the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.UpdateNodeResourceRequest
		/// 	"/>
		/// .</p>
		/// </remarks>
		/// <param name="request">request to update resource for a node in cluster.</param>
		/// <returns>(empty) response on accepting update.</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		[Idempotent]
		UpdateNodeResourceResponse UpdateNodeResource(UpdateNodeResourceRequest request);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		[Idempotent]
		AddToClusterNodeLabelsResponse AddToClusterNodeLabels(AddToClusterNodeLabelsRequest
			 request);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		[Idempotent]
		RemoveFromClusterNodeLabelsResponse RemoveFromClusterNodeLabels(RemoveFromClusterNodeLabelsRequest
			 request);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		[Idempotent]
		ReplaceLabelsOnNodeResponse ReplaceLabelsOnNode(ReplaceLabelsOnNodeRequest request
			);
	}
}
