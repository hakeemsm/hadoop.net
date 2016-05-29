using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Impl.PB.Service
{
	public class ResourceManagerAdministrationProtocolPBServiceImpl : ResourceManagerAdministrationProtocolPB
	{
		private ResourceManagerAdministrationProtocol real;

		public ResourceManagerAdministrationProtocolPBServiceImpl(ResourceManagerAdministrationProtocol
			 impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto 
			RefreshQueues(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto
			 proto)
		{
			RefreshQueuesRequestPBImpl request = new RefreshQueuesRequestPBImpl(proto);
			try
			{
				RefreshQueuesResponse response = real.RefreshQueues(request);
				return ((RefreshQueuesResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshAdminAclsResponseProto
			 RefreshAdminAcls(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto
			 proto)
		{
			RefreshAdminAclsRequestPBImpl request = new RefreshAdminAclsRequestPBImpl(proto);
			try
			{
				RefreshAdminAclsResponse response = real.RefreshAdminAcls(request);
				return ((RefreshAdminAclsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshNodesResponseProto RefreshNodes
			(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto
			 proto)
		{
			RefreshNodesRequestPBImpl request = new RefreshNodesRequestPBImpl(proto);
			try
			{
				RefreshNodesResponse response = real.RefreshNodes(request);
				return ((RefreshNodesResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 RefreshSuperUserGroupsConfiguration(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto
			 proto)
		{
			RefreshSuperUserGroupsConfigurationRequestPBImpl request = new RefreshSuperUserGroupsConfigurationRequestPBImpl
				(proto);
			try
			{
				RefreshSuperUserGroupsConfigurationResponse response = real.RefreshSuperUserGroupsConfiguration
					(request);
				return ((RefreshSuperUserGroupsConfigurationResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsResponseProto
			 RefreshUserToGroupsMappings(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto
			 proto)
		{
			RefreshUserToGroupsMappingsRequestPBImpl request = new RefreshUserToGroupsMappingsRequestPBImpl
				(proto);
			try
			{
				RefreshUserToGroupsMappingsResponse response = real.RefreshUserToGroupsMappings(request
					);
				return ((RefreshUserToGroupsMappingsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RefreshServiceAclsResponseProto
			 RefreshServiceAcls(RpcController controller, YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto
			 proto)
		{
			RefreshServiceAclsRequestPBImpl request = new RefreshServiceAclsRequestPBImpl(proto
				);
			try
			{
				RefreshServiceAclsResponse response = real.RefreshServiceAcls(request);
				return ((RefreshServiceAclsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto
			 GetGroupsForUser(RpcController controller, YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto
			 request)
		{
			string user = request.GetUser();
			try
			{
				string[] groups = real.GetGroupsForUser(user);
				YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto.Builder responseBuilder
					 = YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto.NewBuilder
					();
				foreach (string group in groups)
				{
					responseBuilder.AddGroups(group);
				}
				return ((YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto)responseBuilder
					.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto
			 UpdateNodeResource(RpcController controller, YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto
			 proto)
		{
			UpdateNodeResourceRequestPBImpl request = new UpdateNodeResourceRequestPBImpl(proto
				);
			try
			{
				UpdateNodeResourceResponse response = real.UpdateNodeResource(request);
				return ((UpdateNodeResourceResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
			 AddToClusterNodeLabels(RpcController controller, YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
			 proto)
		{
			AddToClusterNodeLabelsRequestPBImpl request = new AddToClusterNodeLabelsRequestPBImpl
				(proto);
			try
			{
				AddToClusterNodeLabelsResponse response = real.AddToClusterNodeLabels(request);
				return ((AddToClusterNodeLabelsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto
			 RemoveFromClusterNodeLabels(RpcController controller, YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
			 proto)
		{
			RemoveFromClusterNodeLabelsRequestPBImpl request = new RemoveFromClusterNodeLabelsRequestPBImpl
				(proto);
			try
			{
				RemoveFromClusterNodeLabelsResponse response = real.RemoveFromClusterNodeLabels(request
					);
				return ((RemoveFromClusterNodeLabelsResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto
			 ReplaceLabelsOnNodes(RpcController controller, YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
			 proto)
		{
			ReplaceLabelsOnNodeRequestPBImpl request = new ReplaceLabelsOnNodeRequestPBImpl(proto
				);
			try
			{
				ReplaceLabelsOnNodeResponse response = real.ReplaceLabelsOnNode(request);
				return ((ReplaceLabelsOnNodeResponsePBImpl)response).GetProto();
			}
			catch (YarnException e)
			{
				throw new ServiceException(e);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
