using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Impl.PB.Client
{
	public class ResourceManagerAdministrationProtocolPBClientImpl : ResourceManagerAdministrationProtocol
		, IDisposable
	{
		private ResourceManagerAdministrationProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ResourceManagerAdministrationProtocolPBClientImpl(long clientVersion, IPEndPoint
			 addr, Configuration conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ResourceManagerAdministrationProtocolPB), typeof(
				ProtobufRpcEngine));
			proxy = (ResourceManagerAdministrationProtocolPB)RPC.GetProxy<ResourceManagerAdministrationProtocolPB
				>(clientVersion, addr, conf);
		}

		public virtual void Close()
		{
			if (this.proxy != null)
			{
				RPC.StopProxy(this.proxy);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshQueuesResponse RefreshQueues(RefreshQueuesRequest request)
		{
			YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto requestProto = (
				(RefreshQueuesRequestPBImpl)request).GetProto();
			try
			{
				return new RefreshQueuesResponsePBImpl(proxy.RefreshQueues(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshNodesResponse RefreshNodes(RefreshNodesRequest request)
		{
			YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto requestProto = ((
				RefreshNodesRequestPBImpl)request).GetProto();
			try
			{
				return new RefreshNodesResponsePBImpl(proxy.RefreshNodes(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshSuperUserGroupsConfigurationResponse RefreshSuperUserGroupsConfiguration
			(RefreshSuperUserGroupsConfigurationRequest request)
		{
			YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationRequestProto
				 requestProto = ((RefreshSuperUserGroupsConfigurationRequestPBImpl)request).GetProto
				();
			try
			{
				return new RefreshSuperUserGroupsConfigurationResponsePBImpl(proxy.RefreshSuperUserGroupsConfiguration
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshUserToGroupsMappingsResponse RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto requestProto
				 = ((RefreshUserToGroupsMappingsRequestPBImpl)request).GetProto();
			try
			{
				return new RefreshUserToGroupsMappingsResponsePBImpl(proxy.RefreshUserToGroupsMappings
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshAdminAclsResponse RefreshAdminAcls(RefreshAdminAclsRequest 
			request)
		{
			YarnServerResourceManagerServiceProtos.RefreshAdminAclsRequestProto requestProto = 
				((RefreshAdminAclsRequestPBImpl)request).GetProto();
			try
			{
				return new RefreshAdminAclsResponsePBImpl(proxy.RefreshAdminAcls(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RefreshServiceAclsResponse RefreshServiceAcls(RefreshServiceAclsRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto requestProto
				 = ((RefreshServiceAclsRequestPBImpl)request).GetProto();
			try
			{
				return new RefreshServiceAclsResponsePBImpl(proxy.RefreshServiceAcls(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string[] GetGroupsForUser(string user)
		{
			YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto requestProto = 
				((YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto)YarnServerResourceManagerServiceProtos.GetGroupsForUserRequestProto
				.NewBuilder().SetUser(user).Build());
			try
			{
				YarnServerResourceManagerServiceProtos.GetGroupsForUserResponseProto responseProto
					 = proxy.GetGroupsForUser(null, requestProto);
				return (string[])Sharpen.Collections.ToArray(responseProto.GetGroupsList(), new string
					[responseProto.GetGroupsCount()]);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual UpdateNodeResourceResponse UpdateNodeResource(UpdateNodeResourceRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto requestProto
				 = ((UpdateNodeResourceRequestPBImpl)request).GetProto();
			try
			{
				return new UpdateNodeResourceResponsePBImpl(proxy.UpdateNodeResource(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual AddToClusterNodeLabelsResponse AddToClusterNodeLabels(AddToClusterNodeLabelsRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto requestProto
				 = ((AddToClusterNodeLabelsRequestPBImpl)request).GetProto();
			try
			{
				return new AddToClusterNodeLabelsResponsePBImpl(proxy.AddToClusterNodeLabels(null
					, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoveFromClusterNodeLabelsResponse RemoveFromClusterNodeLabels(RemoveFromClusterNodeLabelsRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto requestProto
				 = ((RemoveFromClusterNodeLabelsRequestPBImpl)request).GetProto();
			try
			{
				return new RemoveFromClusterNodeLabelsResponsePBImpl(proxy.RemoveFromClusterNodeLabels
					(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReplaceLabelsOnNodeResponse ReplaceLabelsOnNode(ReplaceLabelsOnNodeRequest
			 request)
		{
			YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto requestProto
				 = ((ReplaceLabelsOnNodeRequestPBImpl)request).GetProto();
			try
			{
				return new ReplaceLabelsOnNodeResponsePBImpl(proxy.ReplaceLabelsOnNodes(null, requestProto
					));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
