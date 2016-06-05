using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Service
{
	public class ContainerManagementProtocolPBServiceImpl : ContainerManagementProtocolPB
	{
		private ContainerManagementProtocol real;

		public ContainerManagementProtocolPBServiceImpl(ContainerManagementProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServiceProtos.StartContainersResponseProto StartContainers(RpcController
			 arg0, YarnServiceProtos.StartContainersRequestProto proto)
		{
			StartContainersRequestPBImpl request = new StartContainersRequestPBImpl(proto);
			try
			{
				StartContainersResponse response = real.StartContainers(request);
				return ((StartContainersResponsePBImpl)response).GetProto();
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
		public virtual YarnServiceProtos.StopContainersResponseProto StopContainers(RpcController
			 arg0, YarnServiceProtos.StopContainersRequestProto proto)
		{
			StopContainersRequestPBImpl request = new StopContainersRequestPBImpl(proto);
			try
			{
				StopContainersResponse response = real.StopContainers(request);
				return ((StopContainersResponsePBImpl)response).GetProto();
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
		public virtual YarnServiceProtos.GetContainerStatusesResponseProto GetContainerStatuses
			(RpcController arg0, YarnServiceProtos.GetContainerStatusesRequestProto proto)
		{
			GetContainerStatusesRequestPBImpl request = new GetContainerStatusesRequestPBImpl
				(proto);
			try
			{
				GetContainerStatusesResponse response = real.GetContainerStatuses(request);
				return ((GetContainerStatusesResponsePBImpl)response).GetProto();
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
