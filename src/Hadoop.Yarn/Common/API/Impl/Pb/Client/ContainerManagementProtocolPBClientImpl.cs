using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Impl.PB.Client
{
	public class ContainerManagementProtocolPBClientImpl : ContainerManagementProtocol
		, IDisposable
	{
		internal const string NmCommandTimeout = YarnConfiguration.YarnPrefix + "rpc.nm-command-timeout";

		/// <summary>Maximum of 1 minute timeout for a Node to react to the command</summary>
		internal const int DefaultCommandTimeout = 60000;

		private ContainerManagementProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ContainerManagementProtocolPBClientImpl(long clientVersion, IPEndPoint addr
			, Configuration conf)
		{
			// Not a documented config. Only used for tests
			RPC.SetProtocolEngine(conf, typeof(ContainerManagementProtocolPB), typeof(ProtobufRpcEngine
				));
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			int expireIntvl = conf.GetInt(NmCommandTimeout, DefaultCommandTimeout);
			proxy = (ContainerManagementProtocolPB)RPC.GetProxy<ContainerManagementProtocolPB
				>(clientVersion, addr, ugi, conf, NetUtils.GetDefaultSocketFactory(conf), expireIntvl
				);
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
		public virtual StartContainersResponse StartContainers(StartContainersRequest requests
			)
		{
			YarnServiceProtos.StartContainersRequestProto requestProto = ((StartContainersRequestPBImpl
				)requests).GetProto();
			try
			{
				return new StartContainersResponsePBImpl(proxy.StartContainers(null, requestProto
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
		public virtual StopContainersResponse StopContainers(StopContainersRequest requests
			)
		{
			YarnServiceProtos.StopContainersRequestProto requestProto = ((StopContainersRequestPBImpl
				)requests).GetProto();
			try
			{
				return new StopContainersResponsePBImpl(proxy.StopContainers(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
			 request)
		{
			YarnServiceProtos.GetContainerStatusesRequestProto requestProto = ((GetContainerStatusesRequestPBImpl
				)request).GetProto();
			try
			{
				return new GetContainerStatusesResponsePBImpl(proxy.GetContainerStatuses(null, requestProto
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
