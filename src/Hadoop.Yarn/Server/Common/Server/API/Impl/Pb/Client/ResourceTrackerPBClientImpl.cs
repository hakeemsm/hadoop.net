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
	public class ResourceTrackerPBClientImpl : ResourceTracker, IDisposable
	{
		private ResourceTrackerPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public ResourceTrackerPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(ResourceTrackerPB), typeof(ProtobufRpcEngine));
			proxy = (ResourceTrackerPB)RPC.GetProxy<ResourceTrackerPB>(clientVersion, addr, conf
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
		public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
			 request)
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto requestProto = ((RegisterNodeManagerRequestPBImpl
				)request).GetProto();
			try
			{
				return new RegisterNodeManagerResponsePBImpl(proxy.RegisterNodeManager(null, requestProto
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
		public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
		{
			YarnServerCommonServiceProtos.NodeHeartbeatRequestProto requestProto = ((NodeHeartbeatRequestPBImpl
				)request).GetProto();
			try
			{
				return new NodeHeartbeatResponsePBImpl(proxy.NodeHeartbeat(null, requestProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
