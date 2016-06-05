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
	public class ResourceTrackerPBServiceImpl : ResourceTrackerPB
	{
		private ResourceTracker real;

		public ResourceTrackerPBServiceImpl(ResourceTracker impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto RegisterNodeManager
			(RpcController controller, YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto
			 proto)
		{
			RegisterNodeManagerRequestPBImpl request = new RegisterNodeManagerRequestPBImpl(proto
				);
			try
			{
				RegisterNodeManagerResponse response = real.RegisterNodeManager(request);
				return ((RegisterNodeManagerResponsePBImpl)response).GetProto();
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
		public virtual YarnServerCommonServiceProtos.NodeHeartbeatResponseProto NodeHeartbeat
			(RpcController controller, YarnServerCommonServiceProtos.NodeHeartbeatRequestProto
			 proto)
		{
			NodeHeartbeatRequestPBImpl request = new NodeHeartbeatRequestPBImpl(proto);
			try
			{
				NodeHeartbeatResponse response = real.NodeHeartbeat(request);
				return ((NodeHeartbeatResponsePBImpl)response).GetProto();
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
