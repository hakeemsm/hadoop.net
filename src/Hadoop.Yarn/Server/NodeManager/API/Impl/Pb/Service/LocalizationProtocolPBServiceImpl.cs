using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Impl.PB.Service
{
	public class LocalizationProtocolPBServiceImpl : LocalizationProtocolPB
	{
		private LocalizationProtocol real;

		public LocalizationProtocolPBServiceImpl(LocalizationProtocol impl)
		{
			this.real = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
			 Heartbeat(RpcController controller, YarnServerNodemanagerServiceProtos.LocalizerStatusProto
			 proto)
		{
			LocalizerStatusPBImpl request = new LocalizerStatusPBImpl(proto);
			try
			{
				LocalizerHeartbeatResponse response = real.Heartbeat(request);
				return ((LocalizerHeartbeatResponsePBImpl)response).GetProto();
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
