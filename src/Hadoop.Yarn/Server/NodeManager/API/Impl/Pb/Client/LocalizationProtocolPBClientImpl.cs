using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Impl.PB.Client
{
	public class LocalizationProtocolPBClientImpl : LocalizationProtocol, IDisposable
	{
		private LocalizationProtocolPB proxy;

		/// <exception cref="System.IO.IOException"/>
		public LocalizationProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			RPC.SetProtocolEngine(conf, typeof(LocalizationProtocolPB), typeof(ProtobufRpcEngine
				));
			proxy = (LocalizationProtocolPB)RPC.GetProxy<LocalizationProtocolPB>(clientVersion
				, addr, conf);
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
		public virtual LocalizerHeartbeatResponse Heartbeat(LocalizerStatus status)
		{
			YarnServerNodemanagerServiceProtos.LocalizerStatusProto statusProto = ((LocalizerStatusPBImpl
				)status).GetProto();
			try
			{
				return new LocalizerHeartbeatResponsePBImpl(proxy.Heartbeat(null, statusProto));
			}
			catch (ServiceException e)
			{
				RPCUtil.UnwrapAndThrowException(e);
				return null;
			}
		}
	}
}
