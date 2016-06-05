using System;
using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.Tracing
{
	public class TraceAdminProtocolServerSideTranslatorPB : TraceAdminProtocolPB, IDisposable
	{
		private readonly TraceAdminProtocol server;

		public TraceAdminProtocolServerSideTranslatorPB(TraceAdminProtocol server)
		{
			this.server = server;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			RPC.StopProxy(server);
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual TraceAdminPB.ListSpanReceiversResponseProto ListSpanReceivers(RpcController
			 controller, TraceAdminPB.ListSpanReceiversRequestProto req)
		{
			try
			{
				SpanReceiverInfo[] descs = server.ListSpanReceivers();
				TraceAdminPB.ListSpanReceiversResponseProto.Builder bld = TraceAdminPB.ListSpanReceiversResponseProto
					.NewBuilder();
				for (int i = 0; i < descs.Length; ++i)
				{
					bld.AddDescriptions(((TraceAdminPB.SpanReceiverListInfo)TraceAdminPB.SpanReceiverListInfo
						.NewBuilder().SetId(descs[i].GetId()).SetClassName(descs[i].GetClassName()).Build
						()));
				}
				return ((TraceAdminPB.ListSpanReceiversResponseProto)bld.Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual TraceAdminPB.AddSpanReceiverResponseProto AddSpanReceiver(RpcController
			 controller, TraceAdminPB.AddSpanReceiverRequestProto req)
		{
			try
			{
				SpanReceiverInfoBuilder factory = new SpanReceiverInfoBuilder(req.GetClassName());
				foreach (TraceAdminPB.ConfigPair config in req.GetConfigList())
				{
					factory.AddConfigurationPair(config.GetKey(), config.GetValue());
				}
				long id = server.AddSpanReceiver(factory.Build());
				return ((TraceAdminPB.AddSpanReceiverResponseProto)TraceAdminPB.AddSpanReceiverResponseProto
					.NewBuilder().SetId(id).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual TraceAdminPB.RemoveSpanReceiverResponseProto RemoveSpanReceiver(RpcController
			 controller, TraceAdminPB.RemoveSpanReceiverRequestProto req)
		{
			try
			{
				server.RemoveSpanReceiver(req.GetId());
				return TraceAdminPB.RemoveSpanReceiverResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string protocol, long clientVersion)
		{
			return TraceAdminProtocol.versionID;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			if (!protocol.Equals(RPC.GetProtocolName(typeof(TraceAdminProtocolPB))))
			{
				throw new IOException("Serverside implements " + RPC.GetProtocolName(typeof(TraceAdminProtocolPB
					)) + ". The following requested protocol is unknown: " + protocol);
			}
			return ProtocolSignature.GetProtocolSignature(clientMethodsHash, RPC.GetProtocolVersion
				(typeof(TraceAdminProtocolPB)), typeof(TraceAdminProtocolPB));
		}
	}
}
