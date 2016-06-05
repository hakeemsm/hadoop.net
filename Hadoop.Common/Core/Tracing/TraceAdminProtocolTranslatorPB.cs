using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.Tracing
{
	public class TraceAdminProtocolTranslatorPB : TraceAdminProtocol, ProtocolTranslator
		, IDisposable
	{
		private readonly TraceAdminProtocolPB rpcProxy;

		public TraceAdminProtocolTranslatorPB(TraceAdminProtocolPB rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SpanReceiverInfo[] ListSpanReceivers()
		{
			AList<SpanReceiverInfo> infos = new AList<SpanReceiverInfo>(1);
			try
			{
				TraceAdminPB.ListSpanReceiversRequestProto req = ((TraceAdminPB.ListSpanReceiversRequestProto
					)TraceAdminPB.ListSpanReceiversRequestProto.NewBuilder().Build());
				TraceAdminPB.ListSpanReceiversResponseProto resp = rpcProxy.ListSpanReceivers(null
					, req);
				foreach (TraceAdminPB.SpanReceiverListInfo info in resp.GetDescriptionsList())
				{
					infos.AddItem(new SpanReceiverInfo(info.GetId(), info.GetClassName()));
				}
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			return Collections.ToArray(infos, new SpanReceiverInfo[infos.Count]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddSpanReceiver(SpanReceiverInfo info)
		{
			try
			{
				TraceAdminPB.AddSpanReceiverRequestProto.Builder bld = TraceAdminPB.AddSpanReceiverRequestProto
					.NewBuilder();
				bld.SetClassName(info.GetClassName());
				foreach (SpanReceiverInfo.ConfigurationPair configPair in info.configPairs)
				{
					TraceAdminPB.ConfigPair tuple = ((TraceAdminPB.ConfigPair)TraceAdminPB.ConfigPair
						.NewBuilder().SetKey(configPair.GetKey()).SetValue(configPair.GetValue()).Build(
						));
					bld.AddConfig(tuple);
				}
				TraceAdminPB.AddSpanReceiverResponseProto resp = rpcProxy.AddSpanReceiver(null, (
					(TraceAdminPB.AddSpanReceiverRequestProto)bld.Build()));
				return resp.GetId();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveSpanReceiver(long spanReceiverId)
		{
			try
			{
				TraceAdminPB.RemoveSpanReceiverRequestProto req = ((TraceAdminPB.RemoveSpanReceiverRequestProto
					)TraceAdminPB.RemoveSpanReceiverRequestProto.NewBuilder().SetId(spanReceiverId).
					Build());
				rpcProxy.RemoveSpanReceiver(null, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
