using Sharpen;

namespace org.apache.hadoop.tracing
{
	public class TraceAdminProtocolTranslatorPB : org.apache.hadoop.tracing.TraceAdminProtocol
		, org.apache.hadoop.ipc.ProtocolTranslator, java.io.Closeable
	{
		private readonly org.apache.hadoop.tracing.TraceAdminProtocolPB rpcProxy;

		public TraceAdminProtocolTranslatorPB(org.apache.hadoop.tracing.TraceAdminProtocolPB
			 rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.tracing.SpanReceiverInfo[] listSpanReceivers()
		{
			System.Collections.Generic.List<org.apache.hadoop.tracing.SpanReceiverInfo> infos
				 = new System.Collections.Generic.List<org.apache.hadoop.tracing.SpanReceiverInfo
				>(1);
			try
			{
				org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto req = ((org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto
					)org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto.newBuilder
					().build());
				org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto resp = rpcProxy
					.listSpanReceivers(null, req);
				foreach (org.apache.hadoop.tracing.TraceAdminPB.SpanReceiverListInfo info in resp
					.getDescriptionsList())
				{
					infos.add(new org.apache.hadoop.tracing.SpanReceiverInfo(info.getId(), info.getClassName
						()));
				}
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
			return Sharpen.Collections.ToArray(infos, new org.apache.hadoop.tracing.SpanReceiverInfo
				[infos.Count]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long addSpanReceiver(org.apache.hadoop.tracing.SpanReceiverInfo info
			)
		{
			try
			{
				org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto.Builder bld = 
					org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto.newBuilder();
				bld.setClassName(info.getClassName());
				foreach (org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair configPair in 
					info.configPairs)
				{
					org.apache.hadoop.tracing.TraceAdminPB.ConfigPair tuple = ((org.apache.hadoop.tracing.TraceAdminPB.ConfigPair
						)org.apache.hadoop.tracing.TraceAdminPB.ConfigPair.newBuilder().setKey(configPair
						.getKey()).setValue(configPair.getValue()).build());
					bld.addConfig(tuple);
				}
				org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto resp = rpcProxy
					.addSpanReceiver(null, ((org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto
					)bld.build()));
				return resp.getId();
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void removeSpanReceiver(long spanReceiverId)
		{
			try
			{
				org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto req = ((org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto
					)org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto.newBuilder
					().setId(spanReceiverId).build());
				rpcProxy.removeSpanReceiver(null, req);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		public virtual object getUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
