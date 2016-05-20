using Sharpen;

namespace org.apache.hadoop.tracing
{
	public class TraceAdminProtocolServerSideTranslatorPB : org.apache.hadoop.tracing.TraceAdminProtocolPB
		, java.io.Closeable
	{
		private readonly org.apache.hadoop.tracing.TraceAdminProtocol server;

		public TraceAdminProtocolServerSideTranslatorPB(org.apache.hadoop.tracing.TraceAdminProtocol
			 server)
		{
			this.server = server;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(server);
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto
			 listSpanReceivers(com.google.protobuf.RpcController controller, org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto
			 req)
		{
			try
			{
				org.apache.hadoop.tracing.SpanReceiverInfo[] descs = server.listSpanReceivers();
				org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto.Builder bld
					 = org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto.newBuilder
					();
				for (int i = 0; i < descs.Length; ++i)
				{
					bld.addDescriptions(((org.apache.hadoop.tracing.TraceAdminPB.SpanReceiverListInfo
						)org.apache.hadoop.tracing.TraceAdminPB.SpanReceiverListInfo.newBuilder().setId(
						descs[i].getId()).setClassName(descs[i].getClassName()).build()));
				}
				return ((org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto)bld
					.build());
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto
			 addSpanReceiver(com.google.protobuf.RpcController controller, org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto
			 req)
		{
			try
			{
				org.apache.hadoop.tracing.SpanReceiverInfoBuilder factory = new org.apache.hadoop.tracing.SpanReceiverInfoBuilder
					(req.getClassName());
				foreach (org.apache.hadoop.tracing.TraceAdminPB.ConfigPair config in req.getConfigList
					())
				{
					factory.addConfigurationPair(config.getKey(), config.getValue());
				}
				long id = server.addSpanReceiver(factory.build());
				return ((org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto)org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto
					.newBuilder().setId(id).build());
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverResponseProto
			 removeSpanReceiver(com.google.protobuf.RpcController controller, org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto
			 req)
		{
			try
			{
				server.removeSpanReceiver(req.getId());
				return org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverResponseProto.getDefaultInstance
					();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getProtocolVersion(string protocol, long clientVersion)
		{
			return org.apache.hadoop.tracing.TraceAdminProtocol.versionID;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
			 protocol, long clientVersion, int clientMethodsHash)
		{
			if (!protocol.Equals(org.apache.hadoop.ipc.RPC.getProtocolName(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.tracing.TraceAdminProtocolPB)))))
			{
				throw new System.IO.IOException("Serverside implements " + org.apache.hadoop.ipc.RPC
					.getProtocolName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.tracing.TraceAdminProtocolPB
					))) + ". The following requested protocol is unknown: " + protocol);
			}
			return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
				, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.tracing.TraceAdminProtocolPB))), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.tracing.TraceAdminProtocolPB)));
		}
	}
}
