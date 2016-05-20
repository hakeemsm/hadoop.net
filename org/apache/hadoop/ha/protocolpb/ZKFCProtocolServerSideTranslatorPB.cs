using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	public class ZKFCProtocolServerSideTranslatorPB : org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB
	{
		private readonly org.apache.hadoop.ha.ZKFCProtocol server;

		public ZKFCProtocolServerSideTranslatorPB(org.apache.hadoop.ha.ZKFCProtocol server
			)
		{
			this.server = server;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveResponseProto
			 cedeActive(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto
			 request)
		{
			try
			{
				server.cedeActive(request.getMillisToCede());
				return org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveResponseProto.getDefaultInstance
					();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverResponseProto
			 gracefulFailover(com.google.protobuf.RpcController controller, org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverRequestProto
			 request)
		{
			try
			{
				server.gracefulFailover();
				return org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverResponseProto
					.getDefaultInstance();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getProtocolVersion(string protocol, long clientVersion)
		{
			return org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
			 protocol, long clientVersion, int clientMethodsHash)
		{
			if (!protocol.Equals(org.apache.hadoop.ipc.RPC.getProtocolName(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB)))))
			{
				throw new System.IO.IOException("Serverside implements " + org.apache.hadoop.ipc.RPC
					.getProtocolName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB
					))) + ". The following requested protocol is unknown: " + protocol);
			}
			return org.apache.hadoop.ipc.ProtocolSignature.getProtocolSignature(clientMethodsHash
				, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB))), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB)));
		}
	}
}
