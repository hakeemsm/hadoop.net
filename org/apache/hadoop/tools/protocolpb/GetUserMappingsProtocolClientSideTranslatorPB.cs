using Sharpen;

namespace org.apache.hadoop.tools.protocolPB
{
	public class GetUserMappingsProtocolClientSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInterface
		, org.apache.hadoop.tools.GetUserMappingsProtocol, java.io.Closeable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB rpcProxy;

		public GetUserMappingsProtocolClientSideTranslatorPB(org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB
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
		public virtual string[] getGroupsForUser(string user)
		{
			org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
				 request = ((org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
				)org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
				.newBuilder().setUser(user).build());
			org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto
				 resp;
			try
			{
				resp = rpcProxy.getGroupsForUser(NULL_CONTROLLER, request);
			}
			catch (com.google.protobuf.ServiceException se)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(se);
			}
			return Sharpen.Collections.ToArray(resp.getGroupsList(), new string[resp.getGroupsCount
				()]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool isMethodSupported(string methodName)
		{
			return org.apache.hadoop.ipc.RpcClientUtil.isMethodSupported(rpcProxy, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB))), methodName
				);
		}
	}
}
