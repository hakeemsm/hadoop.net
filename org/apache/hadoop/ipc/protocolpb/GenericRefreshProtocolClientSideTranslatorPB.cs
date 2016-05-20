using Sharpen;

namespace org.apache.hadoop.ipc.protocolPB
{
	public class GenericRefreshProtocolClientSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInterface
		, org.apache.hadoop.ipc.GenericRefreshProtocol, java.io.Closeable
	{
		/// <summary>RpcController is not used and hence is set to null.</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB rpcProxy;

		public GenericRefreshProtocolClientSideTranslatorPB(org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB
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
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse
			> refresh(string identifier, string[] args)
		{
			System.Collections.Generic.IList<string> argList = java.util.Arrays.asList(args);
			try
			{
				org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto
					 request = ((org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto
					)org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto
					.newBuilder().setIdentifier(identifier).addAllArgs(argList).build());
				org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
					 resp = rpcProxy.refresh(NULL_CONTROLLER, request);
				return unpack(resp);
			}
			catch (com.google.protobuf.ServiceException se)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(se);
			}
		}

		private System.Collections.Generic.ICollection<org.apache.hadoop.ipc.RefreshResponse
			> unpack(org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto
			 collection)
		{
			System.Collections.Generic.IList<org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto
				> responseProtos = collection.getResponsesList();
			System.Collections.Generic.IList<org.apache.hadoop.ipc.RefreshResponse> responses
				 = new System.Collections.Generic.List<org.apache.hadoop.ipc.RefreshResponse>();
			foreach (org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto
				 rp in responseProtos)
			{
				org.apache.hadoop.ipc.RefreshResponse response = unpack(rp);
				responses.add(response);
			}
			return responses;
		}

		private org.apache.hadoop.ipc.RefreshResponse unpack(org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto
			 proto)
		{
			// The default values
			string message = null;
			string sender = null;
			int returnCode = -1;
			// ... that can be overridden by data from the protobuf
			if (proto.hasUserMessage())
			{
				message = proto.getUserMessage();
			}
			if (proto.hasExitStatus())
			{
				returnCode = proto.getExitStatus();
			}
			if (proto.hasSenderName())
			{
				sender = proto.getSenderName();
			}
			// ... and put into a RefreshResponse
			org.apache.hadoop.ipc.RefreshResponse response = new org.apache.hadoop.ipc.RefreshResponse
				(returnCode, message);
			response.setSenderName(sender);
			return response;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool isMethodSupported(string methodName)
		{
			return org.apache.hadoop.ipc.RpcClientUtil.isMethodSupported(rpcProxy, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB))), methodName
				);
		}
	}
}
