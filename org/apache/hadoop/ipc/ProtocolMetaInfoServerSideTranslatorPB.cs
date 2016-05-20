using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// This class serves the requests for protocol versions and signatures by
	/// looking them up in the server registry.
	/// </summary>
	public class ProtocolMetaInfoServerSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInfoPB
	{
		internal org.apache.hadoop.ipc.RPC.Server server;

		public ProtocolMetaInfoServerSideTranslatorPB(org.apache.hadoop.ipc.RPC.Server server
			)
		{
			this.server = server;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto
			 getProtocolVersions(com.google.protobuf.RpcController controller, org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsRequestProto
			 request)
		{
			string protocol = request.getProtocol();
			org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto.Builder
				 builder = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto
				.newBuilder();
			foreach (org.apache.hadoop.ipc.RPC.RpcKind r in org.apache.hadoop.ipc.RPC.RpcKind
				.values())
			{
				long[] versions;
				try
				{
					versions = getProtocolVersionForRpcKind(r, protocol);
				}
				catch (java.lang.ClassNotFoundException e)
				{
					throw new com.google.protobuf.ServiceException(e);
				}
				org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto.Builder b = 
					org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto.newBuilder
					();
				if (versions != null)
				{
					b.setRpcKind(r.ToString());
					foreach (long v in versions)
					{
						b.addVersions(v);
					}
				}
				builder.addProtocolVersions(((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto
					)b.build()));
			}
			return ((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto
				)builder.build());
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
			 getProtocolSignature(com.google.protobuf.RpcController controller, org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto
			 request)
		{
			org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto.Builder
				 builder = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
				.newBuilder();
			string protocol = request.getProtocol();
			string rpcKind = request.getRpcKind();
			long[] versions;
			try
			{
				versions = getProtocolVersionForRpcKind(org.apache.hadoop.ipc.RPC.RpcKind.valueOf
					(rpcKind), protocol);
			}
			catch (java.lang.ClassNotFoundException e1)
			{
				throw new com.google.protobuf.ServiceException(e1);
			}
			if (versions == null)
			{
				return ((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
					)builder.build());
			}
			foreach (long v in versions)
			{
				org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto.Builder 
					sigBuilder = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto
					.newBuilder();
				sigBuilder.setVersion(v);
				try
				{
					org.apache.hadoop.ipc.ProtocolSignature signature = org.apache.hadoop.ipc.ProtocolSignature
						.getProtocolSignature(protocol, v);
					foreach (int m in signature.getMethods())
					{
						sigBuilder.addMethods(m);
					}
				}
				catch (java.lang.ClassNotFoundException e)
				{
					throw new com.google.protobuf.ServiceException(e);
				}
				builder.addProtocolSignature(((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto
					)sigBuilder.build()));
			}
			return ((org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto
				)builder.build());
		}

		/// <exception cref="java.lang.ClassNotFoundException"/>
		private long[] getProtocolVersionForRpcKind(org.apache.hadoop.ipc.RPC.RpcKind rpcKind
			, string protocol)
		{
			java.lang.Class protocolClass = java.lang.Class.forName(protocol);
			string protocolName = org.apache.hadoop.ipc.RPC.getProtocolName(protocolClass);
			org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl[] vers = server.getSupportedProtocolVersions
				(rpcKind, protocolName);
			if (vers == null)
			{
				return null;
			}
			long[] versions = new long[vers.Length];
			for (int i = 0; i < versions.Length; i++)
			{
				versions[i] = vers[i].version;
			}
			return versions;
		}
	}
}
