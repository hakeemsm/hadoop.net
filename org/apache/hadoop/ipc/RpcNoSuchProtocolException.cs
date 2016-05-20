using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>No such protocol (i.e.</summary>
	/// <remarks>No such protocol (i.e. interface) for and Rpc Call</remarks>
	[System.Serializable]
	public class RpcNoSuchProtocolException : org.apache.hadoop.ipc.RpcServerException
	{
		private const long serialVersionUID = 1L;

		public RpcNoSuchProtocolException(string message)
			: base(message)
		{
		}

		/// <summary>get the rpc status corresponding to this exception</summary>
		public override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
			 getRpcStatusProto()
		{
			return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
				.ERROR;
		}

		/// <summary>get the detailed rpc status corresponding to this exception</summary>
		public override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 getRpcErrorCodeProto()
		{
			return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				.ERROR_NO_SUCH_PROTOCOL;
		}
	}
}
