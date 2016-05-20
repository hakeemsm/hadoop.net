using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>No such Method for an Rpc Call</summary>
	[System.Serializable]
	public class RpcNoSuchMethodException : org.apache.hadoop.ipc.RpcServerException
	{
		private const long serialVersionUID = 1L;

		public RpcNoSuchMethodException(string message)
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
				.ERROR_NO_SUCH_METHOD;
		}
	}
}
