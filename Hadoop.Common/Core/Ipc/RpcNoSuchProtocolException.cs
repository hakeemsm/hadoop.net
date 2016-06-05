using Org.Apache.Hadoop.Ipc.Protobuf;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>No such protocol (i.e.</summary>
	/// <remarks>No such protocol (i.e. interface) for and Rpc Call</remarks>
	[System.Serializable]
	public class RpcNoSuchProtocolException : RpcServerException
	{
		private const long serialVersionUID = 1L;

		public RpcNoSuchProtocolException(string message)
			: base(message)
		{
		}

		/// <summary>get the rpc status corresponding to this exception</summary>
		public override RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto GetRpcStatusProto
			()
		{
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error;
		}

		/// <summary>get the detailed rpc status corresponding to this exception</summary>
		public override RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto GetRpcErrorCodeProto
			()
		{
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorNoSuchProtocol;
		}
	}
}
