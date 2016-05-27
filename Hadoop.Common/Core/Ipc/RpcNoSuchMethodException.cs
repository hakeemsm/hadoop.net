using Org.Apache.Hadoop.Ipc.Protobuf;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>No such Method for an Rpc Call</summary>
	[System.Serializable]
	public class RpcNoSuchMethodException : RpcServerException
	{
		private const long serialVersionUID = 1L;

		public RpcNoSuchMethodException(string message)
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
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorNoSuchMethod;
		}
	}
}
