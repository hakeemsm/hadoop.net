using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Indicates an exception on the RPC server</summary>
	[System.Serializable]
	public class RpcServerException : org.apache.hadoop.ipc.RpcException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="message">detailed message.</param>
		public RpcServerException(string message)
			: base(message)
		{
		}

		/// <summary>Constructs exception with the specified detail message and cause.</summary>
		/// <param name="message">message.</param>
		/// <param name="cause">
		/// the cause (can be retried by the
		/// <see cref="System.Exception.InnerException()"/>
		/// method).
		/// (A <tt>null</tt> value is permitted, and indicates that the cause
		/// is nonexistent or unknown.)
		/// </param>
		public RpcServerException(string message, System.Exception cause)
			: base(message, cause)
		{
		}

		/// <summary>get the rpc status corresponding to this exception</summary>
		public virtual org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
			 getRpcStatusProto()
		{
			return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
				.ERROR;
		}

		/// <summary>get the detailed rpc status corresponding to this exception</summary>
		public virtual org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 getRpcErrorCodeProto()
		{
			return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				.ERROR_RPC_SERVER;
		}
	}
}
