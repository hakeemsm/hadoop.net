using System;
using Org.Apache.Hadoop.Ipc.Protobuf;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Indicates an exception on the RPC server</summary>
	[System.Serializable]
	public class RpcServerException : RpcException
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
		public RpcServerException(string message, Exception cause)
			: base(message, cause)
		{
		}

		/// <summary>get the rpc status corresponding to this exception</summary>
		public virtual RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto GetRpcStatusProto
			()
		{
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error;
		}

		/// <summary>get the detailed rpc status corresponding to this exception</summary>
		public virtual RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto GetRpcErrorCodeProto
			()
		{
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorRpcServer;
		}
	}
}
