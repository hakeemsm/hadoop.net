using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class RpcConstants
	{
		private RpcConstants()
		{
		}

		public const int AUTHORIZATION_FAILED_CALL_ID = -1;

		public const int INVALID_CALL_ID = -2;

		public const int CONNECTION_CONTEXT_CALL_ID = -3;

		public const int PING_CALL_ID = -4;

		public static readonly byte[] DUMMY_CLIENT_ID = new byte[0];

		public const int INVALID_RETRY_COUNT = -1;

		/// <summary>The first four bytes of Hadoop RPC connections</summary>
		public static readonly java.nio.ByteBuffer HEADER = java.nio.ByteBuffer.wrap(Sharpen.Runtime.getBytesForString
			("hrpc", org.apache.commons.io.Charsets.UTF_8));

		public const byte CURRENT_VERSION = 9;
		// Hidden Constructor
		// 1 : Introduce ping and server does not throw away RPCs
		// 3 : Introduce the protocol into the RPC connection header
		// 4 : Introduced SASL security layer
		// 5 : Introduced use of {@link ArrayPrimitiveWritable$Internal}
		//     in ObjectWritable to efficiently transmit arrays of primitives
		// 6 : Made RPC Request header explicit
		// 7 : Changed Ipc Connection Header to use Protocol buffers
		// 8 : SASL server always sends a final response
		// 9 : Changes to protocol for HADOOP-8990
	}
}
