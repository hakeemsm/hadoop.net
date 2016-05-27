using Org.Apache.Commons.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class RpcConstants
	{
		private RpcConstants()
		{
		}

		public const int AuthorizationFailedCallId = -1;

		public const int InvalidCallId = -2;

		public const int ConnectionContextCallId = -3;

		public const int PingCallId = -4;

		public static readonly byte[] DummyClientId = new byte[0];

		public const int InvalidRetryCount = -1;

		/// <summary>The first four bytes of Hadoop RPC connections</summary>
		public static readonly ByteBuffer Header = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString
			("hrpc", Charsets.Utf8));

		public const byte CurrentVersion = 9;
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
