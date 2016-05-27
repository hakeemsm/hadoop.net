using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestProtoUtil
	{
		/// <summary>Values to test encoding as variable length integers</summary>
		private static readonly int[] TestVintValues = new int[] { 0, 1, -1, 127, 128, 129
			, 255, 256, 257, unchecked((int)(0x1234)), -unchecked((int)(0x1234)), unchecked(
			(int)(0x123456)), -unchecked((int)(0x123456)), unchecked((int)(0x12345678)), -unchecked(
			(int)(0x12345678)) };

		/// <summary>
		/// Test that readRawVarint32 is compatible with the varints encoded
		/// by ProtoBuf's CodedOutputStream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestVarInt()
		{
			// Test a few manufactured values
			foreach (int value in TestVintValues)
			{
				DoVarIntTest(value);
			}
			// Check 1-bits at every bit position
			for (int i = 1; i != 0; i <<= 1)
			{
				DoVarIntTest(i);
				DoVarIntTest(-i);
				DoVarIntTest(i - 1);
				DoVarIntTest(~i);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoVarIntTest(int value)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			CodedOutputStream cout = CodedOutputStream.NewInstance(baos);
			cout.WriteRawVarint32(value);
			cout.Flush();
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.ToByteArray
				()));
			NUnit.Framework.Assert.AreEqual(value, ProtoUtil.ReadRawVarint32(dis));
		}

		[NUnit.Framework.Test]
		public virtual void TestRpcClientId()
		{
			byte[] uuid = ClientId.GetClientId();
			RpcHeaderProtos.RpcRequestHeaderProto header = ProtoUtil.MakeRpcRequestHeader(RPC.RpcKind
				.RpcProtocolBuffer, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RpcFinalPacket
				, 0, RpcConstants.InvalidRetryCount, uuid);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(uuid, header.GetClientId().ToByteArray
				()));
		}
	}
}
