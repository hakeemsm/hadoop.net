using System.IO;
using Com.Google.Common.Primitives;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	public class TestPacketReceiver
	{
		private const long OffsetInBlock = 12345L;

		private const int Seqno = 54321;

		/// <exception cref="System.IO.IOException"/>
		private byte[] PrepareFakePacket(byte[] data, byte[] sums)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			int packetLen = data.Length + sums.Length + Ints.Bytes;
			PacketHeader header = new PacketHeader(packetLen, OffsetInBlock, Seqno, false, data
				.Length, false);
			header.Write(dos);
			dos.Write(sums);
			dos.Write(data);
			dos.Flush();
			return baos.ToByteArray();
		}

		private static byte[] RemainingAsArray(ByteBuffer buf)
		{
			byte[] b = new byte[buf.Remaining()];
			buf.Get(b);
			return b;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReceiveAndMirror()
		{
			PacketReceiver pr = new PacketReceiver(false);
			// Test three different lengths, to force reallocing
			// the buffer as it grows.
			DoTestReceiveAndMirror(pr, 100, 10);
			DoTestReceiveAndMirror(pr, 50, 10);
			DoTestReceiveAndMirror(pr, 150, 10);
			pr.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoTestReceiveAndMirror(PacketReceiver pr, int dataLen, int checksumsLen
			)
		{
			byte[] Data = AppendTestUtil.InitBuffer(dataLen);
			byte[] Checksums = AppendTestUtil.InitBuffer(checksumsLen);
			byte[] packet = PrepareFakePacket(Data, Checksums);
			ByteArrayInputStream @in = new ByteArrayInputStream(packet);
			pr.ReceiveNextPacket(@in);
			ByteBuffer parsedData = pr.GetDataSlice();
			Assert.AssertArrayEquals(Data, RemainingAsArray(parsedData));
			ByteBuffer parsedChecksums = pr.GetChecksumSlice();
			Assert.AssertArrayEquals(Checksums, RemainingAsArray(parsedChecksums));
			PacketHeader header = pr.GetHeader();
			NUnit.Framework.Assert.AreEqual(Seqno, header.GetSeqno());
			NUnit.Framework.Assert.AreEqual(OffsetInBlock, header.GetOffsetInBlock());
			NUnit.Framework.Assert.AreEqual(dataLen + checksumsLen + Ints.Bytes, header.GetPacketLen
				());
			// Mirror the packet to an output stream and make sure it matches
			// the packet we sent.
			ByteArrayOutputStream mirrored = new ByteArrayOutputStream();
			mirrored = Org.Mockito.Mockito.Spy(mirrored);
			pr.MirrorPacketTo(new DataOutputStream(mirrored));
			// The write should be done in a single call. Otherwise we may hit
			// nasty interactions with nagling (eg HDFS-4049).
			Org.Mockito.Mockito.Verify(mirrored, Org.Mockito.Mockito.Times(1)).Write(Org.Mockito.Mockito
				.Any<byte[]>(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.Eq(packet.Length
				));
			Org.Mockito.Mockito.VerifyNoMoreInteractions(mirrored);
			Assert.AssertArrayEquals(packet, mirrored.ToByteArray());
		}
	}
}
