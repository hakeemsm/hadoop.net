using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSPacket
	{
		private const int chunkSize = 512;

		private const int checksumSize = 4;

		private const int maxChunksPerPacket = 4;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPacket()
		{
			Random r = new Random(12345L);
			byte[] data = new byte[chunkSize];
			r.NextBytes(data);
			byte[] checksum = new byte[checksumSize];
			r.NextBytes(checksum);
			DataOutputBuffer os = new DataOutputBuffer(data.Length * 2);
			byte[] packetBuf = new byte[data.Length * 2];
			DFSPacket p = new DFSPacket(packetBuf, maxChunksPerPacket, 0, 0, checksumSize, false
				);
			p.SetSyncBlock(true);
			p.WriteData(data, 0, data.Length);
			p.WriteChecksum(checksum, 0, checksum.Length);
			p.WriteTo(os);
			//we have set syncBlock to true, so the header has the maximum length
			int headerLen = PacketHeader.PktMaxHeaderLen;
			byte[] readBuf = os.GetData();
			AssertArrayRegionsEqual(readBuf, headerLen, checksum, 0, checksum.Length);
			AssertArrayRegionsEqual(readBuf, headerLen + checksum.Length, data, 0, data.Length
				);
		}

		public static void AssertArrayRegionsEqual(byte[] buf1, int off1, byte[] buf2, int
			 off2, int len)
		{
			for (int i = 0; i < len; i++)
			{
				if (buf1[off1 + i] != buf2[off2 + i])
				{
					NUnit.Framework.Assert.Fail("arrays differ at byte " + i + ". " + "The first array has "
						 + (int)buf1[off1 + i] + ", but the second array has " + (int)buf2[off2 + i]);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddParentsGetParents()
		{
			DFSPacket p = new DFSPacket(null, maxChunksPerPacket, 0, 0, checksumSize, false);
			long[] parents = p.GetTraceParents();
			NUnit.Framework.Assert.AreEqual(0, parents.Length);
			p.AddTraceParent(123);
			p.AddTraceParent(123);
			parents = p.GetTraceParents();
			NUnit.Framework.Assert.AreEqual(1, parents.Length);
			NUnit.Framework.Assert.AreEqual(123, parents[0]);
			parents = p.GetTraceParents();
			// test calling 'get' again.
			NUnit.Framework.Assert.AreEqual(1, parents.Length);
			NUnit.Framework.Assert.AreEqual(123, parents[0]);
			p.AddTraceParent(1);
			p.AddTraceParent(456);
			p.AddTraceParent(789);
			parents = p.GetTraceParents();
			NUnit.Framework.Assert.AreEqual(4, parents.Length);
			NUnit.Framework.Assert.AreEqual(1, parents[0]);
			NUnit.Framework.Assert.AreEqual(123, parents[1]);
			NUnit.Framework.Assert.AreEqual(456, parents[2]);
			NUnit.Framework.Assert.AreEqual(789, parents[3]);
		}
	}
}
