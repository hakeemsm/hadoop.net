using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Util
{
	public class TestDataChecksum
	{
		private const int SumsOffsetInBuffer = 3;

		private const int DataOffsetInBuffer = 3;

		private const int DataTrailerInBuffer = 3;

		private const int BytesPerChunk = 512;

		private static readonly DataChecksum.Type[] ChecksumTypes = new DataChecksum.Type
			[] { DataChecksum.Type.Crc32, DataChecksum.Type.Crc32c };

		// Set up buffers that have some header and trailer before the
		// actual data or checksums, to make sure the code handles
		// buffer.position(), limit, etc correctly.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestBulkOps()
		{
			foreach (DataChecksum.Type type in ChecksumTypes)
			{
				System.Console.Error.WriteLine("---- beginning tests with checksum type " + type 
					+ "----");
				DataChecksum checksum = DataChecksum.NewDataChecksum(type, BytesPerChunk);
				foreach (bool useDirect in new bool[] { false, true })
				{
					DoBulkTest(checksum, 1023, useDirect);
					DoBulkTest(checksum, 1024, useDirect);
					DoBulkTest(checksum, 1025, useDirect);
				}
			}
		}

		private class Harness
		{
			internal readonly DataChecksum checksum;

			internal readonly int dataLength;

			internal readonly int sumsLength;

			internal readonly int numSums;

			internal ByteBuffer dataBuf;

			internal ByteBuffer checksumBuf;

			internal Harness(DataChecksum checksum, int dataLength, bool useDirect)
			{
				this.checksum = checksum;
				this.dataLength = dataLength;
				numSums = (dataLength - 1) / checksum.GetBytesPerChecksum() + 1;
				sumsLength = numSums * checksum.GetChecksumSize();
				byte[] data = new byte[dataLength + DataOffsetInBuffer + DataTrailerInBuffer];
				new Random().NextBytes(data);
				dataBuf = ByteBuffer.Wrap(data, DataOffsetInBuffer, dataLength);
				byte[] checksums = new byte[SumsOffsetInBuffer + sumsLength];
				checksumBuf = ByteBuffer.Wrap(checksums, SumsOffsetInBuffer, sumsLength);
				// Swap out for direct buffers if requested.
				if (useDirect)
				{
					dataBuf = Directify(dataBuf);
					checksumBuf = Directify(checksumBuf);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
			internal virtual void TestCorrectness()
			{
				// calculate real checksum, make sure it passes
				checksum.CalculateChunkedSums(dataBuf, checksumBuf);
				checksum.VerifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
				// Change a byte in the header and in the trailer, make sure
				// it doesn't affect checksum result
				CorruptBufferOffset(checksumBuf, 0);
				checksum.VerifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
				CorruptBufferOffset(dataBuf, 0);
				dataBuf.Limit(dataBuf.Limit() + 1);
				CorruptBufferOffset(dataBuf, dataLength + DataOffsetInBuffer);
				dataBuf.Limit(dataBuf.Limit() - 1);
				checksum.VerifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
				// Make sure bad checksums fail - error at beginning of array
				CorruptBufferOffset(checksumBuf, SumsOffsetInBuffer);
				try
				{
					checksum.VerifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
					NUnit.Framework.Assert.Fail("Did not throw on bad checksums");
				}
				catch (ChecksumException ce)
				{
					Assert.Equal(0, ce.GetPos());
				}
				// Make sure bad checksums fail - error at end of array
				UncorruptBufferOffset(checksumBuf, SumsOffsetInBuffer);
				CorruptBufferOffset(checksumBuf, SumsOffsetInBuffer + sumsLength - 1);
				try
				{
					checksum.VerifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
					NUnit.Framework.Assert.Fail("Did not throw on bad checksums");
				}
				catch (ChecksumException ce)
				{
					int expectedPos = checksum.GetBytesPerChecksum() * (numSums - 1);
					Assert.Equal(expectedPos, ce.GetPos());
					Assert.True(ce.Message.Contains("fake file"));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoBulkTest(DataChecksum checksum, int dataLength, bool useDirect)
		{
			System.Console.Error.WriteLine("Testing bulk checksums of length " + dataLength +
				 " with " + (useDirect ? "direct" : "array-backed") + " buffers");
			new TestDataChecksum.Harness(checksum, dataLength, useDirect).TestCorrectness();
		}

		/// <summary>
		/// Simple performance test for the "common case" checksum usage in HDFS:
		/// computing and verifying CRC32C with 512 byte chunking on native
		/// buffers.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void CommonUsagePerfTest()
		{
			int NumRuns = 5;
			DataChecksum checksum = DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32c, 512
				);
			int dataLength = 512 * 1024 * 1024;
			TestDataChecksum.Harness h = new TestDataChecksum.Harness(checksum, dataLength, true
				);
			for (int i = 0; i < NumRuns; i++)
			{
				StopWatch s = new StopWatch().Start();
				// calculate real checksum, make sure it passes
				checksum.CalculateChunkedSums(h.dataBuf, h.checksumBuf);
				s.Stop();
				System.Console.Error.WriteLine("Calculate run #" + i + ": " + s.Now(TimeUnit.Microseconds
					) + "us");
				s = new StopWatch().Start();
				// calculate real checksum, make sure it passes
				checksum.VerifyChunkedSums(h.dataBuf, h.checksumBuf, "fake file", 0);
				s.Stop();
				System.Console.Error.WriteLine("Verify run #" + i + ": " + s.Now(TimeUnit.Microseconds
					) + "us");
			}
		}

		[Fact]
		public virtual void TestEquality()
		{
			Assert.Equal(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32
				, 512), DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, 512));
			NUnit.Framework.Assert.IsFalse(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32
				, 512).Equals(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, 1024)));
			NUnit.Framework.Assert.IsFalse(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32
				, 512).Equals(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32c, 512)));
		}

		[Fact]
		public virtual void TestToString()
		{
			Assert.Equal("DataChecksum(type=CRC32, chunkSize=512)", DataChecksum
				.NewDataChecksum(DataChecksum.Type.Crc32, 512).ToString());
		}

		private static void CorruptBufferOffset(ByteBuffer buf, int offset)
		{
			buf.Put(offset, unchecked((byte)(buf.Get(offset) + 1)));
		}

		private static void UncorruptBufferOffset(ByteBuffer buf, int offset)
		{
			buf.Put(offset, unchecked((byte)(buf.Get(offset) - 1)));
		}

		private static ByteBuffer Directify(ByteBuffer dataBuf)
		{
			ByteBuffer newBuf = ByteBuffer.AllocateDirect(dataBuf.Capacity());
			newBuf.Position(dataBuf.Position());
			newBuf.Mark();
			newBuf.Put(dataBuf);
			newBuf.Reset();
			newBuf.Limit(dataBuf.Limit());
			return newBuf;
		}
	}
}
