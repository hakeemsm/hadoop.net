using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Util
{
	public class TestNativeCrc32
	{
		private const long BasePosition = 0;

		private const int IoBytesPerChecksumDefault = 512;

		private const string IoBytesPerChecksumKey = "io.bytes.per.checksum";

		private const int NumChunks = 3;

		private readonly DataChecksum.Type checksumType;

		private int bytesPerChecksum;

		private string fileName;

		private ByteBuffer data;

		private ByteBuffer checksums;

		private DataChecksum checksum;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			ICollection<object[]> @params = new AList<object[]>(2);
			@params.AddItem(new object[] { DataChecksum.Type.Crc32 });
			@params.AddItem(new object[] { DataChecksum.Type.Crc32c });
			return @params;
		}

		public TestNativeCrc32(DataChecksum.Type checksumType)
		{
			this.checksumType = checksumType;
		}

		[SetUp]
		public virtual void Setup()
		{
			Assume.AssumeTrue(NativeCrc32.IsAvailable());
			Assert.Equal("These tests assume they can write a checksum value as a 4-byte int."
				, 4, checksumType.size);
			Configuration conf = new Configuration();
			bytesPerChecksum = conf.GetInt(IoBytesPerChecksumKey, IoBytesPerChecksumDefault);
			fileName = this.GetType().Name;
			checksum = DataChecksum.NewDataChecksum(checksumType, bytesPerChecksum);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestVerifyChunkedSumsSuccess()
		{
			AllocateDirectByteBuffers();
			FillDataAndValidChecksums();
			NativeCrc32.VerifyChunkedSums(bytesPerChecksum, checksumType.id, checksums, data, 
				fileName, BasePosition);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestVerifyChunkedSumsFail()
		{
			AllocateDirectByteBuffers();
			FillDataAndInvalidChecksums();
			exception.Expect(typeof(ChecksumException));
			NativeCrc32.VerifyChunkedSums(bytesPerChecksum, checksumType.id, checksums, data, 
				fileName, BasePosition);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestVerifyChunkedSumsByteArraySuccess()
		{
			AllocateArrayByteBuffers();
			FillDataAndValidChecksums();
			NativeCrc32.VerifyChunkedSumsByteArray(bytesPerChecksum, checksumType.id, ((byte[]
				)checksums.Array()), checksums.Position(), ((byte[])data.Array()), data.Position
				(), data.Remaining(), fileName, BasePosition);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestVerifyChunkedSumsByteArrayFail()
		{
			AllocateArrayByteBuffers();
			FillDataAndInvalidChecksums();
			exception.Expect(typeof(ChecksumException));
			NativeCrc32.VerifyChunkedSumsByteArray(bytesPerChecksum, checksumType.id, ((byte[]
				)checksums.Array()), checksums.Position(), ((byte[])data.Array()), data.Position
				(), data.Remaining(), fileName, BasePosition);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestCalculateChunkedSumsSuccess()
		{
			AllocateDirectByteBuffers();
			FillDataAndValidChecksums();
			NativeCrc32.CalculateChunkedSums(bytesPerChecksum, checksumType.id, checksums, data
				);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestCalculateChunkedSumsFail()
		{
			AllocateDirectByteBuffers();
			FillDataAndInvalidChecksums();
			NativeCrc32.CalculateChunkedSums(bytesPerChecksum, checksumType.id, checksums, data
				);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestCalculateChunkedSumsByteArraySuccess()
		{
			AllocateArrayByteBuffers();
			FillDataAndValidChecksums();
			NativeCrc32.CalculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id, ((byte
				[])checksums.Array()), checksums.Position(), ((byte[])data.Array()), data.Position
				(), data.Remaining());
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestCalculateChunkedSumsByteArrayFail()
		{
			AllocateArrayByteBuffers();
			FillDataAndInvalidChecksums();
			NativeCrc32.CalculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id, ((byte
				[])checksums.Array()), checksums.Position(), ((byte[])data.Array()), data.Position
				(), data.Remaining());
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestNativeVerifyChunkedSumsSuccess()
		{
			AllocateDirectByteBuffers();
			FillDataAndValidChecksums();
			NativeCrc32.NativeVerifyChunkedSums(bytesPerChecksum, checksumType.id, checksums, 
				checksums.Position(), data, data.Position(), data.Remaining(), fileName, BasePosition
				);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		[Fact]
		public virtual void TestNativeVerifyChunkedSumsFail()
		{
			AllocateDirectByteBuffers();
			FillDataAndInvalidChecksums();
			exception.Expect(typeof(ChecksumException));
			NativeCrc32.NativeVerifyChunkedSums(bytesPerChecksum, checksumType.id, checksums, 
				checksums.Position(), data, data.Position(), data.Remaining(), fileName, BasePosition
				);
		}

		/// <summary>Allocates data buffer and checksums buffer as arrays on the heap.</summary>
		private void AllocateArrayByteBuffers()
		{
			data = ByteBuffer.Wrap(new byte[bytesPerChecksum * NumChunks]);
			checksums = ByteBuffer.Wrap(new byte[NumChunks * checksumType.size]);
		}

		/// <summary>Allocates data buffer and checksums buffer as direct byte buffers.</summary>
		private void AllocateDirectByteBuffers()
		{
			data = ByteBuffer.AllocateDirect(bytesPerChecksum * NumChunks);
			checksums = ByteBuffer.AllocateDirect(NumChunks * checksumType.size);
		}

		/// <summary>Fill data buffer with monotonically increasing byte values.</summary>
		/// <remarks>
		/// Fill data buffer with monotonically increasing byte values.  Overflow is
		/// fine, because it's just test data.  Update the checksum with the same byte
		/// values.  After every chunk, write the checksum to the checksums buffer.
		/// After finished writing, flip the buffers to prepare them for reading.
		/// </remarks>
		private void FillDataAndValidChecksums()
		{
			for (int i = 0; i < NumChunks; ++i)
			{
				for (int j = 0; j < bytesPerChecksum; ++j)
				{
					byte b = unchecked((byte)((i * bytesPerChecksum + j) & unchecked((int)(0xFF))));
					data.Put(b);
					checksum.Update(b);
				}
				checksums.PutInt((int)checksum.GetValue());
				checksum.Reset();
			}
			data.Flip();
			checksums.Flip();
		}

		/// <summary>Fill data buffer with monotonically increasing byte values.</summary>
		/// <remarks>
		/// Fill data buffer with monotonically increasing byte values.  Overflow is
		/// fine, because it's just test data.  Update the checksum with different byte
		/// byte values, so that the checksums are incorrect intentionally.  After every
		/// chunk, write the checksum to the checksums buffer.  After finished writing,
		/// flip the buffers to prepare them for reading.
		/// </remarks>
		private void FillDataAndInvalidChecksums()
		{
			for (int i = 0; i < NumChunks; ++i)
			{
				for (int j = 0; j < bytesPerChecksum; ++j)
				{
					byte b = unchecked((byte)((i * bytesPerChecksum + j) & unchecked((int)(0xFF))));
					data.Put(b);
					checksum.Update(unchecked((byte)(b + 1)));
				}
				checksums.PutInt((int)checksum.GetValue());
				checksum.Reset();
			}
			data.Flip();
			checksums.Flip();
		}
	}
}
