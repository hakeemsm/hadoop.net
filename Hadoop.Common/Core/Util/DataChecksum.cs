using System;
using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// This class provides interface and utilities for processing checksums for
	/// DFS data transfers.
	/// </summary>
	public class DataChecksum : Checksum
	{
		public const int ChecksumNull = 0;

		public const int ChecksumCrc32 = 1;

		public const int ChecksumCrc32c = 2;

		public const int ChecksumDefault = 3;

		public const int ChecksumMixed = 4;

		/// <summary>The checksum types</summary>
		[System.Serializable]
		public sealed class Type
		{
			public static readonly DataChecksum.Type Null = new DataChecksum.Type(ChecksumNull
				, 0);

			public static readonly DataChecksum.Type Crc32 = new DataChecksum.Type(ChecksumCrc32
				, 4);

			public static readonly DataChecksum.Type Crc32c = new DataChecksum.Type(ChecksumCrc32c
				, 4);

			public static readonly DataChecksum.Type Default = new DataChecksum.Type(ChecksumDefault
				, 0);

			public static readonly DataChecksum.Type Mixed = new DataChecksum.Type(ChecksumMixed
				, 0);

			public readonly int id;

			public readonly int size;

			private Type(int id, int size)
			{
				// checksum types
				// This cannot be used to create DataChecksum
				// This cannot be used to create DataChecksum
				this.id = id;
				this.size = size;
			}

			/// <returns>the type corresponding to the id.</returns>
			public static DataChecksum.Type ValueOf(int id)
			{
				if (id < 0 || id >= Values().Length)
				{
					throw new ArgumentException("id=" + id + " out of range [0, " + Values().Length +
						 ")");
				}
				return Values()[id];
			}
		}

		/// <summary>Create a Crc32 Checksum object.</summary>
		/// <remarks>
		/// Create a Crc32 Checksum object. The implementation of the Crc32 algorithm
		/// is chosen depending on the platform.
		/// </remarks>
		public static Checksum NewCrc32()
		{
			return Shell.IsJava7OrAbove() ? new CRC32() : new PureJavaCrc32();
		}

		public static DataChecksum NewDataChecksum(DataChecksum.Type type, int bytesPerChecksum
			)
		{
			if (bytesPerChecksum <= 0)
			{
				return null;
			}
			switch (type)
			{
				case DataChecksum.Type.Null:
				{
					return new DataChecksum(type, new DataChecksum.ChecksumNull(), bytesPerChecksum);
				}

				case DataChecksum.Type.Crc32:
				{
					return new DataChecksum(type, NewCrc32(), bytesPerChecksum);
				}

				case DataChecksum.Type.Crc32c:
				{
					return new DataChecksum(type, new PureJavaCrc32C(), bytesPerChecksum);
				}

				default:
				{
					return null;
				}
			}
		}

		/// <summary>Creates a DataChecksum from HEADER_LEN bytes from arr[offset].</summary>
		/// <returns>DataChecksum of the type in the array or null in case of an error.</returns>
		public static DataChecksum NewDataChecksum(byte[] bytes, int offset)
		{
			if (offset < 0 || bytes.Length < offset + GetChecksumHeaderSize())
			{
				return null;
			}
			// like readInt():
			int bytesPerChecksum = ((bytes[offset + 1] & unchecked((int)(0xff))) << 24) | ((bytes
				[offset + 2] & unchecked((int)(0xff))) << 16) | ((bytes[offset + 3] & unchecked(
				(int)(0xff))) << 8) | ((bytes[offset + 4] & unchecked((int)(0xff))));
			return NewDataChecksum(DataChecksum.Type.ValueOf(bytes[offset]), bytesPerChecksum
				);
		}

		/// <summary>
		/// This constructs a DataChecksum by reading HEADER_LEN bytes from input
		/// stream <i>in</i>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static DataChecksum NewDataChecksum(DataInputStream @in)
		{
			int type = @in.ReadByte();
			int bpc = @in.ReadInt();
			DataChecksum summer = NewDataChecksum(DataChecksum.Type.ValueOf(type), bpc);
			if (summer == null)
			{
				throw new IOException("Could not create DataChecksum of type " + type + " with bytesPerChecksum "
					 + bpc);
			}
			return summer;
		}

		/// <summary>Writes the checksum header to the output stream <i>out</i>.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteHeader(DataOutputStream @out)
		{
			@out.WriteByte(type.id);
			@out.WriteInt(bytesPerChecksum);
		}

		public virtual byte[] GetHeader()
		{
			byte[] header = new byte[GetChecksumHeaderSize()];
			header[0] = unchecked((byte)(type.id & unchecked((int)(0xff))));
			// Writing in buffer just like BinaryWriter.WriteInt()
			header[1 + 0] = unchecked((byte)(((int)(((uint)bytesPerChecksum) >> 24)) & unchecked(
				(int)(0xff))));
			header[1 + 1] = unchecked((byte)(((int)(((uint)bytesPerChecksum) >> 16)) & unchecked(
				(int)(0xff))));
			header[1 + 2] = unchecked((byte)(((int)(((uint)bytesPerChecksum) >> 8)) & unchecked(
				(int)(0xff))));
			header[1 + 3] = unchecked((byte)(bytesPerChecksum & unchecked((int)(0xff))));
			return header;
		}

		/// <summary>Writes the current checksum to the stream.</summary>
		/// <remarks>
		/// Writes the current checksum to the stream.
		/// If <i>reset</i> is true, then resets the checksum.
		/// </remarks>
		/// <returns>number of bytes written. Will be equal to getChecksumSize();</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int WriteValue(DataOutputStream @out, bool reset)
		{
			if (type.size <= 0)
			{
				return 0;
			}
			if (type.size == 4)
			{
				@out.WriteInt((int)summer.GetValue());
			}
			else
			{
				throw new IOException("Unknown Checksum " + type);
			}
			if (reset)
			{
				Reset();
			}
			return type.size;
		}

		/// <summary>Writes the current checksum to a buffer.</summary>
		/// <remarks>
		/// Writes the current checksum to a buffer.
		/// If <i>reset</i> is true, then resets the checksum.
		/// </remarks>
		/// <returns>number of bytes written. Will be equal to getChecksumSize();</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int WriteValue(byte[] buf, int offset, bool reset)
		{
			if (type.size <= 0)
			{
				return 0;
			}
			if (type.size == 4)
			{
				int checksum = (int)summer.GetValue();
				buf[offset + 0] = unchecked((byte)(((int)(((uint)checksum) >> 24)) & unchecked((int
					)(0xff))));
				buf[offset + 1] = unchecked((byte)(((int)(((uint)checksum) >> 16)) & unchecked((int
					)(0xff))));
				buf[offset + 2] = unchecked((byte)(((int)(((uint)checksum) >> 8)) & unchecked((int
					)(0xff))));
				buf[offset + 3] = unchecked((byte)(checksum & unchecked((int)(0xff))));
			}
			else
			{
				throw new IOException("Unknown Checksum " + type);
			}
			if (reset)
			{
				Reset();
			}
			return type.size;
		}

		/// <summary>Compares the checksum located at buf[offset] with the current checksum.</summary>
		/// <returns>true if the checksum matches and false otherwise.</returns>
		public virtual bool Compare(byte[] buf, int offset)
		{
			if (type.size == 4)
			{
				int checksum = ((buf[offset + 0] & unchecked((int)(0xff))) << 24) | ((buf[offset 
					+ 1] & unchecked((int)(0xff))) << 16) | ((buf[offset + 2] & unchecked((int)(0xff
					))) << 8) | ((buf[offset + 3] & unchecked((int)(0xff))));
				return checksum == (int)summer.GetValue();
			}
			return type.size == 0;
		}

		private readonly DataChecksum.Type type;

		private readonly Checksum summer;

		private readonly int bytesPerChecksum;

		private int inSum = 0;

		private DataChecksum(DataChecksum.Type type, Checksum checksum, int chunkSize)
		{
			this.type = type;
			summer = checksum;
			bytesPerChecksum = chunkSize;
		}

		/// <returns>the checksum algorithm type.</returns>
		public virtual DataChecksum.Type GetChecksumType()
		{
			return type;
		}

		/// <returns>the size for a checksum.</returns>
		public virtual int GetChecksumSize()
		{
			return type.size;
		}

		/// <returns>the required checksum size given the data length.</returns>
		public virtual int GetChecksumSize(int dataSize)
		{
			return ((dataSize - 1) / GetBytesPerChecksum() + 1) * GetChecksumSize();
		}

		public virtual int GetBytesPerChecksum()
		{
			return bytesPerChecksum;
		}

		public virtual int GetNumBytesInSum()
		{
			return inSum;
		}

		public const int SizeOfInteger = int.Size / byte.Size;

		public static int GetChecksumHeaderSize()
		{
			return 1 + SizeOfInteger;
		}

		// type byte, bytesPerChecksum int
		//Checksum Interface. Just a wrapper around member summer.
		public virtual long GetValue()
		{
			return summer.GetValue();
		}

		public virtual void Reset()
		{
			summer.Reset();
			inSum = 0;
		}

		public virtual void Update(byte[] b, int off, int len)
		{
			if (len > 0)
			{
				summer.Update(b, off, len);
				inSum += len;
			}
		}

		public virtual void Update(int b)
		{
			summer.Update(b);
			inSum += 1;
		}

		/// <summary>Verify that the given checksums match the given data.</summary>
		/// <remarks>
		/// Verify that the given checksums match the given data.
		/// The 'mark' of the ByteBuffer parameters may be modified by this function,.
		/// but the position is maintained.
		/// </remarks>
		/// <param name="data">the DirectByteBuffer pointing to the data to verify.</param>
		/// <param name="checksums">
		/// the DirectByteBuffer pointing to a series of stored
		/// checksums
		/// </param>
		/// <param name="fileName">the name of the file being read, for error-reporting</param>
		/// <param name="basePos">the file position to which the start of 'data' corresponds</param>
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException">if the checksums do not match
		/// 	</exception>
		public virtual void VerifyChunkedSums(ByteBuffer data, ByteBuffer checksums, string
			 fileName, long basePos)
		{
			if (type.size == 0)
			{
				return;
			}
			if (data.HasArray() && checksums.HasArray())
			{
				VerifyChunkedSums(((byte[])data.Array()), data.ArrayOffset() + data.Position(), data
					.Remaining(), ((byte[])checksums.Array()), checksums.ArrayOffset() + checksums.Position
					(), fileName, basePos);
				return;
			}
			if (NativeCrc32.IsAvailable())
			{
				NativeCrc32.VerifyChunkedSums(bytesPerChecksum, type.id, checksums, data, fileName
					, basePos);
				return;
			}
			int startDataPos = data.Position();
			data.Mark();
			checksums.Mark();
			try
			{
				byte[] buf = new byte[bytesPerChecksum];
				byte[] sum = new byte[type.size];
				while (data.Remaining() > 0)
				{
					int n = Math.Min(data.Remaining(), bytesPerChecksum);
					checksums.Get(sum);
					data.Get(buf, 0, n);
					summer.Reset();
					summer.Update(buf, 0, n);
					int calculated = (int)summer.GetValue();
					int stored = (sum[0] << 24 & unchecked((int)(0xff000000))) | (sum[1] << 16 & unchecked(
						(int)(0xff0000))) | (sum[2] << 8 & unchecked((int)(0xff00))) | sum[3] & unchecked(
						(int)(0xff));
					if (calculated != stored)
					{
						long errPos = basePos + data.Position() - startDataPos - n;
						throw new ChecksumException("Checksum error: " + fileName + " at " + errPos + " exp: "
							 + stored + " got: " + calculated, errPos);
					}
				}
			}
			finally
			{
				data.Reset();
				checksums.Reset();
			}
		}

		/// <summary>Implementation of chunked verification specifically on byte arrays.</summary>
		/// <remarks>
		/// Implementation of chunked verification specifically on byte arrays. This
		/// is to avoid the copy when dealing with ByteBuffers that have array backing.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		private void VerifyChunkedSums(byte[] data, int dataOff, int dataLen, byte[] checksums
			, int checksumsOff, string fileName, long basePos)
		{
			if (type.size == 0)
			{
				return;
			}
			if (NativeCrc32.IsAvailable())
			{
				NativeCrc32.VerifyChunkedSumsByteArray(bytesPerChecksum, type.id, checksums, checksumsOff
					, data, dataOff, dataLen, fileName, basePos);
				return;
			}
			int remaining = dataLen;
			int dataPos = 0;
			while (remaining > 0)
			{
				int n = Math.Min(remaining, bytesPerChecksum);
				summer.Reset();
				summer.Update(data, dataOff + dataPos, n);
				dataPos += n;
				remaining -= n;
				int calculated = (int)summer.GetValue();
				int stored = (checksums[checksumsOff] << 24 & unchecked((int)(0xff000000))) | (checksums
					[checksumsOff + 1] << 16 & unchecked((int)(0xff0000))) | (checksums[checksumsOff
					 + 2] << 8 & unchecked((int)(0xff00))) | checksums[checksumsOff + 3] & unchecked(
					(int)(0xff));
				checksumsOff += 4;
				if (calculated != stored)
				{
					long errPos = basePos + dataPos - n;
					throw new ChecksumException("Checksum error: " + fileName + " at " + errPos + " exp: "
						 + stored + " got: " + calculated, errPos);
				}
			}
		}

		/// <summary>Calculate checksums for the given data.</summary>
		/// <remarks>
		/// Calculate checksums for the given data.
		/// The 'mark' of the ByteBuffer parameters may be modified by this function,
		/// but the position is maintained.
		/// </remarks>
		/// <param name="data">the DirectByteBuffer pointing to the data to checksum.</param>
		/// <param name="checksums">
		/// the DirectByteBuffer into which checksums will be
		/// stored. Enough space must be available in this
		/// buffer to put the checksums.
		/// </param>
		public virtual void CalculateChunkedSums(ByteBuffer data, ByteBuffer checksums)
		{
			if (type.size == 0)
			{
				return;
			}
			if (data.HasArray() && checksums.HasArray())
			{
				CalculateChunkedSums(((byte[])data.Array()), data.ArrayOffset() + data.Position()
					, data.Remaining(), ((byte[])checksums.Array()), checksums.ArrayOffset() + checksums
					.Position());
				return;
			}
			if (NativeCrc32.IsAvailable())
			{
				NativeCrc32.CalculateChunkedSums(bytesPerChecksum, type.id, checksums, data);
				return;
			}
			data.Mark();
			checksums.Mark();
			try
			{
				byte[] buf = new byte[bytesPerChecksum];
				while (data.Remaining() > 0)
				{
					int n = Math.Min(data.Remaining(), bytesPerChecksum);
					data.Get(buf, 0, n);
					summer.Reset();
					summer.Update(buf, 0, n);
					checksums.PutInt((int)summer.GetValue());
				}
			}
			finally
			{
				data.Reset();
				checksums.Reset();
			}
		}

		/// <summary>Implementation of chunked calculation specifically on byte arrays.</summary>
		/// <remarks>
		/// Implementation of chunked calculation specifically on byte arrays. This
		/// is to avoid the copy when dealing with ByteBuffers that have array backing.
		/// </remarks>
		public virtual void CalculateChunkedSums(byte[] data, int dataOffset, int dataLength
			, byte[] sums, int sumsOffset)
		{
			if (type.size == 0)
			{
				return;
			}
			if (NativeCrc32.IsAvailable())
			{
				NativeCrc32.CalculateChunkedSumsByteArray(bytesPerChecksum, type.id, sums, sumsOffset
					, data, dataOffset, dataLength);
				return;
			}
			int remaining = dataLength;
			while (remaining > 0)
			{
				int n = Math.Min(remaining, bytesPerChecksum);
				summer.Reset();
				summer.Update(data, dataOffset, n);
				dataOffset += n;
				remaining -= n;
				long calculated = summer.GetValue();
				sums[sumsOffset++] = unchecked((byte)(calculated >> 24));
				sums[sumsOffset++] = unchecked((byte)(calculated >> 16));
				sums[sumsOffset++] = unchecked((byte)(calculated >> 8));
				sums[sumsOffset++] = unchecked((byte)(calculated));
			}
		}

		public override bool Equals(object other)
		{
			if (!(other is DataChecksum))
			{
				return false;
			}
			DataChecksum o = (DataChecksum)other;
			return o.bytesPerChecksum == this.bytesPerChecksum && o.type == this.type;
		}

		public override int GetHashCode()
		{
			return (this.type.id + 31) * this.bytesPerChecksum;
		}

		public override string ToString()
		{
			return "DataChecksum(type=" + type + ", chunkSize=" + bytesPerChecksum + ")";
		}

		/// <summary>
		/// This just provides a dummy implimentation for Checksum class
		/// This is used when there is no checksum available or required for
		/// data
		/// </summary>
		internal class ChecksumNull : Checksum
		{
			public ChecksumNull()
			{
			}

			//Dummy interface
			public virtual long GetValue()
			{
				return 0;
			}

			public virtual void Reset()
			{
			}

			public virtual void Update(byte[] b, int off, int len)
			{
			}

			public virtual void Update(int b)
			{
			}
		}
	}
}
