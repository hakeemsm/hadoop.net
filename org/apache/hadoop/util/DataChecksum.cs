using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// This class provides interface and utilities for processing checksums for
	/// DFS data transfers.
	/// </summary>
	public class DataChecksum : java.util.zip.Checksum
	{
		public const int CHECKSUM_NULL = 0;

		public const int CHECKSUM_CRC32 = 1;

		public const int CHECKSUM_CRC32C = 2;

		public const int CHECKSUM_DEFAULT = 3;

		public const int CHECKSUM_MIXED = 4;

		/// <summary>The checksum types</summary>
		[System.Serializable]
		public sealed class Type
		{
			public static readonly org.apache.hadoop.util.DataChecksum.Type NULL = new org.apache.hadoop.util.DataChecksum.Type
				(CHECKSUM_NULL, 0);

			public static readonly org.apache.hadoop.util.DataChecksum.Type CRC32 = new org.apache.hadoop.util.DataChecksum.Type
				(CHECKSUM_CRC32, 4);

			public static readonly org.apache.hadoop.util.DataChecksum.Type CRC32C = new org.apache.hadoop.util.DataChecksum.Type
				(CHECKSUM_CRC32C, 4);

			public static readonly org.apache.hadoop.util.DataChecksum.Type DEFAULT = new org.apache.hadoop.util.DataChecksum.Type
				(CHECKSUM_DEFAULT, 0);

			public static readonly org.apache.hadoop.util.DataChecksum.Type MIXED = new org.apache.hadoop.util.DataChecksum.Type
				(CHECKSUM_MIXED, 0);

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
			public static org.apache.hadoop.util.DataChecksum.Type valueOf(int id)
			{
				if (id < 0 || id >= values().Length)
				{
					throw new System.ArgumentException("id=" + id + " out of range [0, " + values().Length
						 + ")");
				}
				return values()[id];
			}
		}

		/// <summary>Create a Crc32 Checksum object.</summary>
		/// <remarks>
		/// Create a Crc32 Checksum object. The implementation of the Crc32 algorithm
		/// is chosen depending on the platform.
		/// </remarks>
		public static java.util.zip.Checksum newCrc32()
		{
			return org.apache.hadoop.util.Shell.isJava7OrAbove() ? new java.util.zip.CRC32() : 
				new org.apache.hadoop.util.PureJavaCrc32();
		}

		public static org.apache.hadoop.util.DataChecksum newDataChecksum(org.apache.hadoop.util.DataChecksum.Type
			 type, int bytesPerChecksum)
		{
			if (bytesPerChecksum <= 0)
			{
				return null;
			}
			switch (type)
			{
				case org.apache.hadoop.util.DataChecksum.Type.NULL:
				{
					return new org.apache.hadoop.util.DataChecksum(type, new org.apache.hadoop.util.DataChecksum.ChecksumNull
						(), bytesPerChecksum);
				}

				case org.apache.hadoop.util.DataChecksum.Type.CRC32:
				{
					return new org.apache.hadoop.util.DataChecksum(type, newCrc32(), bytesPerChecksum
						);
				}

				case org.apache.hadoop.util.DataChecksum.Type.CRC32C:
				{
					return new org.apache.hadoop.util.DataChecksum(type, new org.apache.hadoop.util.PureJavaCrc32C
						(), bytesPerChecksum);
				}

				default:
				{
					return null;
				}
			}
		}

		/// <summary>Creates a DataChecksum from HEADER_LEN bytes from arr[offset].</summary>
		/// <returns>DataChecksum of the type in the array or null in case of an error.</returns>
		public static org.apache.hadoop.util.DataChecksum newDataChecksum(byte[] bytes, int
			 offset)
		{
			if (offset < 0 || bytes.Length < offset + getChecksumHeaderSize())
			{
				return null;
			}
			// like readInt():
			int bytesPerChecksum = ((bytes[offset + 1] & unchecked((int)(0xff))) << 24) | ((bytes
				[offset + 2] & unchecked((int)(0xff))) << 16) | ((bytes[offset + 3] & unchecked(
				(int)(0xff))) << 8) | ((bytes[offset + 4] & unchecked((int)(0xff))));
			return newDataChecksum(org.apache.hadoop.util.DataChecksum.Type.valueOf(bytes[offset
				]), bytesPerChecksum);
		}

		/// <summary>
		/// This constructs a DataChecksum by reading HEADER_LEN bytes from input
		/// stream <i>in</i>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.util.DataChecksum newDataChecksum(java.io.DataInputStream
			 @in)
		{
			int type = @in.readByte();
			int bpc = @in.readInt();
			org.apache.hadoop.util.DataChecksum summer = newDataChecksum(org.apache.hadoop.util.DataChecksum.Type
				.valueOf(type), bpc);
			if (summer == null)
			{
				throw new System.IO.IOException("Could not create DataChecksum of type " + type +
					 " with bytesPerChecksum " + bpc);
			}
			return summer;
		}

		/// <summary>Writes the checksum header to the output stream <i>out</i>.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void writeHeader(java.io.DataOutputStream @out)
		{
			@out.writeByte(type.id);
			@out.writeInt(bytesPerChecksum);
		}

		public virtual byte[] getHeader()
		{
			byte[] header = new byte[getChecksumHeaderSize()];
			header[0] = unchecked((byte)(type.id & unchecked((int)(0xff))));
			// Writing in buffer just like DataOutput.WriteInt()
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
		public virtual int writeValue(java.io.DataOutputStream @out, bool reset)
		{
			if (type.size <= 0)
			{
				return 0;
			}
			if (type.size == 4)
			{
				@out.writeInt((int)summer.getValue());
			}
			else
			{
				throw new System.IO.IOException("Unknown Checksum " + type);
			}
			if (reset)
			{
				reset();
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
		public virtual int writeValue(byte[] buf, int offset, bool reset)
		{
			if (type.size <= 0)
			{
				return 0;
			}
			if (type.size == 4)
			{
				int checksum = (int)summer.getValue();
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
				throw new System.IO.IOException("Unknown Checksum " + type);
			}
			if (reset)
			{
				reset();
			}
			return type.size;
		}

		/// <summary>Compares the checksum located at buf[offset] with the current checksum.</summary>
		/// <returns>true if the checksum matches and false otherwise.</returns>
		public virtual bool compare(byte[] buf, int offset)
		{
			if (type.size == 4)
			{
				int checksum = ((buf[offset + 0] & unchecked((int)(0xff))) << 24) | ((buf[offset 
					+ 1] & unchecked((int)(0xff))) << 16) | ((buf[offset + 2] & unchecked((int)(0xff
					))) << 8) | ((buf[offset + 3] & unchecked((int)(0xff))));
				return checksum == (int)summer.getValue();
			}
			return type.size == 0;
		}

		private readonly org.apache.hadoop.util.DataChecksum.Type type;

		private readonly java.util.zip.Checksum summer;

		private readonly int bytesPerChecksum;

		private int inSum = 0;

		private DataChecksum(org.apache.hadoop.util.DataChecksum.Type type, java.util.zip.Checksum
			 checksum, int chunkSize)
		{
			this.type = type;
			summer = checksum;
			bytesPerChecksum = chunkSize;
		}

		/// <returns>the checksum algorithm type.</returns>
		public virtual org.apache.hadoop.util.DataChecksum.Type getChecksumType()
		{
			return type;
		}

		/// <returns>the size for a checksum.</returns>
		public virtual int getChecksumSize()
		{
			return type.size;
		}

		/// <returns>the required checksum size given the data length.</returns>
		public virtual int getChecksumSize(int dataSize)
		{
			return ((dataSize - 1) / getBytesPerChecksum() + 1) * getChecksumSize();
		}

		public virtual int getBytesPerChecksum()
		{
			return bytesPerChecksum;
		}

		public virtual int getNumBytesInSum()
		{
			return inSum;
		}

		public const int SIZE_OF_INTEGER = int.SIZE / byte.SIZE;

		public static int getChecksumHeaderSize()
		{
			return 1 + SIZE_OF_INTEGER;
		}

		// type byte, bytesPerChecksum int
		//Checksum Interface. Just a wrapper around member summer.
		public virtual long getValue()
		{
			return summer.getValue();
		}

		public virtual void reset()
		{
			summer.reset();
			inSum = 0;
		}

		public virtual void update(byte[] b, int off, int len)
		{
			if (len > 0)
			{
				summer.update(b, off, len);
				inSum += len;
			}
		}

		public virtual void update(int b)
		{
			summer.update(b);
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
		/// <exception cref="org.apache.hadoop.fs.ChecksumException">if the checksums do not match
		/// 	</exception>
		public virtual void verifyChunkedSums(java.nio.ByteBuffer data, java.nio.ByteBuffer
			 checksums, string fileName, long basePos)
		{
			if (type.size == 0)
			{
				return;
			}
			if (data.hasArray() && checksums.hasArray())
			{
				verifyChunkedSums(((byte[])data.array()), data.arrayOffset() + data.position(), data
					.remaining(), ((byte[])checksums.array()), checksums.arrayOffset() + checksums.position
					(), fileName, basePos);
				return;
			}
			if (org.apache.hadoop.util.NativeCrc32.isAvailable())
			{
				org.apache.hadoop.util.NativeCrc32.verifyChunkedSums(bytesPerChecksum, type.id, checksums
					, data, fileName, basePos);
				return;
			}
			int startDataPos = data.position();
			data.mark();
			checksums.mark();
			try
			{
				byte[] buf = new byte[bytesPerChecksum];
				byte[] sum = new byte[type.size];
				while (data.remaining() > 0)
				{
					int n = System.Math.min(data.remaining(), bytesPerChecksum);
					checksums.get(sum);
					data.get(buf, 0, n);
					summer.reset();
					summer.update(buf, 0, n);
					int calculated = (int)summer.getValue();
					int stored = (sum[0] << 24 & unchecked((int)(0xff000000))) | (sum[1] << 16 & unchecked(
						(int)(0xff0000))) | (sum[2] << 8 & unchecked((int)(0xff00))) | sum[3] & unchecked(
						(int)(0xff));
					if (calculated != stored)
					{
						long errPos = basePos + data.position() - startDataPos - n;
						throw new org.apache.hadoop.fs.ChecksumException("Checksum error: " + fileName + 
							" at " + errPos + " exp: " + stored + " got: " + calculated, errPos);
					}
				}
			}
			finally
			{
				data.reset();
				checksums.reset();
			}
		}

		/// <summary>Implementation of chunked verification specifically on byte arrays.</summary>
		/// <remarks>
		/// Implementation of chunked verification specifically on byte arrays. This
		/// is to avoid the copy when dealing with ByteBuffers that have array backing.
		/// </remarks>
		/// <exception cref="org.apache.hadoop.fs.ChecksumException"/>
		private void verifyChunkedSums(byte[] data, int dataOff, int dataLen, byte[] checksums
			, int checksumsOff, string fileName, long basePos)
		{
			if (type.size == 0)
			{
				return;
			}
			if (org.apache.hadoop.util.NativeCrc32.isAvailable())
			{
				org.apache.hadoop.util.NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum, type
					.id, checksums, checksumsOff, data, dataOff, dataLen, fileName, basePos);
				return;
			}
			int remaining = dataLen;
			int dataPos = 0;
			while (remaining > 0)
			{
				int n = System.Math.min(remaining, bytesPerChecksum);
				summer.reset();
				summer.update(data, dataOff + dataPos, n);
				dataPos += n;
				remaining -= n;
				int calculated = (int)summer.getValue();
				int stored = (checksums[checksumsOff] << 24 & unchecked((int)(0xff000000))) | (checksums
					[checksumsOff + 1] << 16 & unchecked((int)(0xff0000))) | (checksums[checksumsOff
					 + 2] << 8 & unchecked((int)(0xff00))) | checksums[checksumsOff + 3] & unchecked(
					(int)(0xff));
				checksumsOff += 4;
				if (calculated != stored)
				{
					long errPos = basePos + dataPos - n;
					throw new org.apache.hadoop.fs.ChecksumException("Checksum error: " + fileName + 
						" at " + errPos + " exp: " + stored + " got: " + calculated, errPos);
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
		public virtual void calculateChunkedSums(java.nio.ByteBuffer data, java.nio.ByteBuffer
			 checksums)
		{
			if (type.size == 0)
			{
				return;
			}
			if (data.hasArray() && checksums.hasArray())
			{
				calculateChunkedSums(((byte[])data.array()), data.arrayOffset() + data.position()
					, data.remaining(), ((byte[])checksums.array()), checksums.arrayOffset() + checksums
					.position());
				return;
			}
			if (org.apache.hadoop.util.NativeCrc32.isAvailable())
			{
				org.apache.hadoop.util.NativeCrc32.calculateChunkedSums(bytesPerChecksum, type.id
					, checksums, data);
				return;
			}
			data.mark();
			checksums.mark();
			try
			{
				byte[] buf = new byte[bytesPerChecksum];
				while (data.remaining() > 0)
				{
					int n = System.Math.min(data.remaining(), bytesPerChecksum);
					data.get(buf, 0, n);
					summer.reset();
					summer.update(buf, 0, n);
					checksums.putInt((int)summer.getValue());
				}
			}
			finally
			{
				data.reset();
				checksums.reset();
			}
		}

		/// <summary>Implementation of chunked calculation specifically on byte arrays.</summary>
		/// <remarks>
		/// Implementation of chunked calculation specifically on byte arrays. This
		/// is to avoid the copy when dealing with ByteBuffers that have array backing.
		/// </remarks>
		public virtual void calculateChunkedSums(byte[] data, int dataOffset, int dataLength
			, byte[] sums, int sumsOffset)
		{
			if (type.size == 0)
			{
				return;
			}
			if (org.apache.hadoop.util.NativeCrc32.isAvailable())
			{
				org.apache.hadoop.util.NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum
					, type.id, sums, sumsOffset, data, dataOffset, dataLength);
				return;
			}
			int remaining = dataLength;
			while (remaining > 0)
			{
				int n = System.Math.min(remaining, bytesPerChecksum);
				summer.reset();
				summer.update(data, dataOffset, n);
				dataOffset += n;
				remaining -= n;
				long calculated = summer.getValue();
				sums[sumsOffset++] = unchecked((byte)(calculated >> 24));
				sums[sumsOffset++] = unchecked((byte)(calculated >> 16));
				sums[sumsOffset++] = unchecked((byte)(calculated >> 8));
				sums[sumsOffset++] = unchecked((byte)(calculated));
			}
		}

		public override bool Equals(object other)
		{
			if (!(other is org.apache.hadoop.util.DataChecksum))
			{
				return false;
			}
			org.apache.hadoop.util.DataChecksum o = (org.apache.hadoop.util.DataChecksum)other;
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
		internal class ChecksumNull : java.util.zip.Checksum
		{
			public ChecksumNull()
			{
			}

			//Dummy interface
			public virtual long getValue()
			{
				return 0;
			}

			public virtual void reset()
			{
			}

			public virtual void update(byte[] b, int off, int len)
			{
			}

			public virtual void update(int b)
			{
			}
		}
	}
}
