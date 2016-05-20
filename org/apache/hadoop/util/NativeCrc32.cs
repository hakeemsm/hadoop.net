using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Wrapper around JNI support code to do checksum computation
	/// natively.
	/// </summary>
	internal class NativeCrc32
	{
		/// <summary>Return true if the JNI-based native CRC extensions are available.</summary>
		public static bool isAvailable()
		{
			return org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
		}

		/// <summary>
		/// Verify the given buffers of data and checksums, and throw an exception
		/// if any checksum is invalid.
		/// </summary>
		/// <remarks>
		/// Verify the given buffers of data and checksums, and throw an exception
		/// if any checksum is invalid. The buffers given to this function should
		/// have their position initially at the start of the data, and their limit
		/// set at the end of the data. The position, limit, and mark are not
		/// modified.
		/// </remarks>
		/// <param name="bytesPerSum">the chunk size (eg 512 bytes)</param>
		/// <param name="checksumType">the DataChecksum type constant (NULL is not supported)
		/// 	</param>
		/// <param name="sums">
		/// the DirectByteBuffer pointing at the beginning of the
		/// stored checksums
		/// </param>
		/// <param name="data">
		/// the DirectByteBuffer pointing at the beginning of the
		/// data to check
		/// </param>
		/// <param name="basePos">the position in the file where the data buffer starts</param>
		/// <param name="fileName">the name of the file being verified</param>
		/// <exception cref="org.apache.hadoop.fs.ChecksumException">if there is an invalid checksum
		/// 	</exception>
		public static void verifyChunkedSums(int bytesPerSum, int checksumType, java.nio.ByteBuffer
			 sums, java.nio.ByteBuffer data, string fileName, long basePos)
		{
			nativeComputeChunkedSums(bytesPerSum, checksumType, sums, sums.position(), data, 
				data.position(), data.remaining(), fileName, basePos, true);
		}

		/// <exception cref="org.apache.hadoop.fs.ChecksumException"/>
		public static void verifyChunkedSumsByteArray(int bytesPerSum, int checksumType, 
			byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength, string
			 fileName, long basePos)
		{
			nativeComputeChunkedSumsByteArray(bytesPerSum, checksumType, sums, sumsOffset, data
				, dataOffset, dataLength, fileName, basePos, true);
		}

		public static void calculateChunkedSums(int bytesPerSum, int checksumType, java.nio.ByteBuffer
			 sums, java.nio.ByteBuffer data)
		{
			nativeComputeChunkedSums(bytesPerSum, checksumType, sums, sums.position(), data, 
				data.position(), data.remaining(), string.Empty, 0, false);
		}

		public static void calculateChunkedSumsByteArray(int bytesPerSum, int checksumType
			, byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength)
		{
			nativeComputeChunkedSumsByteArray(bytesPerSum, checksumType, sums, sumsOffset, data
				, dataOffset, dataLength, string.Empty, 0, false);
		}

		/// <summary>
		/// Verify the given buffers of data and checksums, and throw an exception
		/// if any checksum is invalid.
		/// </summary>
		/// <remarks>
		/// Verify the given buffers of data and checksums, and throw an exception
		/// if any checksum is invalid. The buffers given to this function should
		/// have their position initially at the start of the data, and their limit
		/// set at the end of the data. The position, limit, and mark are not
		/// modified.  This method is retained only for backwards-compatibility with
		/// prior jar versions that need the corresponding JNI function.
		/// </remarks>
		/// <param name="bytesPerSum">the chunk size (eg 512 bytes)</param>
		/// <param name="checksumType">the DataChecksum type constant</param>
		/// <param name="sums">
		/// the DirectByteBuffer pointing at the beginning of the
		/// stored checksums
		/// </param>
		/// <param name="sumsOffset">start offset in sums buffer</param>
		/// <param name="data">
		/// the DirectByteBuffer pointing at the beginning of the
		/// data to check
		/// </param>
		/// <param name="dataOffset">start offset in data buffer</param>
		/// <param name="dataLength">length of data buffer</param>
		/// <param name="fileName">the name of the file being verified</param>
		/// <param name="basePos">the position in the file where the data buffer starts</param>
		/// <exception cref="org.apache.hadoop.fs.ChecksumException">if there is an invalid checksum
		/// 	</exception>
		[com.google.common.annotations.VisibleForTesting]
		[System.ObsoleteAttribute(@"use nativeComputeChunkedSums(int, int, java.nio.ByteBuffer, int, java.nio.ByteBuffer, int, int, string, long, bool) instead"
			)]
		internal static void nativeVerifyChunkedSums(int bytesPerSum, int checksumType, java.nio.ByteBuffer
			 sums, int sumsOffset, java.nio.ByteBuffer data, int dataOffset, int dataLength, 
			string fileName, long basePos)
		{
		}

		private static void nativeComputeChunkedSums(int bytesPerSum, int checksumType, java.nio.ByteBuffer
			 sums, int sumsOffset, java.nio.ByteBuffer data, int dataOffset, int dataLength, 
			string fileName, long basePos, bool verify)
		{
		}

		private static void nativeComputeChunkedSumsByteArray(int bytesPerSum, int checksumType
			, byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength, string
			 fileName, long basePos, bool verify)
		{
		}

		public const int CHECKSUM_CRC32 = org.apache.hadoop.util.DataChecksum.CHECKSUM_CRC32;

		public const int CHECKSUM_CRC32C = org.apache.hadoop.util.DataChecksum.CHECKSUM_CRC32C;
		// Copy the constants over from DataChecksum so that javah will pick them up
		// and make them available in the native code header.
	}
}
