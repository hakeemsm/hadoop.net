using System;
using Com.Google.Common.Annotations;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Wrapper around JNI support code to do checksum computation
	/// natively.
	/// </summary>
	internal class NativeCrc32
	{
		/// <summary>Return true if the JNI-based native CRC extensions are available.</summary>
		public static bool IsAvailable()
		{
			return NativeCodeLoader.IsNativeCodeLoaded();
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
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException">if there is an invalid checksum
		/// 	</exception>
		public static void VerifyChunkedSums(int bytesPerSum, int checksumType, ByteBuffer
			 sums, ByteBuffer data, string fileName, long basePos)
		{
			NativeComputeChunkedSums(bytesPerSum, checksumType, sums, sums.Position(), data, 
				data.Position(), data.Remaining(), fileName, basePos, true);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		public static void VerifyChunkedSumsByteArray(int bytesPerSum, int checksumType, 
			byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength, string
			 fileName, long basePos)
		{
			NativeComputeChunkedSumsByteArray(bytesPerSum, checksumType, sums, sumsOffset, data
				, dataOffset, dataLength, fileName, basePos, true);
		}

		public static void CalculateChunkedSums(int bytesPerSum, int checksumType, ByteBuffer
			 sums, ByteBuffer data)
		{
			NativeComputeChunkedSums(bytesPerSum, checksumType, sums, sums.Position(), data, 
				data.Position(), data.Remaining(), string.Empty, 0, false);
		}

		public static void CalculateChunkedSumsByteArray(int bytesPerSum, int checksumType
			, byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength)
		{
			NativeComputeChunkedSumsByteArray(bytesPerSum, checksumType, sums, sumsOffset, data
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
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException">if there is an invalid checksum
		/// 	</exception>
		[VisibleForTesting]
		[System.ObsoleteAttribute(@"use NativeComputeChunkedSums(int, int, ByteBuffer, int, ByteBuffer, int, int, string, long, bool) instead"
			)]
		internal static void NativeVerifyChunkedSums(int bytesPerSum, int checksumType, ByteBuffer
			 sums, int sumsOffset, ByteBuffer data, int dataOffset, int dataLength, string fileName
			, long basePos)
		{
		}

		private static void NativeComputeChunkedSums(int bytesPerSum, int checksumType, ByteBuffer
			 sums, int sumsOffset, ByteBuffer data, int dataOffset, int dataLength, string fileName
			, long basePos, bool verify)
		{
		}

		private static void NativeComputeChunkedSumsByteArray(int bytesPerSum, int checksumType
			, byte[] sums, int sumsOffset, byte[] data, int dataOffset, int dataLength, string
			 fileName, long basePos, bool verify)
		{
		}

		public const int ChecksumCrc32 = DataChecksum.ChecksumCrc32;

		public const int ChecksumCrc32c = DataChecksum.ChecksumCrc32c;
		// Copy the constants over from DataChecksum so that javah will pick them up
		// and make them available in the native code header.
	}
}
