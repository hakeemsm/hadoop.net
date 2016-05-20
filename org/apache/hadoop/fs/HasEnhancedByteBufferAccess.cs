using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// FSDataInputStreams implement this interface to provide enhanced
	/// byte buffer access.
	/// </summary>
	/// <remarks>
	/// FSDataInputStreams implement this interface to provide enhanced
	/// byte buffer access.  Usually this takes the form of mmap support.
	/// </remarks>
	public interface HasEnhancedByteBufferAccess
	{
		/// <summary>Get a ByteBuffer containing file data.</summary>
		/// <remarks>
		/// Get a ByteBuffer containing file data.
		/// This ByteBuffer may come from the stream itself, via a call like mmap,
		/// or it may come from the ByteBufferFactory which is passed in as an
		/// argument.
		/// </remarks>
		/// <param name="factory">
		/// If this is non-null, it will be used to create a fallback
		/// ByteBuffer when the stream itself cannot create one.
		/// </param>
		/// <param name="maxLength">
		/// The maximum length of buffer to return.  We may return a buffer
		/// which is shorter than this.
		/// </param>
		/// <param name="opts">Options to use when reading.</param>
		/// <returns>
		/// We will always return an empty buffer if maxLength was 0,
		/// whether or not we are at EOF.
		/// If maxLength &gt; 0, we will return null if the stream has
		/// reached EOF.
		/// Otherwise, we will return a ByteBuffer containing at least one
		/// byte.  You must free this ByteBuffer when you are done with it
		/// by calling releaseBuffer on it.  The buffer will continue to be
		/// readable until it is released in this manner.  However, the
		/// input stream's close method may warn about unclosed buffers.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// : if there was an error reading.
		/// UnsupportedOperationException: if factory was null, and we
		/// needed an external byte buffer.  UnsupportedOperationException
		/// will never be thrown unless the factory argument is null.
		/// </exception>
		/// <exception cref="System.NotSupportedException"/>
		java.nio.ByteBuffer read(org.apache.hadoop.io.ByteBufferPool factory, int maxLength
			, java.util.EnumSet<org.apache.hadoop.fs.ReadOption> opts);

		/// <summary>
		/// Release a ByteBuffer which was created by the enhanced ByteBuffer read
		/// function.
		/// </summary>
		/// <remarks>
		/// Release a ByteBuffer which was created by the enhanced ByteBuffer read
		/// function. You must not continue using the ByteBuffer after calling this
		/// function.
		/// </remarks>
		/// <param name="buffer">The ByteBuffer to release.</param>
		void releaseBuffer(java.nio.ByteBuffer buffer);
	}
}
