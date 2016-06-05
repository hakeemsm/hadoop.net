

namespace Org.Apache.Hadoop.IO
{
	public interface ByteBufferPool
	{
		/// <summary>Get a new direct ByteBuffer.</summary>
		/// <remarks>
		/// Get a new direct ByteBuffer.  The pool can provide this from
		/// removing a buffer from its internal cache, or by allocating a
		/// new buffer.
		/// </remarks>
		/// <param name="direct">Whether the buffer should be direct.</param>
		/// <param name="length">The minimum length the buffer will have.</param>
		/// <returns>
		/// A new ByteBuffer.  This ByteBuffer must be direct.
		/// Its capacity can be less than what was requested, but
		/// must be at least 1 byte.
		/// </returns>
		ByteBuffer GetBuffer(bool direct, int length);

		/// <summary>Release a buffer back to the pool.</summary>
		/// <remarks>
		/// Release a buffer back to the pool.
		/// The pool may choose to put this buffer into its cache.
		/// </remarks>
		/// <param name="buffer">a direct bytebuffer</param>
		void PutBuffer(ByteBuffer buffer);
	}
}
