using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A simple class for pooling direct ByteBuffers.</summary>
	/// <remarks>
	/// A simple class for pooling direct ByteBuffers. This is necessary
	/// because Direct Byte Buffers do not take up much space on the heap,
	/// and hence will not trigger GCs on their own. However, they do take
	/// native memory, and thus can cause high memory usage if not pooled.
	/// The pooled instances are referred to only via weak references, allowing
	/// them to be collected when a GC does run.
	/// This class only does effective pooling when many buffers will be
	/// allocated at the same size. There is no attempt to reuse larger
	/// buffers to satisfy smaller allocations.
	/// </remarks>
	public class DirectBufferPool
	{
		internal readonly java.util.concurrent.ConcurrentMap<int, java.util.Queue<java.lang.@ref.WeakReference
			<java.nio.ByteBuffer>>> buffersBySize = new java.util.concurrent.ConcurrentHashMap
			<int, java.util.Queue<java.lang.@ref.WeakReference<java.nio.ByteBuffer>>>();

		// Essentially implement a multimap with weak values.
		/// <summary>Allocate a direct buffer of the specified size, in bytes.</summary>
		/// <remarks>
		/// Allocate a direct buffer of the specified size, in bytes.
		/// If a pooled buffer is available, returns that. Otherwise
		/// allocates a new one.
		/// </remarks>
		public virtual java.nio.ByteBuffer getBuffer(int size)
		{
			java.util.Queue<java.lang.@ref.WeakReference<java.nio.ByteBuffer>> list = buffersBySize
				[size];
			if (list == null)
			{
				// no available buffers for this size
				return java.nio.ByteBuffer.allocateDirect(size);
			}
			java.lang.@ref.WeakReference<java.nio.ByteBuffer> @ref;
			while ((@ref = list.poll()) != null)
			{
				java.nio.ByteBuffer b = @ref.get();
				if (b != null)
				{
					return b;
				}
			}
			return java.nio.ByteBuffer.allocateDirect(size);
		}

		/// <summary>Return a buffer into the pool.</summary>
		/// <remarks>
		/// Return a buffer into the pool. After being returned,
		/// the buffer may be recycled, so the user must not
		/// continue to use it in any way.
		/// </remarks>
		/// <param name="buf">the buffer to return</param>
		public virtual void returnBuffer(java.nio.ByteBuffer buf)
		{
			buf.clear();
			// reset mark, limit, etc
			int size = buf.capacity();
			java.util.Queue<java.lang.@ref.WeakReference<java.nio.ByteBuffer>> list = buffersBySize
				[size];
			if (list == null)
			{
				list = new java.util.concurrent.ConcurrentLinkedQueue<java.lang.@ref.WeakReference
					<java.nio.ByteBuffer>>();
				java.util.Queue<java.lang.@ref.WeakReference<java.nio.ByteBuffer>> prev = buffersBySize
					.putIfAbsent(size, list);
				// someone else put a queue in the map before we did
				if (prev != null)
				{
					list = prev;
				}
			}
			list.add(new java.lang.@ref.WeakReference<java.nio.ByteBuffer>(buf));
		}

		/// <summary>Return the number of available buffers of a given size.</summary>
		/// <remarks>
		/// Return the number of available buffers of a given size.
		/// This is used only for tests.
		/// </remarks>
		[com.google.common.annotations.VisibleForTesting]
		internal virtual int countBuffersOfSize(int size)
		{
			java.util.Queue<java.lang.@ref.WeakReference<java.nio.ByteBuffer>> list = buffersBySize
				[size];
			if (list == null)
			{
				return 0;
			}
			return list.Count;
		}
	}
}
