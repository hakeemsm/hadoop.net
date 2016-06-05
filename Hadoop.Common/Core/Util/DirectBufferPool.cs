using Com.Google.Common.Annotations;


namespace Org.Apache.Hadoop.Util
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
		internal readonly ConcurrentMap<int, Queue<WeakReference<ByteBuffer>>> buffersBySize
			 = new ConcurrentHashMap<int, Queue<WeakReference<ByteBuffer>>>();

		// Essentially implement a multimap with weak values.
		/// <summary>Allocate a direct buffer of the specified size, in bytes.</summary>
		/// <remarks>
		/// Allocate a direct buffer of the specified size, in bytes.
		/// If a pooled buffer is available, returns that. Otherwise
		/// allocates a new one.
		/// </remarks>
		public virtual ByteBuffer GetBuffer(int size)
		{
			Queue<WeakReference<ByteBuffer>> list = buffersBySize[size];
			if (list == null)
			{
				// no available buffers for this size
				return ByteBuffer.AllocateDirect(size);
			}
			WeakReference<ByteBuffer> @ref;
			while ((@ref = list.Poll()) != null)
			{
				ByteBuffer b = @ref.Get();
				if (b != null)
				{
					return b;
				}
			}
			return ByteBuffer.AllocateDirect(size);
		}

		/// <summary>Return a buffer into the pool.</summary>
		/// <remarks>
		/// Return a buffer into the pool. After being returned,
		/// the buffer may be recycled, so the user must not
		/// continue to use it in any way.
		/// </remarks>
		/// <param name="buf">the buffer to return</param>
		public virtual void ReturnBuffer(ByteBuffer buf)
		{
			buf.Clear();
			// reset mark, limit, etc
			int size = buf.Capacity();
			Queue<WeakReference<ByteBuffer>> list = buffersBySize[size];
			if (list == null)
			{
				list = new ConcurrentLinkedQueue<WeakReference<ByteBuffer>>();
				Queue<WeakReference<ByteBuffer>> prev = buffersBySize.PutIfAbsent(size, list);
				// someone else put a queue in the map before we did
				if (prev != null)
				{
					list = prev;
				}
			}
			list.AddItem(new WeakReference<ByteBuffer>(buf));
		}

		/// <summary>Return the number of available buffers of a given size.</summary>
		/// <remarks>
		/// Return the number of available buffers of a given size.
		/// This is used only for tests.
		/// </remarks>
		[VisibleForTesting]
		internal virtual int CountBuffersOfSize(int size)
		{
			Queue<WeakReference<ByteBuffer>> list = buffersBySize[size];
			if (list == null)
			{
				return 0;
			}
			return list.Count;
		}
	}
}
