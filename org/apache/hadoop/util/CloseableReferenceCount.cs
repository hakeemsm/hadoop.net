using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A closeable object that maintains a reference count.</summary>
	/// <remarks>
	/// A closeable object that maintains a reference count.
	/// Once the object is closed, attempting to take a new reference will throw
	/// ClosedChannelException.
	/// </remarks>
	public class CloseableReferenceCount
	{
		/// <summary>Bit mask representing a closed domain socket.</summary>
		private const int STATUS_CLOSED_MASK = 1 << 30;

		/// <summary>The status bits.</summary>
		/// <remarks>
		/// The status bits.
		/// Bit 30: 0 = open, 1 = closed.
		/// Bits 29 to 0: the reference count.
		/// </remarks>
		private readonly java.util.concurrent.atomic.AtomicInteger status = new java.util.concurrent.atomic.AtomicInteger
			(0);

		public CloseableReferenceCount()
		{
		}

		/// <summary>Increment the reference count.</summary>
		/// <exception cref="java.nio.channels.ClosedChannelException">If the status is closed.
		/// 	</exception>
		public virtual void reference()
		{
			int curBits = status.incrementAndGet();
			if ((curBits & STATUS_CLOSED_MASK) != 0)
			{
				status.decrementAndGet();
				throw new java.nio.channels.ClosedChannelException();
			}
		}

		/// <summary>Decrement the reference count.</summary>
		/// <returns>
		/// True if the object is closed and has no outstanding
		/// references.
		/// </returns>
		public virtual bool unreference()
		{
			int newVal = status.decrementAndGet();
			com.google.common.@base.Preconditions.checkState(newVal != unchecked((int)(0xffffffff
				)), "called unreference when the reference count was already at 0.");
			return newVal == STATUS_CLOSED_MASK;
		}

		/// <summary>
		/// Decrement the reference count, checking to make sure that the
		/// CloseableReferenceCount is not closed.
		/// </summary>
		/// <exception cref="java.nio.channels.AsynchronousCloseException">If the status is closed.
		/// 	</exception>
		/// <exception cref="java.nio.channels.ClosedChannelException"/>
		public virtual void unreferenceCheckClosed()
		{
			int newVal = status.decrementAndGet();
			if ((newVal & STATUS_CLOSED_MASK) != 0)
			{
				throw new java.nio.channels.AsynchronousCloseException();
			}
		}

		/// <summary>Return true if the status is currently open.</summary>
		/// <returns>True if the status is currently open.</returns>
		public virtual bool isOpen()
		{
			return ((status.get() & STATUS_CLOSED_MASK) == 0);
		}

		/// <summary>Mark the status as closed.</summary>
		/// <remarks>
		/// Mark the status as closed.
		/// Once the status is closed, it cannot be reopened.
		/// </remarks>
		/// <returns>The current reference count.</returns>
		/// <exception cref="java.nio.channels.ClosedChannelException">
		/// If someone else closes the object
		/// before we do.
		/// </exception>
		public virtual int setClosed()
		{
			while (true)
			{
				int curBits = status.get();
				if ((curBits & STATUS_CLOSED_MASK) != 0)
				{
					throw new java.nio.channels.ClosedChannelException();
				}
				if (status.compareAndSet(curBits, curBits | STATUS_CLOSED_MASK))
				{
					return curBits & (~STATUS_CLOSED_MASK);
				}
			}
		}

		/// <summary>Get the current reference count.</summary>
		/// <returns>The current reference count.</returns>
		public virtual int getReferenceCount()
		{
			return status.get() & (~STATUS_CLOSED_MASK);
		}
	}
}
