using Com.Google.Common.Base;


namespace Org.Apache.Hadoop.Util
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
		private const int StatusClosedMask = 1 << 30;

		/// <summary>The status bits.</summary>
		/// <remarks>
		/// The status bits.
		/// Bit 30: 0 = open, 1 = closed.
		/// Bits 29 to 0: the reference count.
		/// </remarks>
		private readonly AtomicInteger status = new AtomicInteger(0);

		public CloseableReferenceCount()
		{
		}

		/// <summary>Increment the reference count.</summary>
		/// <exception cref="ClosedChannelException">If the status is closed.</exception>
		public virtual void Reference()
		{
			int curBits = status.IncrementAndGet();
			if ((curBits & StatusClosedMask) != 0)
			{
				status.DecrementAndGet();
				throw new ClosedChannelException();
			}
		}

		/// <summary>Decrement the reference count.</summary>
		/// <returns>
		/// True if the object is closed and has no outstanding
		/// references.
		/// </returns>
		public virtual bool Unreference()
		{
			int newVal = status.DecrementAndGet();
			Preconditions.CheckState(newVal != unchecked((int)(0xffffffff)), "called unreference when the reference count was already at 0."
				);
			return newVal == StatusClosedMask;
		}

		/// <summary>
		/// Decrement the reference count, checking to make sure that the
		/// CloseableReferenceCount is not closed.
		/// </summary>
		/// <exception cref="AsynchronousCloseException">If the status is closed.</exception>
		/// <exception cref="ClosedChannelException"/>
		public virtual void UnreferenceCheckClosed()
		{
			int newVal = status.DecrementAndGet();
			if ((newVal & StatusClosedMask) != 0)
			{
				throw new AsynchronousCloseException();
			}
		}

		/// <summary>Return true if the status is currently open.</summary>
		/// <returns>True if the status is currently open.</returns>
		public virtual bool IsOpen()
		{
			return ((status.Get() & StatusClosedMask) == 0);
		}

		/// <summary>Mark the status as closed.</summary>
		/// <remarks>
		/// Mark the status as closed.
		/// Once the status is closed, it cannot be reopened.
		/// </remarks>
		/// <returns>The current reference count.</returns>
		/// <exception cref="ClosedChannelException">
		/// If someone else closes the object
		/// before we do.
		/// </exception>
		public virtual int SetClosed()
		{
			while (true)
			{
				int curBits = status.Get();
				if ((curBits & StatusClosedMask) != 0)
				{
					throw new ClosedChannelException();
				}
				if (status.CompareAndSet(curBits, curBits | StatusClosedMask))
				{
					return curBits & (~StatusClosedMask);
				}
			}
		}

		/// <summary>Get the current reference count.</summary>
		/// <returns>The current reference count.</returns>
		public virtual int GetReferenceCount()
		{
			return status.Get() & (~StatusClosedMask);
		}
	}
}
