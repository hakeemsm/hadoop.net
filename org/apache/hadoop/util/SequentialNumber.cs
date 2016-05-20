using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Sequential number generator.</summary>
	/// <remarks>
	/// Sequential number generator.
	/// This class is thread safe.
	/// </remarks>
	public abstract class SequentialNumber
	{
		private readonly java.util.concurrent.atomic.AtomicLong currentValue;

		/// <summary>Create a new instance with the given initial value.</summary>
		protected internal SequentialNumber(long initialValue)
		{
			currentValue = new java.util.concurrent.atomic.AtomicLong(initialValue);
		}

		/// <returns>the current value.</returns>
		public virtual long getCurrentValue()
		{
			return currentValue.get();
		}

		/// <summary>Set current value.</summary>
		public virtual void setCurrentValue(long value)
		{
			currentValue.set(value);
		}

		/// <summary>Increment and then return the next value.</summary>
		public virtual long nextValue()
		{
			return currentValue.incrementAndGet();
		}

		/// <summary>Skip to the new value.</summary>
		/// <exception cref="System.InvalidOperationException"/>
		public virtual void skipTo(long newValue)
		{
			for (; ; )
			{
				long c = getCurrentValue();
				if (newValue < c)
				{
					throw new System.InvalidOperationException("Cannot skip to less than the current value (="
						 + c + "), where newValue=" + newValue);
				}
				if (currentValue.compareAndSet(c, newValue))
				{
					return;
				}
			}
		}

		public override bool Equals(object that)
		{
			if (that == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
				(that))
			{
				return false;
			}
			java.util.concurrent.atomic.AtomicLong thatValue = ((org.apache.hadoop.util.SequentialNumber
				)that).currentValue;
			return currentValue.Equals(thatValue);
		}

		public override int GetHashCode()
		{
			long v = currentValue.get();
			return (int)v ^ (int)((long)(((ulong)v) >> 32));
		}
	}
}
