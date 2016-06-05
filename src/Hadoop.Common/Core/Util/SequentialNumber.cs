using System;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>Sequential number generator.</summary>
	/// <remarks>
	/// Sequential number generator.
	/// This class is thread safe.
	/// </remarks>
	public abstract class SequentialNumber
	{
		private readonly AtomicLong currentValue;

		/// <summary>Create a new instance with the given initial value.</summary>
		protected internal SequentialNumber(long initialValue)
		{
			currentValue = new AtomicLong(initialValue);
		}

		/// <returns>the current value.</returns>
		public virtual long GetCurrentValue()
		{
			return currentValue.Get();
		}

		/// <summary>Set current value.</summary>
		public virtual void SetCurrentValue(long value)
		{
			currentValue.Set(value);
		}

		/// <summary>Increment and then return the next value.</summary>
		public virtual long NextValue()
		{
			return currentValue.IncrementAndGet();
		}

		/// <summary>Skip to the new value.</summary>
		/// <exception cref="System.InvalidOperationException"/>
		public virtual void SkipTo(long newValue)
		{
			for (; ; )
			{
				long c = GetCurrentValue();
				if (newValue < c)
				{
					throw new InvalidOperationException("Cannot skip to less than the current value (="
						 + c + "), where newValue=" + newValue);
				}
				if (currentValue.CompareAndSet(c, newValue))
				{
					return;
				}
			}
		}

		public override bool Equals(object that)
		{
			if (that == null || this.GetType() != that.GetType())
			{
				return false;
			}
			AtomicLong thatValue = ((Org.Apache.Hadoop.Util.SequentialNumber)that).currentValue;
			return currentValue.Equals(thatValue);
		}

		public override int GetHashCode()
		{
			long v = currentValue.Get();
			return (int)v ^ (int)((long)(((ulong)v) >> 32));
		}
	}
}
