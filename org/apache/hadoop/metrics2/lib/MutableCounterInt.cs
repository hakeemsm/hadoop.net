using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>A mutable int counter for implementing metrics sources</summary>
	public class MutableCounterInt : org.apache.hadoop.metrics2.lib.MutableCounter
	{
		private java.util.concurrent.atomic.AtomicInteger value = new java.util.concurrent.atomic.AtomicInteger
			();

		internal MutableCounterInt(org.apache.hadoop.metrics2.MetricsInfo info, int initValue
			)
			: base(info)
		{
			this.value.set(initValue);
		}

		public override void incr()
		{
			incr(1);
		}

		/// <summary>Increment the value by a delta</summary>
		/// <param name="delta">of the increment</param>
		public virtual void incr(int delta)
		{
			lock (this)
			{
				value.addAndGet(delta);
				setChanged();
			}
		}

		public virtual int value()
		{
			return value.get();
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			if (all || changed())
			{
				builder.addCounter(info(), value());
				clearChanged();
			}
		}
	}
}
