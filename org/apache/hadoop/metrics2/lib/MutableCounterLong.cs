using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>A mutable long counter</summary>
	public class MutableCounterLong : org.apache.hadoop.metrics2.lib.MutableCounter
	{
		private java.util.concurrent.atomic.AtomicLong value = new java.util.concurrent.atomic.AtomicLong
			();

		internal MutableCounterLong(org.apache.hadoop.metrics2.MetricsInfo info, long initValue
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
		public virtual void incr(long delta)
		{
			value.addAndGet(delta);
			setChanged();
		}

		public virtual long value()
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
