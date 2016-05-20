using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>A mutable int gauge</summary>
	public class MutableGaugeInt : org.apache.hadoop.metrics2.lib.MutableGauge
	{
		private java.util.concurrent.atomic.AtomicInteger value = new java.util.concurrent.atomic.AtomicInteger
			();

		internal MutableGaugeInt(org.apache.hadoop.metrics2.MetricsInfo info, int initValue
			)
			: base(info)
		{
			this.value.set(initValue);
		}

		public virtual int value()
		{
			return value.get();
		}

		public override void incr()
		{
			incr(1);
		}

		/// <summary>Increment by delta</summary>
		/// <param name="delta">of the increment</param>
		public virtual void incr(int delta)
		{
			value.addAndGet(delta);
			setChanged();
		}

		public override void decr()
		{
			decr(1);
		}

		/// <summary>decrement by delta</summary>
		/// <param name="delta">of the decrement</param>
		public virtual void decr(int delta)
		{
			value.addAndGet(-delta);
			setChanged();
		}

		/// <summary>Set the value of the metric</summary>
		/// <param name="value">to set</param>
		public virtual void set(int value)
		{
			this.value.set(value);
			setChanged();
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			if (all || changed())
			{
				builder.addGauge(info(), value());
				clearChanged();
			}
		}
	}
}
