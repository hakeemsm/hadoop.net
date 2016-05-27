using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A mutable int gauge</summary>
	public class MutableGaugeInt : MutableGauge
	{
		private AtomicInteger value = new AtomicInteger();

		internal MutableGaugeInt(MetricsInfo info, int initValue)
			: base(info)
		{
			this.value.Set(initValue);
		}

		public virtual int Value()
		{
			return value.Get();
		}

		public override void Incr()
		{
			Incr(1);
		}

		/// <summary>Increment by delta</summary>
		/// <param name="delta">of the increment</param>
		public virtual void Incr(int delta)
		{
			value.AddAndGet(delta);
			SetChanged();
		}

		public override void Decr()
		{
			Decr(1);
		}

		/// <summary>decrement by delta</summary>
		/// <param name="delta">of the decrement</param>
		public virtual void Decr(int delta)
		{
			value.AddAndGet(-delta);
			SetChanged();
		}

		/// <summary>Set the value of the metric</summary>
		/// <param name="value">to set</param>
		public virtual void Set(int value)
		{
			this.value.Set(value);
			SetChanged();
		}

		public override void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			if (all || Changed())
			{
				builder.AddGauge(Info(), Value());
				ClearChanged();
			}
		}
	}
}
