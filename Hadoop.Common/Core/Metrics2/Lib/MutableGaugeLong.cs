using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A mutable long gauge</summary>
	public class MutableGaugeLong : MutableGauge
	{
		private AtomicLong value = new AtomicLong();

		internal MutableGaugeLong(MetricsInfo info, long initValue)
			: base(info)
		{
			this.value.Set(initValue);
		}

		public virtual long Value()
		{
			return value.Get();
		}

		public override void Incr()
		{
			Incr(1);
		}

		/// <summary>Increment by delta</summary>
		/// <param name="delta">of the increment</param>
		public virtual void Incr(long delta)
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
		public virtual void Decr(long delta)
		{
			value.AddAndGet(-delta);
			SetChanged();
		}

		/// <summary>Set the value of the metric</summary>
		/// <param name="value">to set</param>
		public virtual void Set(long value)
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
