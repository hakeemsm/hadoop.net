using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A mutable long counter</summary>
	public class MutableCounterLong : MutableCounter
	{
		private AtomicLong value = new AtomicLong();

		internal MutableCounterLong(MetricsInfo info, long initValue)
			: base(info)
		{
			this.value.Set(initValue);
		}

		public override void Incr()
		{
			Incr(1);
		}

		/// <summary>Increment the value by a delta</summary>
		/// <param name="delta">of the increment</param>
		public virtual void Incr(long delta)
		{
			value.AddAndGet(delta);
			SetChanged();
		}

		public virtual long Value()
		{
			return value.Get();
		}

		public override void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			if (all || Changed())
			{
				builder.AddCounter(Info(), Value());
				ClearChanged();
			}
		}
	}
}
