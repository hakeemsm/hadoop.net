using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A mutable int counter for implementing metrics sources</summary>
	public class MutableCounterInt : MutableCounter
	{
		private AtomicInteger value = new AtomicInteger();

		internal MutableCounterInt(MetricsInfo info, int initValue)
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
		public virtual void Incr(int delta)
		{
			lock (this)
			{
				value.AddAndGet(delta);
				SetChanged();
			}
		}

		public virtual int Value()
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
