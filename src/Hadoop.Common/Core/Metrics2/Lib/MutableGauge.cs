using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>The mutable gauge metric interface</summary>
	public abstract class MutableGauge : MutableMetric
	{
		private readonly MetricsInfo info;

		protected internal MutableGauge(MetricsInfo info)
		{
			this.info = Preconditions.CheckNotNull(info, "metric info");
		}

		protected internal virtual MetricsInfo Info()
		{
			return info;
		}

		/// <summary>Increment the value of the metric by 1</summary>
		public abstract void Incr();

		/// <summary>Decrement the value of the metric by 1</summary>
		public abstract void Decr();
	}
}
