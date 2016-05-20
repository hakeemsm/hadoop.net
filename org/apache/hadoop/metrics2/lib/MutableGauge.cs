using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>The mutable gauge metric interface</summary>
	public abstract class MutableGauge : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		protected internal MutableGauge(org.apache.hadoop.metrics2.MetricsInfo info)
		{
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "metric info"
				);
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		/// <summary>Increment the value of the metric by 1</summary>
		public abstract void incr();

		/// <summary>Decrement the value of the metric by 1</summary>
		public abstract void decr();
	}
}
