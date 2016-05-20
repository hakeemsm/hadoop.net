using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>The mutable counter (monotonically increasing) metric interface</summary>
	public abstract class MutableCounter : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		protected internal MutableCounter(org.apache.hadoop.metrics2.MetricsInfo info)
		{
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "counter info"
				);
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		/// <summary>Increment the metric value by 1.</summary>
		public abstract void incr();
	}
}
