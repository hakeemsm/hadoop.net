using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>The mutable counter (monotonically increasing) metric interface</summary>
	public abstract class MutableCounter : MutableMetric
	{
		private readonly MetricsInfo info;

		protected internal MutableCounter(MetricsInfo info)
		{
			this.info = Preconditions.CheckNotNull(info, "counter info");
		}

		protected internal virtual MetricsInfo Info()
		{
			return info;
		}

		/// <summary>Increment the metric value by 1.</summary>
		public abstract void Incr();
	}
}
