

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics source interface</summary>
	public interface MetricsSource
	{
		/// <summary>Get metrics from the source</summary>
		/// <param name="collector">to contain the resulting metrics snapshot</param>
		/// <param name="all">if true, return all metrics even if unchanged.</param>
		void GetMetrics(MetricsCollector collector, bool all);
	}
}
