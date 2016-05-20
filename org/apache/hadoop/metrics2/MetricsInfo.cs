using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>Interface to provide immutable meta info for metrics</summary>
	public interface MetricsInfo
	{
		/// <returns>the name of the metric/tag</returns>
		string name();

		/// <returns>the description of the metric/tag</returns>
		string description();
	}
}
