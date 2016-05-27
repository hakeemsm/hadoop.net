using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>Interface to provide immutable meta info for metrics</summary>
	public interface MetricsInfo
	{
		/// <returns>the name of the metric/tag</returns>
		string Name();

		/// <returns>the description of the metric/tag</returns>
		string Description();
	}
}
