using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The plugin interface for the metrics framework</summary>
	public interface MetricsPlugin
	{
		/// <summary>Initialize the plugin</summary>
		/// <param name="conf">the configuration object for the plugin</param>
		void init(org.apache.commons.configuration.SubsetConfiguration conf);
	}
}
