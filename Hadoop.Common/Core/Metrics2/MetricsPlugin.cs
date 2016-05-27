using Org.Apache.Commons.Configuration;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The plugin interface for the metrics framework</summary>
	public interface MetricsPlugin
	{
		/// <summary>Initialize the plugin</summary>
		/// <param name="conf">the configuration object for the plugin</param>
		void Init(SubsetConfiguration conf);
	}
}
