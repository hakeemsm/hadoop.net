using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The JMX interface to the metrics system</summary>
	public interface MetricsSystemMXBean
	{
		/// <summary>Start the metrics system</summary>
		/// <exception cref="MetricsException"/>
		void start();

		/// <summary>Stop the metrics system</summary>
		/// <exception cref="MetricsException"/>
		void stop();

		/// <summary>Start metrics MBeans</summary>
		/// <exception cref="MetricsException"/>
		void startMetricsMBeans();

		/// <summary>Stop metrics MBeans.</summary>
		/// <remarks>
		/// Stop metrics MBeans.
		/// Note, it doesn't stop the metrics system control MBean,
		/// i.e this interface.
		/// </remarks>
		/// <exception cref="MetricsException"/>
		void stopMetricsMBeans();

		/// <returns>
		/// the current config
		/// Avoided getConfig, as it'll turn into a "Config" attribute,
		/// which doesn't support multiple line values in jconsole.
		/// </returns>
		/// <exception cref="MetricsException"/>
		string currentConfig();
	}
}
