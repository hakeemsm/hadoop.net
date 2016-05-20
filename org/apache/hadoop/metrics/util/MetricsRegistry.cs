using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>This is the registry for metrics.</summary>
	/// <remarks>
	/// This is the registry for metrics.
	/// Related set of metrics should be declared in a holding class and registered
	/// in a registry for those metrics which is also stored in the the holding class.
	/// </remarks>
	public class MetricsRegistry
	{
		private java.util.concurrent.ConcurrentHashMap<string, org.apache.hadoop.metrics.util.MetricsBase
			> metricsList = new java.util.concurrent.ConcurrentHashMap<string, org.apache.hadoop.metrics.util.MetricsBase
			>();

		public MetricsRegistry()
		{
		}

		/// <returns>number of metrics in the registry</returns>
		public virtual int size()
		{
			return metricsList.Count;
		}

		/// <summary>Add a new metrics to the registry</summary>
		/// <param name="metricsName">- the name</param>
		/// <param name="theMetricsObj">- the metrics</param>
		/// <exception cref="System.ArgumentException">if a name is already registered</exception>
		public virtual void add(string metricsName, org.apache.hadoop.metrics.util.MetricsBase
			 theMetricsObj)
		{
			if (metricsList.putIfAbsent(metricsName, theMetricsObj) != null)
			{
				throw new System.ArgumentException("Duplicate metricsName:" + metricsName);
			}
		}

		/// <param name="metricsName"/>
		/// <returns>
		/// the metrics if there is one registered by the supplied name.
		/// Returns null if none is registered
		/// </returns>
		public virtual org.apache.hadoop.metrics.util.MetricsBase get(string metricsName)
		{
			return metricsList[metricsName];
		}

		/// <returns>the list of metrics names</returns>
		public virtual System.Collections.Generic.ICollection<string> getKeyList()
		{
			return metricsList.Keys;
		}

		/// <returns>the list of metrics</returns>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.metrics.util.MetricsBase
			> getMetricsList()
		{
			return metricsList.Values;
		}
	}
}
