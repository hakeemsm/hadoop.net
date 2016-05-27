using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>This is the registry for metrics.</summary>
	/// <remarks>
	/// This is the registry for metrics.
	/// Related set of metrics should be declared in a holding class and registered
	/// in a registry for those metrics which is also stored in the the holding class.
	/// </remarks>
	public class MetricsRegistry
	{
		private ConcurrentHashMap<string, MetricsBase> metricsList = new ConcurrentHashMap
			<string, MetricsBase>();

		public MetricsRegistry()
		{
		}

		/// <returns>number of metrics in the registry</returns>
		public virtual int Size()
		{
			return metricsList.Count;
		}

		/// <summary>Add a new metrics to the registry</summary>
		/// <param name="metricsName">- the name</param>
		/// <param name="theMetricsObj">- the metrics</param>
		/// <exception cref="System.ArgumentException">if a name is already registered</exception>
		public virtual void Add(string metricsName, MetricsBase theMetricsObj)
		{
			if (metricsList.PutIfAbsent(metricsName, theMetricsObj) != null)
			{
				throw new ArgumentException("Duplicate metricsName:" + metricsName);
			}
		}

		/// <param name="metricsName"/>
		/// <returns>
		/// the metrics if there is one registered by the supplied name.
		/// Returns null if none is registered
		/// </returns>
		public virtual MetricsBase Get(string metricsName)
		{
			return metricsList[metricsName];
		}

		/// <returns>the list of metrics names</returns>
		public virtual ICollection<string> GetKeyList()
		{
			return metricsList.Keys;
		}

		/// <returns>the list of metrics</returns>
		public virtual ICollection<MetricsBase> GetMetricsList()
		{
			return metricsList.Values;
		}
	}
}
