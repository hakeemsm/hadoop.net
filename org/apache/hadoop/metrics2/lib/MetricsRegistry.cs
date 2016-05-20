using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>
	/// An optional metrics registry class for creating and maintaining a
	/// collection of MetricsMutables, making writing metrics source easier.
	/// </summary>
	public class MetricsRegistry
	{
		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.lib.MutableMetric
			> metricsMap = com.google.common.collect.Maps.newLinkedHashMap();

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.MetricsTag
			> tagsMap = com.google.common.collect.Maps.newLinkedHashMap();

		private readonly org.apache.hadoop.metrics2.MetricsInfo metricsInfo;

		/// <summary>Construct the registry with a record name</summary>
		/// <param name="name">of the record of the metrics</param>
		public MetricsRegistry(string name)
		{
			metricsInfo = org.apache.hadoop.metrics2.lib.Interns.info(name, name);
		}

		/// <summary>Construct the registry with a metadata object</summary>
		/// <param name="info">the info object for the metrics record/group</param>
		public MetricsRegistry(org.apache.hadoop.metrics2.MetricsInfo info)
		{
			metricsInfo = info;
		}

		/// <returns>the info object of the metrics registry</returns>
		public virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return metricsInfo;
		}

		/// <summary>Get a metric by name</summary>
		/// <param name="name">of the metric</param>
		/// <returns>the metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableMetric get(string name)
		{
			lock (this)
			{
				return metricsMap[name];
			}
		}

		/// <summary>Get a tag by name</summary>
		/// <param name="name">of the tag</param>
		/// <returns>the tag object</returns>
		public virtual org.apache.hadoop.metrics2.MetricsTag getTag(string name)
		{
			lock (this)
			{
				return tagsMap[name];
			}
		}

		/// <summary>Create a mutable integer counter</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableCounterInt newCounter(string
			 name, string desc, int iVal)
		{
			return newCounter(org.apache.hadoop.metrics2.lib.Interns.info(name, desc), iVal);
		}

		/// <summary>Create a mutable integer counter</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableCounterInt newCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, int iVal)
		{
			lock (this)
			{
				checkMetricName(info.name());
				org.apache.hadoop.metrics2.lib.MutableCounterInt ret = new org.apache.hadoop.metrics2.lib.MutableCounterInt
					(info, iVal);
				metricsMap[info.name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable long integer counter</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableCounterLong newCounter(string
			 name, string desc, long iVal)
		{
			return newCounter(org.apache.hadoop.metrics2.lib.Interns.info(name, desc), iVal);
		}

		/// <summary>Create a mutable long integer counter</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableCounterLong newCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, long iVal)
		{
			lock (this)
			{
				checkMetricName(info.name());
				org.apache.hadoop.metrics2.lib.MutableCounterLong ret = new org.apache.hadoop.metrics2.lib.MutableCounterLong
					(info, iVal);
				metricsMap[info.name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable integer gauge</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableGaugeInt newGauge(string name
			, string desc, int iVal)
		{
			return newGauge(org.apache.hadoop.metrics2.lib.Interns.info(name, desc), iVal);
		}

		/// <summary>Create a mutable integer gauge</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableGaugeInt newGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, int iVal)
		{
			lock (this)
			{
				checkMetricName(info.name());
				org.apache.hadoop.metrics2.lib.MutableGaugeInt ret = new org.apache.hadoop.metrics2.lib.MutableGaugeInt
					(info, iVal);
				metricsMap[info.name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable long integer gauge</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableGaugeLong newGauge(string name
			, string desc, long iVal)
		{
			return newGauge(org.apache.hadoop.metrics2.lib.Interns.info(name, desc), iVal);
		}

		/// <summary>Create a mutable long integer gauge</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableGaugeLong newGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, long iVal)
		{
			lock (this)
			{
				checkMetricName(info.name());
				org.apache.hadoop.metrics2.lib.MutableGaugeLong ret = new org.apache.hadoop.metrics2.lib.MutableGaugeLong
					(info, iVal);
				metricsMap[info.name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable metric that estimates quantiles of a stream of values</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="sampleName">of the metric (e.g., "Ops")</param>
		/// <param name="valueName">of the metric (e.g., "Time" or "Latency")</param>
		/// <param name="interval">rollover interval of estimator in seconds</param>
		/// <returns>a new quantile estimator object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableQuantiles newQuantiles(string
			 name, string desc, string sampleName, string valueName, int interval)
		{
			lock (this)
			{
				checkMetricName(name);
				org.apache.hadoop.metrics2.lib.MutableQuantiles ret = new org.apache.hadoop.metrics2.lib.MutableQuantiles
					(name, desc, sampleName, valueName, interval);
				metricsMap[name] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable metric with stats</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="sampleName">of the metric (e.g., "Ops")</param>
		/// <param name="valueName">of the metric (e.g., "Time" or "Latency")</param>
		/// <param name="extended">produce extended stat (stdev, min/max etc.) if true.</param>
		/// <returns>a new mutable stat metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableStat newStat(string name, string
			 desc, string sampleName, string valueName, bool extended)
		{
			lock (this)
			{
				checkMetricName(name);
				org.apache.hadoop.metrics2.lib.MutableStat ret = new org.apache.hadoop.metrics2.lib.MutableStat
					(name, desc, sampleName, valueName, extended);
				metricsMap[name] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable metric with stats</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="sampleName">of the metric (e.g., "Ops")</param>
		/// <param name="valueName">of the metric (e.g., "Time" or "Latency")</param>
		/// <returns>a new mutable metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableStat newStat(string name, string
			 desc, string sampleName, string valueName)
		{
			return newStat(name, desc, sampleName, valueName, false);
		}

		/// <summary>Create a mutable rate metric</summary>
		/// <param name="name">of the metric</param>
		/// <returns>a new mutable metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableRate newRate(string name)
		{
			return newRate(name, name, false);
		}

		/// <summary>Create a mutable rate metric</summary>
		/// <param name="name">of the metric</param>
		/// <param name="description">of the metric</param>
		/// <returns>a new mutable rate metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableRate newRate(string name, string
			 description)
		{
			return newRate(name, description, false);
		}

		/// <summary>Create a mutable rate metric (for throughput measurement)</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">description</param>
		/// <param name="extended">produce extended stat (stdev/min/max etc.) if true</param>
		/// <returns>a new mutable rate metric object</returns>
		public virtual org.apache.hadoop.metrics2.lib.MutableRate newRate(string name, string
			 desc, bool extended)
		{
			return newRate(name, desc, extended, true);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.metrics2.lib.MutableRate newRate(string name, string
			 desc, bool extended, bool returnExisting)
		{
			lock (this)
			{
				if (returnExisting)
				{
					org.apache.hadoop.metrics2.lib.MutableMetric rate = metricsMap[name];
					if (rate != null)
					{
						if (rate is org.apache.hadoop.metrics2.lib.MutableRate)
						{
							return (org.apache.hadoop.metrics2.lib.MutableRate)rate;
						}
						throw new org.apache.hadoop.metrics2.MetricsException("Unexpected metrics type " 
							+ Sharpen.Runtime.getClassForObject(rate) + " for " + name);
					}
				}
				checkMetricName(name);
				org.apache.hadoop.metrics2.lib.MutableRate ret = new org.apache.hadoop.metrics2.lib.MutableRate
					(name, desc, extended);
				metricsMap[name] = ret;
				return ret;
			}
		}

		internal virtual void add(string name, org.apache.hadoop.metrics2.lib.MutableMetric
			 metric)
		{
			lock (this)
			{
				checkMetricName(name);
				metricsMap[name] = metric;
			}
		}

		/// <summary>Add sample to a stat metric by name.</summary>
		/// <param name="name">of the metric</param>
		/// <param name="value">of the snapshot to add</param>
		public virtual void add(string name, long value)
		{
			lock (this)
			{
				org.apache.hadoop.metrics2.lib.MutableMetric m = metricsMap[name];
				if (m != null)
				{
					if (m is org.apache.hadoop.metrics2.lib.MutableStat)
					{
						((org.apache.hadoop.metrics2.lib.MutableStat)m).add(value);
					}
					else
					{
						throw new org.apache.hadoop.metrics2.MetricsException("Unsupported add(value) for metric "
							 + name);
					}
				}
				else
				{
					metricsMap[name] = newRate(name);
					// default is a rate metric
					add(name, value);
				}
			}
		}

		/// <summary>Set the metrics context tag</summary>
		/// <param name="name">of the context</param>
		/// <returns>the registry itself as a convenience</returns>
		public virtual org.apache.hadoop.metrics2.lib.MetricsRegistry setContext(string name
			)
		{
			return tag(org.apache.hadoop.metrics2.impl.MsInfo.Context, name, true);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>the registry (for keep adding tags)</returns>
		public virtual org.apache.hadoop.metrics2.lib.MetricsRegistry tag(string name, string
			 description, string value)
		{
			return tag(name, description, value, false);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <param name="override">existing tag if true</param>
		/// <returns>the registry (for keep adding tags)</returns>
		public virtual org.apache.hadoop.metrics2.lib.MetricsRegistry tag(string name, string
			 description, string value, bool @override)
		{
			return tag(org.apache.hadoop.metrics2.lib.Interns.info(name, description), value, 
				@override);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="info">metadata of the tag</param>
		/// <param name="value">of the tag</param>
		/// <param name="override">existing tag if true</param>
		/// <returns>the registry (for keep adding tags etc.)</returns>
		public virtual org.apache.hadoop.metrics2.lib.MetricsRegistry tag(org.apache.hadoop.metrics2.MetricsInfo
			 info, string value, bool @override)
		{
			lock (this)
			{
				if (!@override)
				{
					checkTagName(info.name());
				}
				tagsMap[info.name()] = org.apache.hadoop.metrics2.lib.Interns.tag(info, value);
				return this;
			}
		}

		public virtual org.apache.hadoop.metrics2.lib.MetricsRegistry tag(org.apache.hadoop.metrics2.MetricsInfo
			 info, string value)
		{
			return tag(info, value, false);
		}

		internal virtual System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag
			> tags()
		{
			return tagsMap.Values;
		}

		internal virtual System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.lib.MutableMetric
			> metrics()
		{
			return metricsMap.Values;
		}

		private void checkMetricName(string name)
		{
			// Check for invalid characters in metric name
			bool foundWhitespace = false;
			for (int i = 0; i < name.Length; i++)
			{
				char c = name[i];
				if (char.IsWhiteSpace(c))
				{
					foundWhitespace = true;
					break;
				}
			}
			if (foundWhitespace)
			{
				throw new org.apache.hadoop.metrics2.MetricsException("Metric name '" + name + "' contains illegal whitespace character"
					);
			}
			// Check if name has already been registered
			if (metricsMap.Contains(name))
			{
				throw new org.apache.hadoop.metrics2.MetricsException("Metric name " + name + " already exists!"
					);
			}
		}

		private void checkTagName(string name)
		{
			if (tagsMap.Contains(name))
			{
				throw new org.apache.hadoop.metrics2.MetricsException("Tag " + name + " already exists!"
					);
			}
		}

		/// <summary>Sample all the mutable metrics and put the snapshot in the builder</summary>
		/// <param name="builder">to contain the metrics snapshot</param>
		/// <param name="all">get all the metrics even if the values are not changed.</param>
		public virtual void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			lock (this)
			{
				foreach (org.apache.hadoop.metrics2.MetricsTag tag in tags())
				{
					builder.add(tag);
				}
				foreach (org.apache.hadoop.metrics2.lib.MutableMetric metric in metrics())
				{
					metric.snapshot(builder, all);
				}
			}
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("info", metricsInfo
				).add("tags", tags()).add("metrics", metrics()).ToString();
		}
	}
}
