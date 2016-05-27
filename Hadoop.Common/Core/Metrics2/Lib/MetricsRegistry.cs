using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>
	/// An optional metrics registry class for creating and maintaining a
	/// collection of MetricsMutables, making writing metrics source easier.
	/// </summary>
	public class MetricsRegistry
	{
		private readonly IDictionary<string, MutableMetric> metricsMap = Maps.NewLinkedHashMap
			();

		private readonly IDictionary<string, MetricsTag> tagsMap = Maps.NewLinkedHashMap(
			);

		private readonly MetricsInfo metricsInfo;

		/// <summary>Construct the registry with a record name</summary>
		/// <param name="name">of the record of the metrics</param>
		public MetricsRegistry(string name)
		{
			metricsInfo = Interns.Info(name, name);
		}

		/// <summary>Construct the registry with a metadata object</summary>
		/// <param name="info">the info object for the metrics record/group</param>
		public MetricsRegistry(MetricsInfo info)
		{
			metricsInfo = info;
		}

		/// <returns>the info object of the metrics registry</returns>
		public virtual MetricsInfo Info()
		{
			return metricsInfo;
		}

		/// <summary>Get a metric by name</summary>
		/// <param name="name">of the metric</param>
		/// <returns>the metric object</returns>
		public virtual MutableMetric Get(string name)
		{
			lock (this)
			{
				return metricsMap[name];
			}
		}

		/// <summary>Get a tag by name</summary>
		/// <param name="name">of the tag</param>
		/// <returns>the tag object</returns>
		public virtual MetricsTag GetTag(string name)
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
		public virtual MutableCounterInt NewCounter(string name, string desc, int iVal)
		{
			return NewCounter(Interns.Info(name, desc), iVal);
		}

		/// <summary>Create a mutable integer counter</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual MutableCounterInt NewCounter(MetricsInfo info, int iVal)
		{
			lock (this)
			{
				CheckMetricName(info.Name());
				MutableCounterInt ret = new MutableCounterInt(info, iVal);
				metricsMap[info.Name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable long integer counter</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual MutableCounterLong NewCounter(string name, string desc, long iVal)
		{
			return NewCounter(Interns.Info(name, desc), iVal);
		}

		/// <summary>Create a mutable long integer counter</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new counter object</returns>
		public virtual MutableCounterLong NewCounter(MetricsInfo info, long iVal)
		{
			lock (this)
			{
				CheckMetricName(info.Name());
				MutableCounterLong ret = new MutableCounterLong(info, iVal);
				metricsMap[info.Name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable integer gauge</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual MutableGaugeInt NewGauge(string name, string desc, int iVal)
		{
			return NewGauge(Interns.Info(name, desc), iVal);
		}

		/// <summary>Create a mutable integer gauge</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual MutableGaugeInt NewGauge(MetricsInfo info, int iVal)
		{
			lock (this)
			{
				CheckMetricName(info.Name());
				MutableGaugeInt ret = new MutableGaugeInt(info, iVal);
				metricsMap[info.Name()] = ret;
				return ret;
			}
		}

		/// <summary>Create a mutable long integer gauge</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">metric description</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual MutableGaugeLong NewGauge(string name, string desc, long iVal)
		{
			return NewGauge(Interns.Info(name, desc), iVal);
		}

		/// <summary>Create a mutable long integer gauge</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="iVal">initial value</param>
		/// <returns>a new gauge object</returns>
		public virtual MutableGaugeLong NewGauge(MetricsInfo info, long iVal)
		{
			lock (this)
			{
				CheckMetricName(info.Name());
				MutableGaugeLong ret = new MutableGaugeLong(info, iVal);
				metricsMap[info.Name()] = ret;
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
		public virtual MutableQuantiles NewQuantiles(string name, string desc, string sampleName
			, string valueName, int interval)
		{
			lock (this)
			{
				CheckMetricName(name);
				MutableQuantiles ret = new MutableQuantiles(name, desc, sampleName, valueName, interval
					);
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
		public virtual MutableStat NewStat(string name, string desc, string sampleName, string
			 valueName, bool extended)
		{
			lock (this)
			{
				CheckMetricName(name);
				MutableStat ret = new MutableStat(name, desc, sampleName, valueName, extended);
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
		public virtual MutableStat NewStat(string name, string desc, string sampleName, string
			 valueName)
		{
			return NewStat(name, desc, sampleName, valueName, false);
		}

		/// <summary>Create a mutable rate metric</summary>
		/// <param name="name">of the metric</param>
		/// <returns>a new mutable metric object</returns>
		public virtual MutableRate NewRate(string name)
		{
			return NewRate(name, name, false);
		}

		/// <summary>Create a mutable rate metric</summary>
		/// <param name="name">of the metric</param>
		/// <param name="description">of the metric</param>
		/// <returns>a new mutable rate metric object</returns>
		public virtual MutableRate NewRate(string name, string description)
		{
			return NewRate(name, description, false);
		}

		/// <summary>Create a mutable rate metric (for throughput measurement)</summary>
		/// <param name="name">of the metric</param>
		/// <param name="desc">description</param>
		/// <param name="extended">produce extended stat (stdev/min/max etc.) if true</param>
		/// <returns>a new mutable rate metric object</returns>
		public virtual MutableRate NewRate(string name, string desc, bool extended)
		{
			return NewRate(name, desc, extended, true);
		}

		[InterfaceAudience.Private]
		public virtual MutableRate NewRate(string name, string desc, bool extended, bool 
			returnExisting)
		{
			lock (this)
			{
				if (returnExisting)
				{
					MutableMetric rate = metricsMap[name];
					if (rate != null)
					{
						if (rate is MutableRate)
						{
							return (MutableRate)rate;
						}
						throw new MetricsException("Unexpected metrics type " + rate.GetType() + " for " 
							+ name);
					}
				}
				CheckMetricName(name);
				MutableRate ret = new MutableRate(name, desc, extended);
				metricsMap[name] = ret;
				return ret;
			}
		}

		internal virtual void Add(string name, MutableMetric metric)
		{
			lock (this)
			{
				CheckMetricName(name);
				metricsMap[name] = metric;
			}
		}

		/// <summary>Add sample to a stat metric by name.</summary>
		/// <param name="name">of the metric</param>
		/// <param name="value">of the snapshot to add</param>
		public virtual void Add(string name, long value)
		{
			lock (this)
			{
				MutableMetric m = metricsMap[name];
				if (m != null)
				{
					if (m is MutableStat)
					{
						((MutableStat)m).Add(value);
					}
					else
					{
						throw new MetricsException("Unsupported add(value) for metric " + name);
					}
				}
				else
				{
					metricsMap[name] = NewRate(name);
					// default is a rate metric
					Add(name, value);
				}
			}
		}

		/// <summary>Set the metrics context tag</summary>
		/// <param name="name">of the context</param>
		/// <returns>the registry itself as a convenience</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Lib.MetricsRegistry SetContext(string name
			)
		{
			return Tag(MsInfo.Context, name, true);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>the registry (for keep adding tags)</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Lib.MetricsRegistry Tag(string name, string
			 description, string value)
		{
			return Tag(name, description, value, false);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <param name="override">existing tag if true</param>
		/// <returns>the registry (for keep adding tags)</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Lib.MetricsRegistry Tag(string name, string
			 description, string value, bool @override)
		{
			return Tag(Interns.Info(name, description), value, @override);
		}

		/// <summary>Add a tag to the metrics</summary>
		/// <param name="info">metadata of the tag</param>
		/// <param name="value">of the tag</param>
		/// <param name="override">existing tag if true</param>
		/// <returns>the registry (for keep adding tags etc.)</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Lib.MetricsRegistry Tag(MetricsInfo info
			, string value, bool @override)
		{
			lock (this)
			{
				if (!@override)
				{
					CheckTagName(info.Name());
				}
				tagsMap[info.Name()] = Interns.Tag(info, value);
				return this;
			}
		}

		public virtual Org.Apache.Hadoop.Metrics2.Lib.MetricsRegistry Tag(MetricsInfo info
			, string value)
		{
			return Tag(info, value, false);
		}

		internal virtual ICollection<MetricsTag> Tags()
		{
			return tagsMap.Values;
		}

		internal virtual ICollection<MutableMetric> Metrics()
		{
			return metricsMap.Values;
		}

		private void CheckMetricName(string name)
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
				throw new MetricsException("Metric name '" + name + "' contains illegal whitespace character"
					);
			}
			// Check if name has already been registered
			if (metricsMap.Contains(name))
			{
				throw new MetricsException("Metric name " + name + " already exists!");
			}
		}

		private void CheckTagName(string name)
		{
			if (tagsMap.Contains(name))
			{
				throw new MetricsException("Tag " + name + " already exists!");
			}
		}

		/// <summary>Sample all the mutable metrics and put the snapshot in the builder</summary>
		/// <param name="builder">to contain the metrics snapshot</param>
		/// <param name="all">get all the metrics even if the values are not changed.</param>
		public virtual void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			lock (this)
			{
				foreach (MetricsTag tag in Tags())
				{
					builder.Add(tag);
				}
				foreach (MutableMetric metric in Metrics())
				{
					metric.Snapshot(builder, all);
				}
			}
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("info", metricsInfo).Add("tags", Tags()).
				Add("metrics", Metrics()).ToString();
		}
	}
}
