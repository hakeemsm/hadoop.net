using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>An adapter class for metrics source and associated filter and jmx impl</summary>
	internal class MetricsSourceAdapter : DynamicMBean
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Impl.MetricsSourceAdapter
			));

		private readonly string prefix;

		private readonly string name;

		private readonly MetricsSource source;

		private readonly MetricsFilter recordFilter;

		private readonly MetricsFilter metricFilter;

		private readonly Dictionary<string, Attribute> attrCache;

		private readonly MBeanInfoBuilder infoBuilder;

		private readonly IEnumerable<MetricsTag> injectedTags;

		private IEnumerable<MetricsRecordImpl> lastRecs;

		private long jmxCacheTS = 0;

		private int jmxCacheTTL;

		private MBeanInfo infoCache;

		private ObjectName mbeanName;

		private readonly bool startMBeans;

		internal MetricsSourceAdapter(string prefix, string name, string description, MetricsSource
			 source, IEnumerable<MetricsTag> injectedTags, MetricsFilter recordFilter, MetricsFilter
			 metricFilter, int jmxCacheTTL, bool startMBeans)
		{
			this.prefix = Preconditions.CheckNotNull(prefix, "prefix");
			this.name = Preconditions.CheckNotNull(name, "name");
			this.source = Preconditions.CheckNotNull(source, "source");
			attrCache = Maps.NewHashMap();
			infoBuilder = new MBeanInfoBuilder(name, description);
			this.injectedTags = injectedTags;
			this.recordFilter = recordFilter;
			this.metricFilter = metricFilter;
			this.jmxCacheTTL = Contracts.CheckArg(jmxCacheTTL, jmxCacheTTL > 0, "jmxCacheTTL"
				);
			this.startMBeans = startMBeans;
		}

		internal MetricsSourceAdapter(string prefix, string name, string description, MetricsSource
			 source, IEnumerable<MetricsTag> injectedTags, int period, MetricsConfig conf)
			: this(prefix, name, description, source, injectedTags, conf.GetFilter(RecordFilterKey
				), conf.GetFilter(MetricFilterKey), period + 1, conf.GetBoolean(StartMbeansKey, 
				true))
		{
		}

		// hack to avoid most of the "innocuous" races.
		internal virtual void Start()
		{
			if (startMBeans)
			{
				StartMBeans();
			}
		}

		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual object GetAttribute(string attribute)
		{
			UpdateJmxCache();
			lock (this)
			{
				Attribute a = attrCache[attribute];
				if (a == null)
				{
					throw new AttributeNotFoundException(attribute + " not found");
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug(attribute + ": " + a);
				}
				return a.GetValue();
			}
		}

		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.InvalidAttributeValueException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual void SetAttribute(Attribute attribute)
		{
			throw new NotSupportedException("Metrics are read-only.");
		}

		public virtual AttributeList GetAttributes(string[] attributes)
		{
			UpdateJmxCache();
			lock (this)
			{
				AttributeList ret = new AttributeList();
				foreach (string key in attributes)
				{
					Attribute attr = attrCache[key];
					if (Log.IsDebugEnabled())
					{
						Log.Debug(key + ": " + attr);
					}
					ret.Add(attr);
				}
				return ret;
			}
		}

		public virtual AttributeList SetAttributes(AttributeList attributes)
		{
			throw new NotSupportedException("Metrics are read-only.");
		}

		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual object Invoke(string actionName, object[] @params, string[] signature
			)
		{
			throw new NotSupportedException("Not supported yet.");
		}

		public virtual MBeanInfo GetMBeanInfo()
		{
			UpdateJmxCache();
			return infoCache;
		}

		private void UpdateJmxCache()
		{
			bool getAllMetrics = false;
			lock (this)
			{
				if (Time.Now() - jmxCacheTS >= jmxCacheTTL)
				{
					// temporarilly advance the expiry while updating the cache
					jmxCacheTS = Time.Now() + jmxCacheTTL;
					if (lastRecs == null)
					{
						getAllMetrics = true;
					}
				}
				else
				{
					return;
				}
			}
			if (getAllMetrics)
			{
				MetricsCollectorImpl builder = new MetricsCollectorImpl();
				GetMetrics(builder, true);
			}
			lock (this)
			{
				UpdateAttrCache();
				if (getAllMetrics)
				{
					UpdateInfoCache();
				}
				jmxCacheTS = Time.Now();
				lastRecs = null;
			}
		}

		// in case regular interval update is not running
		internal virtual IEnumerable<MetricsRecordImpl> GetMetrics(MetricsCollectorImpl builder
			, bool all)
		{
			builder.SetRecordFilter(recordFilter).SetMetricFilter(metricFilter);
			lock (this)
			{
				if (lastRecs == null && jmxCacheTS == 0)
				{
					all = true;
				}
			}
			// Get all the metrics to populate the sink caches
			try
			{
				source.GetMetrics(builder, all);
			}
			catch (Exception e)
			{
				Log.Error("Error getting metrics from source " + name, e);
			}
			foreach (MetricsRecordBuilderImpl rb in builder)
			{
				foreach (MetricsTag t in injectedTags)
				{
					rb.Add(t);
				}
			}
			lock (this)
			{
				lastRecs = builder.GetRecords();
				return lastRecs;
			}
		}

		internal virtual void Stop()
		{
			lock (this)
			{
				StopMBeans();
			}
		}

		internal virtual void StartMBeans()
		{
			lock (this)
			{
				if (mbeanName != null)
				{
					Log.Warn("MBean " + name + " already initialized!");
					Log.Debug("Stacktrace: ", new Exception());
					return;
				}
				mbeanName = MBeans.Register(prefix, name, this);
				Log.Debug("MBean for source " + name + " registered.");
			}
		}

		internal virtual void StopMBeans()
		{
			lock (this)
			{
				if (mbeanName != null)
				{
					MBeans.Unregister(mbeanName);
					mbeanName = null;
				}
			}
		}

		[VisibleForTesting]
		internal virtual ObjectName GetMBeanName()
		{
			return mbeanName;
		}

		private void UpdateInfoCache()
		{
			Log.Debug("Updating info cache...");
			infoCache = infoBuilder.Reset(lastRecs).Get();
			Log.Debug("Done");
		}

		private int UpdateAttrCache()
		{
			Log.Debug("Updating attr cache...");
			int recNo = 0;
			int numMetrics = 0;
			foreach (MetricsRecordImpl record in lastRecs)
			{
				foreach (MetricsTag t in ((IList<MetricsTag>)record.Tags()))
				{
					SetAttrCacheTag(t, recNo);
					++numMetrics;
				}
				foreach (AbstractMetric m in record.Metrics())
				{
					SetAttrCacheMetric(m, recNo);
					++numMetrics;
				}
				++recNo;
			}
			Log.Debug("Done. # tags & metrics=" + numMetrics);
			return numMetrics;
		}

		private static string TagName(string name, int recNo)
		{
			StringBuilder sb = new StringBuilder(name.Length + 16);
			sb.Append("tag.").Append(name);
			if (recNo > 0)
			{
				sb.Append('.').Append(recNo);
			}
			return sb.ToString();
		}

		private void SetAttrCacheTag(MetricsTag tag, int recNo)
		{
			string key = TagName(tag.Name(), recNo);
			attrCache[key] = new Attribute(key, tag.Value());
		}

		private static string MetricName(string name, int recNo)
		{
			if (recNo == 0)
			{
				return name;
			}
			StringBuilder sb = new StringBuilder(name.Length + 12);
			sb.Append(name);
			if (recNo > 0)
			{
				sb.Append('.').Append(recNo);
			}
			return sb.ToString();
		}

		private void SetAttrCacheMetric(AbstractMetric metric, int recNo)
		{
			string key = MetricName(metric.Name(), recNo);
			attrCache[key] = new Attribute(key, metric.Value());
		}

		internal virtual string Name()
		{
			return name;
		}

		internal virtual MetricsSource Source()
		{
			return source;
		}
	}
}
