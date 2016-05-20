using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>An adapter class for metrics source and associated filter and jmx impl</summary>
	internal class MetricsSourceAdapter : javax.management.DynamicMBean
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.impl.MetricsSourceAdapter
			)));

		private readonly string prefix;

		private readonly string name;

		private readonly org.apache.hadoop.metrics2.MetricsSource source;

		private readonly org.apache.hadoop.metrics2.MetricsFilter recordFilter;

		private readonly org.apache.hadoop.metrics2.MetricsFilter metricFilter;

		private readonly System.Collections.Generic.Dictionary<string, javax.management.Attribute
			> attrCache;

		private readonly org.apache.hadoop.metrics2.impl.MBeanInfoBuilder infoBuilder;

		private readonly System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.MetricsTag
			> injectedTags;

		private System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
			> lastRecs;

		private long jmxCacheTS = 0;

		private int jmxCacheTTL;

		private javax.management.MBeanInfo infoCache;

		private javax.management.ObjectName mbeanName;

		private readonly bool startMBeans;

		internal MetricsSourceAdapter(string prefix, string name, string description, org.apache.hadoop.metrics2.MetricsSource
			 source, System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.MetricsTag
			> injectedTags, org.apache.hadoop.metrics2.MetricsFilter recordFilter, org.apache.hadoop.metrics2.MetricsFilter
			 metricFilter, int jmxCacheTTL, bool startMBeans)
		{
			this.prefix = com.google.common.@base.Preconditions.checkNotNull(prefix, "prefix"
				);
			this.name = com.google.common.@base.Preconditions.checkNotNull(name, "name");
			this.source = com.google.common.@base.Preconditions.checkNotNull(source, "source"
				);
			attrCache = com.google.common.collect.Maps.newHashMap();
			infoBuilder = new org.apache.hadoop.metrics2.impl.MBeanInfoBuilder(name, description
				);
			this.injectedTags = injectedTags;
			this.recordFilter = recordFilter;
			this.metricFilter = metricFilter;
			this.jmxCacheTTL = org.apache.hadoop.metrics2.util.Contracts.checkArg(jmxCacheTTL
				, jmxCacheTTL > 0, "jmxCacheTTL");
			this.startMBeans = startMBeans;
		}

		internal MetricsSourceAdapter(string prefix, string name, string description, org.apache.hadoop.metrics2.MetricsSource
			 source, System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.MetricsTag
			> injectedTags, int period, org.apache.hadoop.metrics2.impl.MetricsConfig conf)
			: this(prefix, name, description, source, injectedTags, conf.getFilter(RECORD_FILTER_KEY
				), conf.getFilter(METRIC_FILTER_KEY), period + 1, conf.getBoolean(START_MBEANS_KEY
				, true))
		{
		}

		// hack to avoid most of the "innocuous" races.
		internal virtual void start()
		{
			if (startMBeans)
			{
				startMBeans();
			}
		}

		/// <exception cref="javax.management.AttributeNotFoundException"/>
		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual object getAttribute(string attribute)
		{
			updateJmxCache();
			lock (this)
			{
				javax.management.Attribute a = attrCache[attribute];
				if (a == null)
				{
					throw new javax.management.AttributeNotFoundException(attribute + " not found");
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug(attribute + ": " + a);
				}
				return a.getValue();
			}
		}

		/// <exception cref="javax.management.AttributeNotFoundException"/>
		/// <exception cref="javax.management.InvalidAttributeValueException"/>
		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual void setAttribute(javax.management.Attribute attribute)
		{
			throw new System.NotSupportedException("Metrics are read-only.");
		}

		public virtual javax.management.AttributeList getAttributes(string[] attributes)
		{
			updateJmxCache();
			lock (this)
			{
				javax.management.AttributeList ret = new javax.management.AttributeList();
				foreach (string key in attributes)
				{
					javax.management.Attribute attr = attrCache[key];
					if (LOG.isDebugEnabled())
					{
						LOG.debug(key + ": " + attr);
					}
					ret.add(attr);
				}
				return ret;
			}
		}

		public virtual javax.management.AttributeList setAttributes(javax.management.AttributeList
			 attributes)
		{
			throw new System.NotSupportedException("Metrics are read-only.");
		}

		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual object invoke(string actionName, object[] @params, string[] signature
			)
		{
			throw new System.NotSupportedException("Not supported yet.");
		}

		public virtual javax.management.MBeanInfo getMBeanInfo()
		{
			updateJmxCache();
			return infoCache;
		}

		private void updateJmxCache()
		{
			bool getAllMetrics = false;
			lock (this)
			{
				if (org.apache.hadoop.util.Time.now() - jmxCacheTS >= jmxCacheTTL)
				{
					// temporarilly advance the expiry while updating the cache
					jmxCacheTS = org.apache.hadoop.util.Time.now() + jmxCacheTTL;
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
				org.apache.hadoop.metrics2.impl.MetricsCollectorImpl builder = new org.apache.hadoop.metrics2.impl.MetricsCollectorImpl
					();
				getMetrics(builder, true);
			}
			lock (this)
			{
				updateAttrCache();
				if (getAllMetrics)
				{
					updateInfoCache();
				}
				jmxCacheTS = org.apache.hadoop.util.Time.now();
				lastRecs = null;
			}
		}

		// in case regular interval update is not running
		internal virtual System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
			> getMetrics(org.apache.hadoop.metrics2.impl.MetricsCollectorImpl builder, bool 
			all)
		{
			builder.setRecordFilter(recordFilter).setMetricFilter(metricFilter);
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
				source.getMetrics(builder, all);
			}
			catch (System.Exception e)
			{
				LOG.error("Error getting metrics from source " + name, e);
			}
			foreach (org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl rb in builder)
			{
				foreach (org.apache.hadoop.metrics2.MetricsTag t in injectedTags)
				{
					rb.add(t);
				}
			}
			lock (this)
			{
				lastRecs = builder.getRecords();
				return lastRecs;
			}
		}

		internal virtual void stop()
		{
			lock (this)
			{
				stopMBeans();
			}
		}

		internal virtual void startMBeans()
		{
			lock (this)
			{
				if (mbeanName != null)
				{
					LOG.warn("MBean " + name + " already initialized!");
					LOG.debug("Stacktrace: ", new System.Exception());
					return;
				}
				mbeanName = org.apache.hadoop.metrics2.util.MBeans.register(prefix, name, this);
				LOG.debug("MBean for source " + name + " registered.");
			}
		}

		internal virtual void stopMBeans()
		{
			lock (this)
			{
				if (mbeanName != null)
				{
					org.apache.hadoop.metrics2.util.MBeans.unregister(mbeanName);
					mbeanName = null;
				}
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual javax.management.ObjectName getMBeanName()
		{
			return mbeanName;
		}

		private void updateInfoCache()
		{
			LOG.debug("Updating info cache...");
			infoCache = infoBuilder.reset(lastRecs).get();
			LOG.debug("Done");
		}

		private int updateAttrCache()
		{
			LOG.debug("Updating attr cache...");
			int recNo = 0;
			int numMetrics = 0;
			foreach (org.apache.hadoop.metrics2.impl.MetricsRecordImpl record in lastRecs)
			{
				foreach (org.apache.hadoop.metrics2.MetricsTag t in ((System.Collections.Generic.IList
					<org.apache.hadoop.metrics2.MetricsTag>)record.tags()))
				{
					setAttrCacheTag(t, recNo);
					++numMetrics;
				}
				foreach (org.apache.hadoop.metrics2.AbstractMetric m in record.metrics())
				{
					setAttrCacheMetric(m, recNo);
					++numMetrics;
				}
				++recNo;
			}
			LOG.debug("Done. # tags & metrics=" + numMetrics);
			return numMetrics;
		}

		private static string tagName(string name, int recNo)
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(name.Length + 16);
			sb.Append("tag.").Append(name);
			if (recNo > 0)
			{
				sb.Append('.').Append(recNo);
			}
			return sb.ToString();
		}

		private void setAttrCacheTag(org.apache.hadoop.metrics2.MetricsTag tag, int recNo
			)
		{
			string key = tagName(tag.name(), recNo);
			attrCache[key] = new javax.management.Attribute(key, tag.value());
		}

		private static string metricName(string name, int recNo)
		{
			if (recNo == 0)
			{
				return name;
			}
			java.lang.StringBuilder sb = new java.lang.StringBuilder(name.Length + 12);
			sb.Append(name);
			if (recNo > 0)
			{
				sb.Append('.').Append(recNo);
			}
			return sb.ToString();
		}

		private void setAttrCacheMetric(org.apache.hadoop.metrics2.AbstractMetric metric, 
			int recNo)
		{
			string key = metricName(metric.name(), recNo);
			attrCache[key] = new javax.management.Attribute(key, metric.value());
		}

		internal virtual string name()
		{
			return name;
		}

		internal virtual org.apache.hadoop.metrics2.MetricsSource source()
		{
			return source;
		}
	}
}
