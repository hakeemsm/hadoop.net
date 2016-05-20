using Sharpen;

namespace org.apache.hadoop.metrics2.sink.ganglia
{
	/// <summary>This code supports Ganglia 3.0</summary>
	public class GangliaSink30 : org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink
	{
		public readonly org.apache.commons.logging.Log LOG;

		private const string TAGS_FOR_PREFIX_PROPERTY_PREFIX = "tagsForPrefix.";

		private org.apache.hadoop.metrics2.util.MetricsCache metricsCache = new org.apache.hadoop.metrics2.util.MetricsCache
			();

		private System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
			<string>> useTagsMap = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.ICollection
			<string>>();

		// a key with a NULL value means ALL
		public override void init(org.apache.commons.configuration.SubsetConfiguration conf
			)
		{
			base.init(conf);
			conf.setListDelimiter(',');
			System.Collections.Generic.IEnumerator<string> it = (System.Collections.Generic.IEnumerator
				<string>)conf.getKeys();
			while (it.MoveNext())
			{
				string propertyName = it.Current;
				if (propertyName.StartsWith(TAGS_FOR_PREFIX_PROPERTY_PREFIX))
				{
					string contextName = Sharpen.Runtime.substring(propertyName, TAGS_FOR_PREFIX_PROPERTY_PREFIX
						.Length);
					string[] tags = conf.getStringArray(propertyName);
					bool useAllTags = false;
					System.Collections.Generic.ICollection<string> set = null;
					if (tags.Length > 0)
					{
						set = new java.util.HashSet<string>();
						foreach (string tag in tags)
						{
							tag = tag.Trim();
							useAllTags |= tag.Equals("*");
							if (tag.Length > 0)
							{
								set.add(tag);
							}
						}
						if (useAllTags)
						{
							set = null;
						}
					}
					useTagsMap[contextName] = set;
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void appendPrefix(org.apache.hadoop.metrics2.MetricsRecord record, 
			java.lang.StringBuilder sb)
		{
			string contextName = record.context();
			System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag> tags
				 = record.tags();
			if (useTagsMap.Contains(contextName))
			{
				System.Collections.Generic.ICollection<string> useTags = useTagsMap[contextName];
				foreach (org.apache.hadoop.metrics2.MetricsTag t in tags)
				{
					if (useTags == null || useTags.contains(t.name()))
					{
						// the context is always skipped here because it is always added
						// the hostname is always skipped to avoid case-mismatches 
						// from different DNSes.
						if (t.info() != org.apache.hadoop.metrics2.impl.MsInfo.Context && t.info() != org.apache.hadoop.metrics2.impl.MsInfo
							.Hostname && t.value() != null)
						{
							sb.Append('.').Append(t.name()).Append('=').Append(t.value());
						}
					}
				}
			}
		}

		public override void putMetrics(org.apache.hadoop.metrics2.MetricsRecord record)
		{
			// The method handles both cases whether Ganglia support dense publish
			// of metrics of sparse (only on change) publish of metrics
			try
			{
				string recordName = record.name();
				string contextName = record.context();
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append(contextName);
				sb.Append('.');
				sb.Append(recordName);
				appendPrefix(record, sb);
				string groupName = sb.ToString();
				sb.Append('.');
				int sbBaseLen = sb.Length;
				string type = null;
				org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope slopeFromMetric
					 = null;
				org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope calculatedSlope
					 = null;
				org.apache.hadoop.metrics2.util.MetricsCache.Record cachedMetrics = null;
				resetBuffer();
				// reset the buffer to the beginning
				if (!isSupportSparseMetrics())
				{
					// for sending dense metrics, update metrics cache
					// and get the updated data
					cachedMetrics = metricsCache.update(record);
					if (cachedMetrics != null && cachedMetrics.metricsEntrySet() != null)
					{
						foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.AbstractMetric
							> entry in cachedMetrics.metricsEntrySet())
						{
							org.apache.hadoop.metrics2.AbstractMetric metric = entry.Value;
							sb.Append(metric.name());
							string name = sb.ToString();
							// visit the metric to identify the Ganglia type and
							// slope
							metric.visit(gangliaMetricVisitor);
							type = gangliaMetricVisitor.getType();
							slopeFromMetric = gangliaMetricVisitor.getSlope();
							org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gConf = getGangliaConfForMetric
								(name);
							calculatedSlope = calculateSlope(gConf, slopeFromMetric);
							// send metric to Ganglia
							emitMetric(groupName, name, type, metric.value().ToString(), gConf, calculatedSlope
								);
							// reset the length of the buffer for next iteration
							sb.Length = sbBaseLen;
						}
					}
				}
				else
				{
					// we support sparse updates
					System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.AbstractMetric>
						 metrics = (System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.AbstractMetric
						>)record.metrics();
					if (metrics.Count > 0)
					{
						// we got metrics. so send the latest
						foreach (org.apache.hadoop.metrics2.AbstractMetric metric in record.metrics())
						{
							sb.Append(metric.name());
							string name = sb.ToString();
							// visit the metric to identify the Ganglia type and
							// slope
							metric.visit(gangliaMetricVisitor);
							type = gangliaMetricVisitor.getType();
							slopeFromMetric = gangliaMetricVisitor.getSlope();
							org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gConf = getGangliaConfForMetric
								(name);
							calculatedSlope = calculateSlope(gConf, slopeFromMetric);
							// send metric to Ganglia
							emitMetric(groupName, name, type, metric.value().ToString(), gConf, calculatedSlope
								);
							// reset the length of the buffer for next iteration
							sb.Length = sbBaseLen;
						}
					}
				}
			}
			catch (System.IO.IOException io)
			{
				throw new org.apache.hadoop.metrics2.MetricsException("Failed to putMetrics", io);
			}
		}

		// Calculate the slope from properties and metric
		private org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope 
			calculateSlope(org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gConf, org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 slopeFromMetric)
		{
			if (gConf.getSlope() != null)
			{
				// if slope has been specified in properties, use that
				return gConf.getSlope();
			}
			else
			{
				if (slopeFromMetric != null)
				{
					// slope not specified in properties, use derived from Metric
					return slopeFromMetric;
				}
				else
				{
					return DEFAULT_SLOPE;
				}
			}
		}

		/// <summary>The method sends metrics to Ganglia servers.</summary>
		/// <remarks>
		/// The method sends metrics to Ganglia servers. The method has been taken from
		/// org.apache.hadoop.metrics.ganglia.GangliaContext30 with minimal changes in
		/// order to keep it in sync.
		/// </remarks>
		/// <param name="groupName">The group name of the metric</param>
		/// <param name="name">The metric name</param>
		/// <param name="type">The type of the metric</param>
		/// <param name="value">The value of the metric</param>
		/// <param name="gConf">The GangliaConf for this metric</param>
		/// <param name="gSlope">The slope for this metric</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void emitMetric(string groupName, string name, string 
			type, string value, org.apache.hadoop.metrics2.sink.ganglia.GangliaConf gConf, org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 gSlope)
		{
			if (name == null)
			{
				LOG.warn("Metric was emitted with no name.");
				return;
			}
			else
			{
				if (value == null)
				{
					LOG.warn("Metric name " + name + " was emitted with a null value.");
					return;
				}
				else
				{
					if (type == null)
					{
						LOG.warn("Metric name " + name + ", value " + value + " has no type.");
						return;
					}
				}
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Emitting metric " + name + ", type " + type + ", value " + value + ", slope "
					 + gSlope.ToString() + " from hostname " + getHostName());
			}
			xdr_int(0);
			// metric_user_defined
			xdr_string(type);
			xdr_string(name);
			xdr_string(value);
			xdr_string(gConf.getUnits());
			xdr_int((int)(gSlope));
			xdr_int(gConf.getTmax());
			xdr_int(gConf.getDmax());
			// send the metric to Ganglia hosts
			emitToGangliaHosts();
		}

		public GangliaSink30()
		{
			LOG = org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject
				(this));
		}
	}
}
