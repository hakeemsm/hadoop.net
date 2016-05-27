using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>This code supports Ganglia 3.0</summary>
	public class GangliaSink30 : AbstractGangliaSink
	{
		public readonly Log Log = LogFactory.GetLog(this.GetType());

		private const string TagsForPrefixPropertyPrefix = "tagsForPrefix.";

		private MetricsCache metricsCache = new MetricsCache();

		private IDictionary<string, ICollection<string>> useTagsMap = new Dictionary<string
			, ICollection<string>>();

		// a key with a NULL value means ALL
		public override void Init(SubsetConfiguration conf)
		{
			base.Init(conf);
			conf.SetListDelimiter(',');
			IEnumerator<string> it = (IEnumerator<string>)conf.GetKeys();
			while (it.HasNext())
			{
				string propertyName = it.Next();
				if (propertyName.StartsWith(TagsForPrefixPropertyPrefix))
				{
					string contextName = Sharpen.Runtime.Substring(propertyName, TagsForPrefixPropertyPrefix
						.Length);
					string[] tags = conf.GetStringArray(propertyName);
					bool useAllTags = false;
					ICollection<string> set = null;
					if (tags.Length > 0)
					{
						set = new HashSet<string>();
						foreach (string tag in tags)
						{
							tag = tag.Trim();
							useAllTags |= tag.Equals("*");
							if (tag.Length > 0)
							{
								set.AddItem(tag);
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

		[InterfaceAudience.Private]
		public virtual void AppendPrefix(MetricsRecord record, StringBuilder sb)
		{
			string contextName = record.Context();
			ICollection<MetricsTag> tags = record.Tags();
			if (useTagsMap.Contains(contextName))
			{
				ICollection<string> useTags = useTagsMap[contextName];
				foreach (MetricsTag t in tags)
				{
					if (useTags == null || useTags.Contains(t.Name()))
					{
						// the context is always skipped here because it is always added
						// the hostname is always skipped to avoid case-mismatches 
						// from different DNSes.
						if (t.Info() != MsInfo.Context && t.Info() != MsInfo.Hostname && t.Value() != null)
						{
							sb.Append('.').Append(t.Name()).Append('=').Append(t.Value());
						}
					}
				}
			}
		}

		public override void PutMetrics(MetricsRecord record)
		{
			// The method handles both cases whether Ganglia support dense publish
			// of metrics of sparse (only on change) publish of metrics
			try
			{
				string recordName = record.Name();
				string contextName = record.Context();
				StringBuilder sb = new StringBuilder();
				sb.Append(contextName);
				sb.Append('.');
				sb.Append(recordName);
				AppendPrefix(record, sb);
				string groupName = sb.ToString();
				sb.Append('.');
				int sbBaseLen = sb.Length;
				string type = null;
				AbstractGangliaSink.GangliaSlope slopeFromMetric = null;
				AbstractGangliaSink.GangliaSlope calculatedSlope = null;
				MetricsCache.Record cachedMetrics = null;
				ResetBuffer();
				// reset the buffer to the beginning
				if (!IsSupportSparseMetrics())
				{
					// for sending dense metrics, update metrics cache
					// and get the updated data
					cachedMetrics = metricsCache.Update(record);
					if (cachedMetrics != null && cachedMetrics.MetricsEntrySet() != null)
					{
						foreach (KeyValuePair<string, AbstractMetric> entry in cachedMetrics.MetricsEntrySet
							())
						{
							AbstractMetric metric = entry.Value;
							sb.Append(metric.Name());
							string name = sb.ToString();
							// visit the metric to identify the Ganglia type and
							// slope
							metric.Visit(gangliaMetricVisitor);
							type = gangliaMetricVisitor.GetType();
							slopeFromMetric = gangliaMetricVisitor.GetSlope();
							GangliaConf gConf = GetGangliaConfForMetric(name);
							calculatedSlope = CalculateSlope(gConf, slopeFromMetric);
							// send metric to Ganglia
							EmitMetric(groupName, name, type, metric.Value().ToString(), gConf, calculatedSlope
								);
							// reset the length of the buffer for next iteration
							sb.Length = sbBaseLen;
						}
					}
				}
				else
				{
					// we support sparse updates
					ICollection<AbstractMetric> metrics = (ICollection<AbstractMetric>)record.Metrics
						();
					if (metrics.Count > 0)
					{
						// we got metrics. so send the latest
						foreach (AbstractMetric metric in record.Metrics())
						{
							sb.Append(metric.Name());
							string name = sb.ToString();
							// visit the metric to identify the Ganglia type and
							// slope
							metric.Visit(gangliaMetricVisitor);
							type = gangliaMetricVisitor.GetType();
							slopeFromMetric = gangliaMetricVisitor.GetSlope();
							GangliaConf gConf = GetGangliaConfForMetric(name);
							calculatedSlope = CalculateSlope(gConf, slopeFromMetric);
							// send metric to Ganglia
							EmitMetric(groupName, name, type, metric.Value().ToString(), gConf, calculatedSlope
								);
							// reset the length of the buffer for next iteration
							sb.Length = sbBaseLen;
						}
					}
				}
			}
			catch (IOException io)
			{
				throw new MetricsException("Failed to putMetrics", io);
			}
		}

		// Calculate the slope from properties and metric
		private AbstractGangliaSink.GangliaSlope CalculateSlope(GangliaConf gConf, AbstractGangliaSink.GangliaSlope
			 slopeFromMetric)
		{
			if (gConf.GetSlope() != null)
			{
				// if slope has been specified in properties, use that
				return gConf.GetSlope();
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
					return DefaultSlope;
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
		protected internal virtual void EmitMetric(string groupName, string name, string 
			type, string value, GangliaConf gConf, AbstractGangliaSink.GangliaSlope gSlope)
		{
			if (name == null)
			{
				Log.Warn("Metric was emitted with no name.");
				return;
			}
			else
			{
				if (value == null)
				{
					Log.Warn("Metric name " + name + " was emitted with a null value.");
					return;
				}
				else
				{
					if (type == null)
					{
						Log.Warn("Metric name " + name + ", value " + value + " has no type.");
						return;
					}
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Emitting metric " + name + ", type " + type + ", value " + value + ", slope "
					 + gSlope.ToString() + " from hostname " + GetHostName());
			}
			Xdr_int(0);
			// metric_user_defined
			Xdr_string(type);
			Xdr_string(name);
			Xdr_string(value);
			Xdr_string(gConf.GetUnits());
			Xdr_int((int)(gSlope));
			Xdr_int(gConf.GetTmax());
			Xdr_int(gConf.GetDmax());
			// send the metric to Ganglia hosts
			EmitToGangliaHosts();
		}
	}
}
