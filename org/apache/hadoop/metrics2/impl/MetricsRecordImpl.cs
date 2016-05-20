using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricsRecordImpl : org.apache.hadoop.metrics2.impl.AbstractMetricsRecord
	{
		protected internal const string DEFAULT_CONTEXT = "default";

		private readonly long timestamp;

		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsTag
			> tags;

		private readonly System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			> metrics;

		/// <summary>Construct a metrics record</summary>
		/// <param name="info">
		/// 
		/// <see cref="MetricInfo"/>
		/// of the record
		/// </param>
		/// <param name="timestamp">of the record</param>
		/// <param name="tags">of the record</param>
		/// <param name="metrics">of the record</param>
		public MetricsRecordImpl(org.apache.hadoop.metrics2.MetricsInfo info, long timestamp
			, System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsTag> tags, 
			System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			> metrics)
		{
			this.timestamp = org.apache.hadoop.metrics2.util.Contracts.checkArg(timestamp, timestamp
				 > 0, "timestamp");
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "info");
			this.tags = com.google.common.@base.Preconditions.checkNotNull(tags, "tags");
			this.metrics = com.google.common.@base.Preconditions.checkNotNull(metrics, "metrics"
				);
		}

		public override long timestamp()
		{
			return timestamp;
		}

		public override string name()
		{
			return info.name();
		}

		internal virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		public override string description()
		{
			return info.description();
		}

		public override string context()
		{
			// usually the first tag
			foreach (org.apache.hadoop.metrics2.MetricsTag t in tags)
			{
				if (t.info() == org.apache.hadoop.metrics2.impl.MsInfo.Context)
				{
					return t.value();
				}
			}
			return DEFAULT_CONTEXT;
		}

		public override System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag
			> tags()
		{
			return tags;
		}

		// already unmodifiable from MetricsRecordBuilderImpl#tags
		public override System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			> metrics()
		{
			return metrics;
		}
	}
}
