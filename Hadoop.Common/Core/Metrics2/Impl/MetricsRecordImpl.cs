using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricsRecordImpl : AbstractMetricsRecord
	{
		protected internal const string DefaultContext = "default";

		private readonly long timestamp;

		private readonly MetricsInfo info;

		private readonly IList<MetricsTag> tags;

		private readonly IEnumerable<AbstractMetric> metrics;

		/// <summary>Construct a metrics record</summary>
		/// <param name="info">
		/// 
		/// <see cref="MetricInfo"/>
		/// of the record
		/// </param>
		/// <param name="timestamp">of the record</param>
		/// <param name="tags">of the record</param>
		/// <param name="metrics">of the record</param>
		public MetricsRecordImpl(MetricsInfo info, long timestamp, IList<MetricsTag> tags
			, IEnumerable<AbstractMetric> metrics)
		{
			this.timestamp = Contracts.CheckArg(timestamp, timestamp > 0, "timestamp");
			this.info = Preconditions.CheckNotNull(info, "info");
			this.tags = Preconditions.CheckNotNull(tags, "tags");
			this.metrics = Preconditions.CheckNotNull(metrics, "metrics");
		}

		public override long Timestamp()
		{
			return timestamp;
		}

		public override string Name()
		{
			return info.Name();
		}

		internal virtual MetricsInfo Info()
		{
			return info;
		}

		public override string Description()
		{
			return info.Description();
		}

		public override string Context()
		{
			// usually the first tag
			foreach (MetricsTag t in tags)
			{
				if (t.Info() == MsInfo.Context)
				{
					return t.Value();
				}
			}
			return DefaultContext;
		}

		public override ICollection<MetricsTag> Tags()
		{
			return tags;
		}

		// already unmodifiable from MetricsRecordBuilderImpl#tags
		public override IEnumerable<AbstractMetric> Metrics()
		{
			return metrics;
		}
	}
}
