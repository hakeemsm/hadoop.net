using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class MetricsCollectorImpl : MetricsCollector, IEnumerable<MetricsRecordBuilderImpl
		>
	{
		private readonly IList<MetricsRecordBuilderImpl> rbs = Lists.NewArrayList();

		private MetricsFilter recordFilter;

		private MetricsFilter metricFilter;

		public virtual MetricsRecordBuilderImpl AddRecord(MetricsInfo info)
		{
			bool acceptable = recordFilter == null || recordFilter.Accepts(info.Name());
			MetricsRecordBuilderImpl rb = new MetricsRecordBuilderImpl(this, info, recordFilter
				, metricFilter, acceptable);
			if (acceptable)
			{
				rbs.AddItem(rb);
			}
			return rb;
		}

		public virtual MetricsRecordBuilderImpl AddRecord(string name)
		{
			return AddRecord(Interns.Info(name, name + " record"));
		}

		public virtual IList<MetricsRecordImpl> GetRecords()
		{
			IList<MetricsRecordImpl> recs = Lists.NewArrayListWithCapacity(rbs.Count);
			foreach (MetricsRecordBuilderImpl rb in rbs)
			{
				MetricsRecordImpl mr = rb.GetRecord();
				if (mr != null)
				{
					recs.AddItem(mr);
				}
			}
			return recs;
		}

		public virtual IEnumerator<MetricsRecordBuilderImpl> GetEnumerator()
		{
			return rbs.GetEnumerator();
		}

		[InterfaceAudience.Private]
		public virtual void Clear()
		{
			rbs.Clear();
		}

		internal virtual MetricsCollectorImpl SetRecordFilter(MetricsFilter rf)
		{
			recordFilter = rf;
			return this;
		}

		internal virtual MetricsCollectorImpl SetMetricFilter(MetricsFilter mf)
		{
			metricFilter = mf;
			return this;
		}
	}
}
