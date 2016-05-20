using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	public class MetricsCollectorImpl : org.apache.hadoop.metrics2.MetricsCollector, 
		System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl
		>
	{
		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl
			> rbs = com.google.common.collect.Lists.newArrayList();

		private org.apache.hadoop.metrics2.MetricsFilter recordFilter;

		private org.apache.hadoop.metrics2.MetricsFilter metricFilter;

		public virtual org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl addRecord
			(org.apache.hadoop.metrics2.MetricsInfo info)
		{
			bool acceptable = recordFilter == null || recordFilter.accepts(info.name());
			org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl rb = new org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl
				(this, info, recordFilter, metricFilter, acceptable);
			if (acceptable)
			{
				rbs.add(rb);
			}
			return rb;
		}

		public virtual org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl addRecord
			(string name)
		{
			return addRecord(org.apache.hadoop.metrics2.lib.Interns.info(name, name + " record"
				));
		}

		public virtual System.Collections.Generic.IList<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
			> getRecords()
		{
			System.Collections.Generic.IList<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
				> recs = com.google.common.collect.Lists.newArrayListWithCapacity(rbs.Count);
			foreach (org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl rb in rbs)
			{
				org.apache.hadoop.metrics2.impl.MetricsRecordImpl mr = rb.getRecord();
				if (mr != null)
				{
					recs.add(mr);
				}
			}
			return recs;
		}

		public virtual System.Collections.Generic.IEnumerator<org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl
			> GetEnumerator()
		{
			return rbs.GetEnumerator();
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void clear()
		{
			rbs.clear();
		}

		internal virtual org.apache.hadoop.metrics2.impl.MetricsCollectorImpl setRecordFilter
			(org.apache.hadoop.metrics2.MetricsFilter rf)
		{
			recordFilter = rf;
			return this;
		}

		internal virtual org.apache.hadoop.metrics2.impl.MetricsCollectorImpl setMetricFilter
			(org.apache.hadoop.metrics2.MetricsFilter mf)
		{
			metricFilter = mf;
			return this;
		}
	}
}
