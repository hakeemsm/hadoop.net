using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This abstract class implements some common functionalities of the
	/// the generic mapper, reducer and combiner classes of Aggregate.
	/// </summary>
	public abstract class ValueAggregatorJobBase<K1, V1> : Mapper<K1, V1, Text, Text>
		, Reducer<Text, Text, Text, Text>
		where K1 : WritableComparable
		where V1 : Writable
	{
		protected internal AList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;

		public virtual void Configure(JobConf job)
		{
			this.InitializeMySpec(job);
			this.LogSpec();
		}

		private static ValueAggregatorDescriptor GetValueAggregatorDescriptor(string spec
			, JobConf job)
		{
			if (spec == null)
			{
				return null;
			}
			string[] segments = spec.Split(",", -1);
			string type = segments[0];
			if (type.CompareToIgnoreCase("UserDefined") == 0)
			{
				string className = segments[1];
				return new UserDefinedValueAggregatorDescriptor(className, job);
			}
			return null;
		}

		private static AList<ValueAggregatorDescriptor> GetAggregatorDescriptors(JobConf 
			job)
		{
			string advn = "aggregator.descriptor";
			int num = job.GetInt(advn + ".num", 0);
			AList<ValueAggregatorDescriptor> retv = new AList<ValueAggregatorDescriptor>(num);
			for (int i = 0; i < num; i++)
			{
				string spec = job.Get(advn + "." + i);
				ValueAggregatorDescriptor ad = GetValueAggregatorDescriptor(spec, job);
				if (ad != null)
				{
					retv.AddItem(ad);
				}
			}
			return retv;
		}

		private void InitializeMySpec(JobConf job)
		{
			this.aggregatorDescriptorList = GetAggregatorDescriptors(job);
			if (this.aggregatorDescriptorList.Count == 0)
			{
				this.aggregatorDescriptorList.AddItem(new UserDefinedValueAggregatorDescriptor(typeof(
					ValueAggregatorBaseDescriptor).GetCanonicalName(), job));
			}
		}

		protected internal virtual void LogSpec()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		public abstract void Map(K1 arg1, V1 arg2, OutputCollector<Text, Text> arg3, Reporter
			 arg4);

		public abstract void Reduce(Text arg1, IEnumerator<Text> arg2, OutputCollector<Text
			, Text> arg3, Reporter arg4);
	}
}
