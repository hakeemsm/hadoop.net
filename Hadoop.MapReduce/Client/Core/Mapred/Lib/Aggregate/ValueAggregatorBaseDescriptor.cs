using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements the common functionalities of
	/// the subclasses of ValueAggregatorDescriptor class.
	/// </summary>
	public class ValueAggregatorBaseDescriptor : ValueAggregatorBaseDescriptor, ValueAggregatorDescriptor
	{
		public const string UniqValueCount = ValueAggregatorBaseDescriptor.UniqValueCount;

		public const string LongValueSum = ValueAggregatorBaseDescriptor.LongValueSum;

		public const string DoubleValueSum = ValueAggregatorBaseDescriptor.DoubleValueSum;

		public const string ValueHistogram = ValueAggregatorBaseDescriptor.ValueHistogram;

		public const string LongValueMax = ValueAggregatorBaseDescriptor.LongValueMax;

		public const string LongValueMin = ValueAggregatorBaseDescriptor.LongValueMin;

		public const string StringValueMax = ValueAggregatorBaseDescriptor.StringValueMax;

		public const string StringValueMin = ValueAggregatorBaseDescriptor.StringValueMin;

		private static long maxNumItems = long.MaxValue;

		/// <param name="type">the aggregation type</param>
		/// <param name="id">the aggregation id</param>
		/// <param name="val">the val associated with the id to be aggregated</param>
		/// <returns>
		/// an Entry whose key is the aggregation id prefixed with
		/// the aggregation type.
		/// </returns>
		public static KeyValuePair<Text, Text> GenerateEntry(string type, string id, Text
			 val)
		{
			return ValueAggregatorBaseDescriptor.GenerateEntry(type, id, val);
		}

		/// <param name="type">the aggregation type</param>
		/// <returns>a value aggregator of the given type.</returns>
		public static ValueAggregator GenerateValueAggregator(string type)
		{
			ValueAggregator retv = null;
			if (type.CompareToIgnoreCase(LongValueSum) == 0)
			{
				retv = new LongValueSum();
			}
			if (type.CompareToIgnoreCase(LongValueMax) == 0)
			{
				retv = new LongValueMax();
			}
			else
			{
				if (type.CompareToIgnoreCase(LongValueMin) == 0)
				{
					retv = new LongValueMin();
				}
				else
				{
					if (type.CompareToIgnoreCase(StringValueMax) == 0)
					{
						retv = new StringValueMax();
					}
					else
					{
						if (type.CompareToIgnoreCase(StringValueMin) == 0)
						{
							retv = new StringValueMin();
						}
						else
						{
							if (type.CompareToIgnoreCase(DoubleValueSum) == 0)
							{
								retv = new DoubleValueSum();
							}
							else
							{
								if (type.CompareToIgnoreCase(UniqValueCount) == 0)
								{
									retv = new UniqValueCount(maxNumItems);
								}
								else
								{
									if (type.CompareToIgnoreCase(ValueHistogram) == 0)
									{
										retv = new ValueHistogram();
									}
								}
							}
						}
					}
				}
			}
			return retv;
		}

		/// <summary>get the input file name.</summary>
		/// <param name="job">a job configuration object</param>
		public override void Configure(JobConf job)
		{
			base.Configure(job);
			maxNumItems = job.GetLong("aggregate.max.num.unique.values", long.MaxValue);
		}
	}
}
