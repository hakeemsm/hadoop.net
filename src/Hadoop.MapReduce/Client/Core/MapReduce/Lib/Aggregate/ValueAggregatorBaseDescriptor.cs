using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements the common functionalities of
	/// the subclasses of ValueAggregatorDescriptor class.
	/// </summary>
	public class ValueAggregatorBaseDescriptor : ValueAggregatorDescriptor
	{
		public const string UniqValueCount = "UniqValueCount";

		public const string LongValueSum = "LongValueSum";

		public const string DoubleValueSum = "DoubleValueSum";

		public const string ValueHistogram = "ValueHistogram";

		public const string LongValueMax = "LongValueMax";

		public const string LongValueMin = "LongValueMin";

		public const string StringValueMax = "StringValueMax";

		public const string StringValueMin = "StringValueMin";

		public string inputFile = null;

		private class MyEntry : KeyValuePair<Text, Text>
		{
			internal Text key;

			internal Text val;

			public virtual Text Key
			{
				get
				{
					return key;
				}
			}

			public virtual Text Value
			{
				get
				{
					return val;
				}
			}

			public virtual Text SetValue(Text val)
			{
				this.val = val;
				return val;
			}

			public MyEntry(Text key, Text val)
			{
				this.key = key;
				this.val = val;
			}
		}

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
			Text key = new Text(type + TypeSeparator + id);
			return new ValueAggregatorBaseDescriptor.MyEntry(key, val);
		}

		/// <param name="type">the aggregation type</param>
		/// <param name="uniqCount">
		/// the limit in the number of unique values to keep,
		/// if type is UNIQ_VALUE_COUNT
		/// </param>
		/// <returns>a value aggregator of the given type.</returns>
		public static ValueAggregator GenerateValueAggregator(string type, long uniqCount
			)
		{
			if (type.CompareToIgnoreCase(LongValueSum) == 0)
			{
				return new LongValueSum();
			}
			if (type.CompareToIgnoreCase(LongValueMax) == 0)
			{
				return new LongValueMax();
			}
			else
			{
				if (type.CompareToIgnoreCase(LongValueMin) == 0)
				{
					return new LongValueMin();
				}
				else
				{
					if (type.CompareToIgnoreCase(StringValueMax) == 0)
					{
						return new StringValueMax();
					}
					else
					{
						if (type.CompareToIgnoreCase(StringValueMin) == 0)
						{
							return new StringValueMin();
						}
						else
						{
							if (type.CompareToIgnoreCase(DoubleValueSum) == 0)
							{
								return new DoubleValueSum();
							}
							else
							{
								if (type.CompareToIgnoreCase(UniqValueCount) == 0)
								{
									return new UniqValueCount(uniqCount);
								}
								else
								{
									if (type.CompareToIgnoreCase(ValueHistogram) == 0)
									{
										return new ValueHistogram();
									}
								}
							}
						}
					}
				}
			}
			return null;
		}

		/// <summary>Generate 1 or 2 aggregation-id/value pairs for the given key/value pair.
		/// 	</summary>
		/// <remarks>
		/// Generate 1 or 2 aggregation-id/value pairs for the given key/value pair.
		/// The first id will be of type LONG_VALUE_SUM, with "record_count" as
		/// its aggregation id. If the input is a file split,
		/// the second id of the same type will be generated too, with the file name
		/// as its aggregation id. This achieves the behavior of counting the total
		/// number of records in the input data, and the number of records
		/// in each input file.
		/// </remarks>
		/// <param name="key">input key</param>
		/// <param name="val">input value</param>
		/// <returns>
		/// a list of aggregation id/value pairs. An aggregation id encodes an
		/// aggregation type which is used to guide the way to aggregate the
		/// value in the reduce/combiner phrase of an Aggregate based job.
		/// </returns>
		public override AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
			 val)
		{
			AList<KeyValuePair<Text, Text>> retv = new AList<KeyValuePair<Text, Text>>();
			string countType = LongValueSum;
			string id = "record_count";
			KeyValuePair<Text, Text> e = GenerateEntry(countType, id, One);
			if (e != null)
			{
				retv.AddItem(e);
			}
			if (this.inputFile != null)
			{
				e = GenerateEntry(countType, this.inputFile, One);
				if (e != null)
				{
					retv.AddItem(e);
				}
			}
			return retv;
		}

		/// <summary>get the input file name.</summary>
		/// <param name="conf">a configuration object</param>
		public override void Configure(Configuration conf)
		{
			this.inputFile = conf.Get(MRJobConfig.MapInputFile);
		}
	}
}
