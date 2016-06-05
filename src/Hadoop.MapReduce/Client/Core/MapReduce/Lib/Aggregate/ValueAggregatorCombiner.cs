using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>This class implements the generic combiner of Aggregate.</summary>
	public class ValueAggregatorCombiner<K1, V1> : Reducer<Text, Text, Text, Text>
		where K1 : WritableComparable<object>
		where V1 : Writable
	{
		/// <summary>Combines values for a given key.</summary>
		/// <param name="key">
		/// the key is expected to be a Text object, whose prefix indicates
		/// the type of aggregation to aggregate the values.
		/// </param>
		/// <param name="values">the values to combine</param>
		/// <param name="context">to collect combined values</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Reduce(Text key, IEnumerable<Text> values, Reducer.Context
			 context)
		{
			string keyStr = key.ToString();
			int pos = keyStr.IndexOf(ValueAggregatorDescriptor.TypeSeparator);
			string type = Sharpen.Runtime.Substring(keyStr, 0, pos);
			long uniqCount = context.GetConfiguration().GetLong(UniqValueCount.MaxNumUniqueValues
				, long.MaxValue);
			ValueAggregator aggregator = ValueAggregatorBaseDescriptor.GenerateValueAggregator
				(type, uniqCount);
			foreach (Text val in values)
			{
				aggregator.AddNextValue(val);
			}
			IEnumerator<object> outputs = aggregator.GetCombinerOutput().GetEnumerator();
			while (outputs.HasNext())
			{
				object v = outputs.Next();
				if (v is Text)
				{
					context.Write(key, (Text)v);
				}
				else
				{
					context.Write(key, new Text(v.ToString()));
				}
			}
		}
	}
}
