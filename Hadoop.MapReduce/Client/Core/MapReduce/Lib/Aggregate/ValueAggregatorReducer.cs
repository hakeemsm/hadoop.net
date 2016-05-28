using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>This class implements the generic reducer of Aggregate.</summary>
	public class ValueAggregatorReducer<K1, V1> : Reducer<Text, Text, Text, Text>
		where K1 : WritableComparable<object>
		where V1 : Writable
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Setup(Reducer.Context context)
		{
			ValueAggregatorJobBase.Setup(context.GetConfiguration());
		}

		/// <param name="key">
		/// the key is expected to be a Text object, whose prefix indicates
		/// the type of aggregation to aggregate the values. In effect, data
		/// driven computing is achieved. It is assumed that each aggregator's
		/// getReport method emits appropriate output for the aggregator. This
		/// may be further customized.
		/// </param>
		/// <param name="values">the values to be aggregated</param>
		/// <param name="context"></param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Reduce(Text key, IEnumerable<Text> values, Reducer.Context
			 context)
		{
			string keyStr = key.ToString();
			int pos = keyStr.IndexOf(ValueAggregatorDescriptor.TypeSeparator);
			string type = Sharpen.Runtime.Substring(keyStr, 0, pos);
			keyStr = Sharpen.Runtime.Substring(keyStr, pos + ValueAggregatorDescriptor.TypeSeparator
				.Length);
			long uniqCount = context.GetConfiguration().GetLong(UniqValueCount.MaxNumUniqueValues
				, long.MaxValue);
			ValueAggregator aggregator = ValueAggregatorBaseDescriptor.GenerateValueAggregator
				(type, uniqCount);
			foreach (Text value in values)
			{
				aggregator.AddNextValue(value);
			}
			string val = aggregator.GetReport();
			key = new Text(keyStr);
			context.Write(key, new Text(val));
		}
	}
}
