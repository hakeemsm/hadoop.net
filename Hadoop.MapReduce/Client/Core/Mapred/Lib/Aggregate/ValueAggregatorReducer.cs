using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>This class implements the generic reducer of Aggregate.</summary>
	public class ValueAggregatorReducer<K1, V1> : ValueAggregatorJobBase<K1, V1>
		where K1 : WritableComparable
		where V1 : Writable
	{
		/// <param name="key">
		/// the key is expected to be a Text object, whose prefix indicates
		/// the type of aggregation to aggregate the values. In effect, data
		/// driven computing is achieved. It is assumed that each aggregator's
		/// getReport method emits appropriate output for the aggregator. This
		/// may be further customiized.
		/// </param>
		/// <param name="values">the values to be aggregated</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
			, Text> output, Reporter reporter)
		{
			string keyStr = key.ToString();
			int pos = keyStr.IndexOf(ValueAggregatorDescriptor.TypeSeparator);
			string type = Sharpen.Runtime.Substring(keyStr, 0, pos);
			keyStr = Sharpen.Runtime.Substring(keyStr, pos + ValueAggregatorDescriptor.TypeSeparator
				.Length);
			ValueAggregator aggregator = ValueAggregatorBaseDescriptor.GenerateValueAggregator
				(type);
			while (values.HasNext())
			{
				aggregator.AddNextValue(values.Next());
			}
			string val = aggregator.GetReport();
			key = new Text(keyStr);
			output.Collect(key, new Text(val));
		}

		/// <summary>Do nothing.</summary>
		/// <remarks>Do nothing. Should not be called</remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Map(K1 arg0, V1 arg1, OutputCollector<Text, Text> arg2, Reporter
			 arg3)
		{
			throw new IOException("should not be called\n");
		}
	}
}
