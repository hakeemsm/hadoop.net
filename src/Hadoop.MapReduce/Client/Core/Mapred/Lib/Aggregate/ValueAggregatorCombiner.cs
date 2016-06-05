using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>This class implements the generic combiner of Aggregate.</summary>
	public class ValueAggregatorCombiner<K1, V1> : ValueAggregatorJobBase<K1, V1>
		where K1 : WritableComparable
		where V1 : Writable
	{
		/// <summary>Combiner does not need to configure.</summary>
		public override void Configure(JobConf job)
		{
		}

		/// <summary>Combines values for a given key.</summary>
		/// <param name="key">
		/// the key is expected to be a Text object, whose prefix indicates
		/// the type of aggregation to aggregate the values.
		/// </param>
		/// <param name="values">the values to combine</param>
		/// <param name="output">to collect combined values</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
			, Text> output, Reporter reporter)
		{
			string keyStr = key.ToString();
			int pos = keyStr.IndexOf(ValueAggregatorDescriptor.TypeSeparator);
			string type = Sharpen.Runtime.Substring(keyStr, 0, pos);
			ValueAggregator aggregator = ValueAggregatorBaseDescriptor.GenerateValueAggregator
				(type);
			while (values.HasNext())
			{
				aggregator.AddNextValue(values.Next());
			}
			IEnumerator outputs = aggregator.GetCombinerOutput().GetEnumerator();
			while (outputs.HasNext())
			{
				object v = outputs.Next();
				if (v is Text)
				{
					output.Collect(key, (Text)v);
				}
				else
				{
					output.Collect(key, new Text(v.ToString()));
				}
			}
		}

		/// <summary>Do nothing.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
		}

		/// <summary>Do nothing.</summary>
		/// <remarks>Do nothing. Should not be called.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Map(K1 arg0, V1 arg1, OutputCollector<Text, Text> arg2, Reporter
			 arg3)
		{
			throw new IOException("should not be called\n");
		}
	}
}
