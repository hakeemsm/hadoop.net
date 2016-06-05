using System.Collections;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>This class implements the generic mapper of Aggregate.</summary>
	public class ValueAggregatorMapper<K1, V1> : ValueAggregatorJobBase<K1, V1>
		where K1 : WritableComparable
		where V1 : Writable
	{
		/// <summary>the map function.</summary>
		/// <remarks>
		/// the map function. It iterates through the value aggregator descriptor
		/// list to generate aggregation id/value pairs and emit them.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Map(K1 key, V1 value, OutputCollector<Text, Text> output, Reporter
			 reporter)
		{
			IEnumerator iter = this.aggregatorDescriptorList.GetEnumerator();
			while (iter.HasNext())
			{
				ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor)iter.Next();
				IEnumerator<KeyValuePair<Text, Text>> ens = ad.GenerateKeyValPairs(key, value).GetEnumerator
					();
				while (ens.HasNext())
				{
					KeyValuePair<Text, Text> en = ens.Next();
					output.Collect(en.Key, en.Value);
				}
			}
		}

		/// <summary>Do nothing.</summary>
		/// <remarks>Do nothing. Should not be called.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Reduce(Text arg0, IEnumerator<Text> arg1, OutputCollector<Text
			, Text> arg2, Reporter arg3)
		{
			throw new IOException("should not be called\n");
		}
	}
}
