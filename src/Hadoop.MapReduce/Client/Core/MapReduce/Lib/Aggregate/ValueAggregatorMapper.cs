using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>This class implements the generic mapper of Aggregate.</summary>
	public class ValueAggregatorMapper<K1, V1> : Mapper<K1, V1, Text, Text>
		where K1 : WritableComparable<object>
		where V1 : Writable
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Setup(Mapper.Context context)
		{
			ValueAggregatorJobBase.Setup(context.GetConfiguration());
		}

		/// <summary>the map function.</summary>
		/// <remarks>
		/// the map function. It iterates through the value aggregator descriptor
		/// list to generate aggregation id/value pairs and emit them.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Map(K1 key, V1 value, Mapper.Context context)
		{
			IEnumerator<object> iter = ValueAggregatorJobBase.aggregatorDescriptorList.GetEnumerator
				();
			while (iter.HasNext())
			{
				ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor)iter.Next();
				IEnumerator<KeyValuePair<Text, Text>> ens = ad.GenerateKeyValPairs(key, value).GetEnumerator
					();
				while (ens.HasNext())
				{
					KeyValuePair<Text, Text> en = ens.Next();
					context.Write(en.Key, en.Value);
				}
			}
		}
	}
}
