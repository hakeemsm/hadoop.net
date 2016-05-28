using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapred.Reducer{K2, V2, K3, V3}"/>
	/// that sums long values.
	/// </summary>
	public class LongSumReducer<K> : MapReduceBase, Reducer<K, LongWritable, K, LongWritable
		>
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(K key, IEnumerator<LongWritable> values, OutputCollector
			<K, LongWritable> output, Reporter reporter)
		{
			// sum all values for this key
			long sum = 0;
			while (values.HasNext())
			{
				sum += values.Next().Get();
			}
			// output sum
			output.Collect(key, new LongWritable(sum));
		}
	}
}
