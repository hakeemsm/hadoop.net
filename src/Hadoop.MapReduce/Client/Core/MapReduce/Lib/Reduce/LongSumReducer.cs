using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Reduce
{
	public class LongSumReducer<Key> : Reducer<KEY, LongWritable, KEY, LongWritable>
	{
		private LongWritable result = new LongWritable();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Reduce(KEY key, IEnumerable<LongWritable> values
			, Reducer.Context context)
		{
			long sum = 0;
			foreach (LongWritable val in values)
			{
				sum += val.Get();
			}
			result.Set(sum);
			context.Write(key, result);
		}
	}
}
