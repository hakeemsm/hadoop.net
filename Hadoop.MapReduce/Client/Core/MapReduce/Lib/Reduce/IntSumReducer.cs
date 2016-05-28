using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Reduce
{
	public class IntSumReducer<Key> : Reducer<Key, IntWritable, Key, IntWritable>
	{
		private IntWritable result = new IntWritable();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Reduce(Key key, IEnumerable<IntWritable> values, 
			Reducer.Context context)
		{
			int sum = 0;
			foreach (IntWritable val in values)
			{
				sum += val.Get();
			}
			result.Set(sum);
			context.Write(key, result);
		}
	}
}
