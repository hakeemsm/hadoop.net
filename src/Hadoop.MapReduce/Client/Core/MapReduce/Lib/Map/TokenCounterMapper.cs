using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	/// <summary>Tokenize the input values and emit each word with a count of 1.</summary>
	public class TokenCounterMapper : Mapper<object, Text, Text, IntWritable>
	{
		private static readonly IntWritable one = new IntWritable(1);

		private Text word = new Text();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Map(object key, Text value, Mapper.Context context
			)
		{
			StringTokenizer itr = new StringTokenizer(value.ToString());
			while (itr.HasMoreTokens())
			{
				word.Set(itr.NextToken());
				context.Write(word, one);
			}
		}
	}
}
