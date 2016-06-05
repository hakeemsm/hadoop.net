using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// that extracts text matching a regular expression.
	/// </summary>
	public class RegexMapper<K> : MapReduceBase, Mapper<K, Text, Text, LongWritable>
	{
		private Sharpen.Pattern pattern;

		private int group;

		public override void Configure(JobConf job)
		{
			pattern = Sharpen.Pattern.Compile(job.Get(RegexMapper.Pattern));
			group = job.GetInt(RegexMapper.Group, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K key, Text value, OutputCollector<Text, LongWritable> output
			, Reporter reporter)
		{
			string text = value.ToString();
			Matcher matcher = pattern.Matcher(text);
			while (matcher.Find())
			{
				output.Collect(new Text(matcher.Group(group)), new LongWritable(1));
			}
		}
	}
}
