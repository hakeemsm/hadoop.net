using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// that extracts text matching a regular expression.
	/// </summary>
	public class RegexMapper<K> : Mapper<K, Text, Text, LongWritable>
	{
		public static string Pattern = "mapreduce.mapper.regex";

		public static string Group = "mapreduce.mapper.regexmapper..group";

		private Sharpen.Pattern pattern;

		private int group;

		protected internal override void Setup(Mapper.Context context)
		{
			Configuration conf = context.GetConfiguration();
			pattern = Sharpen.Pattern.Compile(conf.Get(Pattern));
			group = conf.GetInt(Group, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Map(K key, Text value, Mapper.Context context)
		{
			string text = value.ToString();
			Matcher matcher = pattern.Matcher(text);
			while (matcher.Find())
			{
				context.Write(new Text(matcher.Group(group)), new LongWritable(1));
			}
		}
	}
}
