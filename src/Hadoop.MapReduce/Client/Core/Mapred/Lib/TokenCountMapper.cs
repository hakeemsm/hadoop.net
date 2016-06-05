using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// that maps text values into &lt;token,freq&gt; pairs.  Uses
	/// <see cref="Sharpen.StringTokenizer"/>
	/// to break text into tokens.
	/// </summary>
	public class TokenCountMapper<K> : MapReduceBase, Mapper<K, Text, Text, LongWritable
		>
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K key, Text value, OutputCollector<Text, LongWritable> output
			, Reporter reporter)
		{
			// get input text
			string text = value.ToString();
			// value is line of text
			// tokenize the value
			StringTokenizer st = new StringTokenizer(text);
			while (st.HasMoreTokens())
			{
				// output <token,1> pairs
				output.Collect(new Text(st.NextToken()), new LongWritable(1));
			}
		}
	}
}
