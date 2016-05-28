using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	public class WordCount
	{
		public class TokenizerMapper : Mapper<object, Text, Text, IntWritable>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Text value, Mapper.Context context)
			{
				StringTokenizer itr = new StringTokenizer(value.ToString());
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					context.Write(word, one);
				}
			}
		}

		public class IntSumReducer : Reducer<Text, IntWritable, Text, IntWritable>
		{
			private IntWritable result = new IntWritable();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<IntWritable> values, Reducer.Context
				 context)
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

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration conf = new Configuration();
			string[] otherArgs = new GenericOptionsParser(conf, args).GetRemainingArgs();
			if (otherArgs.Length < 2)
			{
				System.Console.Error.WriteLine("Usage: wordcount <in> [<in>...] <out>");
				System.Environment.Exit(2);
			}
			Job job = Job.GetInstance(conf, "word count");
			job.SetJarByClass(typeof(WordCount));
			job.SetMapperClass(typeof(WordCount.TokenizerMapper));
			job.SetCombinerClass(typeof(WordCount.IntSumReducer));
			job.SetReducerClass(typeof(WordCount.IntSumReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			for (int i = 0; i < otherArgs.Length - 1; ++i)
			{
				FileInputFormat.AddInputPath(job, new Path(otherArgs[i]));
			}
			FileOutputFormat.SetOutputPath(job, new Path(otherArgs[otherArgs.Length - 1]));
			System.Environment.Exit(job.WaitForCompletion(true) ? 0 : 1);
		}
	}
}
