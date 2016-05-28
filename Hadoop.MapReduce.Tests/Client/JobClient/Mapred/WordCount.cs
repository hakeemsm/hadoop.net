using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>This is an example Hadoop Map/Reduce application.</summary>
	/// <remarks>
	/// This is an example Hadoop Map/Reduce application.
	/// It reads the text input files, breaks each line into words
	/// and counts them. The output is a locally sorted list of words and the
	/// count of how often they occurred.
	/// To run: bin/hadoop jar build/hadoop-examples.jar wordcount
	/// [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
	/// </remarks>
	public class WordCount : Configured, Tool
	{
		/// <summary>Counts the words in each line.</summary>
		/// <remarks>
		/// Counts the words in each line.
		/// For each line of input, break the line into words and emit them as
		/// (<b>word</b>, <b>1</b>).
		/// </remarks>
		public class MapClass : MapReduceBase, Mapper<LongWritable, Text, Text, IntWritable
			>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text value, OutputCollector<Text, IntWritable
				> output, Reporter reporter)
			{
				string line = value.ToString();
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					output.Collect(word, one);
				}
			}
		}

		/// <summary>A reducer class that just emits the sum of the input values.</summary>
		public class Reduce : MapReduceBase, Reducer<Text, IntWritable, Text, IntWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Text key, IEnumerator<IntWritable> values, OutputCollector
				<Text, IntWritable> output, Reporter reporter)
			{
				int sum = 0;
				while (values.HasNext())
				{
					sum += values.Next().Get();
				}
				output.Collect(key, new IntWritable(sum));
			}
		}

		internal static int PrintUsage()
		{
			System.Console.Out.WriteLine("wordcount [-m <maps>] [-r <reduces>] <input> <output>"
				);
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			return -1;
		}

		/// <summary>The main driver for word count map/reduce program.</summary>
		/// <remarks>
		/// The main driver for word count map/reduce program.
		/// Invoke this method to submit the map/reduce job.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// When there is communication problems with the
		/// job tracker.
		/// </exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			JobConf conf = new JobConf(GetConf(), typeof(WordCount));
			conf.SetJobName("wordcount");
			// the keys are words (strings)
			conf.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			conf.SetOutputValueClass(typeof(IntWritable));
			conf.SetMapperClass(typeof(WordCount.MapClass));
			conf.SetCombinerClass(typeof(WordCount.Reduce));
			conf.SetReducerClass(typeof(WordCount.Reduce));
			IList<string> other_args = new AList<string>();
			for (int i = 0; i < args.Length; ++i)
			{
				try
				{
					if ("-m".Equals(args[i]))
					{
						conf.SetNumMapTasks(System.Convert.ToInt32(args[++i]));
					}
					else
					{
						if ("-r".Equals(args[i]))
						{
							conf.SetNumReduceTasks(System.Convert.ToInt32(args[++i]));
						}
						else
						{
							other_args.AddItem(args[i]);
						}
					}
				}
				catch (FormatException)
				{
					System.Console.Out.WriteLine("ERROR: Integer expected instead of " + args[i]);
					return PrintUsage();
				}
				catch (IndexOutOfRangeException)
				{
					System.Console.Out.WriteLine("ERROR: Required parameter missing from " + args[i -
						 1]);
					return PrintUsage();
				}
			}
			// Make sure there are exactly 2 parameters left.
			if (other_args.Count != 2)
			{
				System.Console.Out.WriteLine("ERROR: Wrong number of parameters: " + other_args.Count
					 + " instead of 2.");
				return PrintUsage();
			}
			FileInputFormat.SetInputPaths(conf, other_args[0]);
			FileOutputFormat.SetOutputPath(conf, new Path(other_args[1]));
			JobClient.RunJob(conf);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new WordCount(), args);
			System.Environment.Exit(res);
		}
	}
}
