using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
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
	public class WordMedian : Configured, Tool
	{
		private double median = 0;

		private static readonly IntWritable One = new IntWritable(1);

		/// <summary>
		/// Maps words from line of text into a key-value pair; the length of the word
		/// as the key, and 1 as the value.
		/// </summary>
		public class WordMedianMapper : Mapper<object, Text, IntWritable, IntWritable>
		{
			private IntWritable length = new IntWritable();

			/// <summary>Emits a key-value pair for counting the word.</summary>
			/// <remarks>
			/// Emits a key-value pair for counting the word. Outputs are (IntWritable,
			/// IntWritable).
			/// </remarks>
			/// <param name="value">This will be a line of text coming in from our input file.</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Text value, Mapper.Context context)
			{
				StringTokenizer itr = new StringTokenizer(value.ToString());
				while (itr.HasMoreTokens())
				{
					string @string = itr.NextToken();
					length.Set(@string.Length);
					context.Write(length, One);
				}
			}
		}

		/// <summary>Performs integer summation of all the values for each key.</summary>
		public class WordMedianReducer : Reducer<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			private IntWritable val = new IntWritable();

			/// <summary>
			/// Sums all the individual values within the iterator and writes them to the
			/// same key.
			/// </summary>
			/// <param name="key">This will be a length of a word that was read.</param>
			/// <param name="values">
			/// This will be an iterator of all the values associated with that
			/// key.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(IntWritable key, IEnumerable<IntWritable> values, 
				Reducer.Context context)
			{
				int sum = 0;
				foreach (IntWritable value in values)
				{
					sum += value.Get();
				}
				val.Set(sum);
				context.Write(key, val);
			}
		}

		/// <summary>
		/// This is a standard program to read and find a median value based on a file
		/// of word counts such as: 1 456, 2 132, 3 56...
		/// </summary>
		/// <remarks>
		/// This is a standard program to read and find a median value based on a file
		/// of word counts such as: 1 456, 2 132, 3 56... Where the first values are
		/// the word lengths and the following values are the number of times that
		/// words of that length appear.
		/// </remarks>
		/// <param name="path">The path to read the HDFS file from (part-r-00000...00001...etc).
		/// 	</param>
		/// <param name="medianIndex1">The first length value to look for.</param>
		/// <param name="medianIndex2">
		/// The second length value to look for (will be the same as the first
		/// if there are an even number of words total).
		/// </param>
		/// <exception cref="System.IO.IOException">If file cannot be found, we throw an exception.
		/// 	</exception>
		private double ReadAndFindMedian(string path, int medianIndex1, int medianIndex2, 
			Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			Path file = new Path(path, "part-r-00000");
			if (!fs.Exists(file))
			{
				throw new IOException("Output not found!");
			}
			BufferedReader br = null;
			try
			{
				br = new BufferedReader(new InputStreamReader(fs.Open(file), Charsets.Utf8));
				int num = 0;
				string line;
				while ((line = br.ReadLine()) != null)
				{
					StringTokenizer st = new StringTokenizer(line);
					// grab length
					string currLen = st.NextToken();
					// grab count
					string lengthFreq = st.NextToken();
					int prevNum = num;
					num += System.Convert.ToInt32(lengthFreq);
					if (medianIndex2 >= prevNum && medianIndex1 <= num)
					{
						System.Console.Out.WriteLine("The median is: " + currLen);
						br.Close();
						return double.ParseDouble(currLen);
					}
					else
					{
						if (medianIndex2 >= prevNum && medianIndex1 < num)
						{
							string nextCurrLen = st.NextToken();
							double theMedian = (System.Convert.ToInt32(currLen) + System.Convert.ToInt32(nextCurrLen
								)) / 2.0;
							System.Console.Out.WriteLine("The median is: " + theMedian);
							br.Close();
							return theMedian;
						}
					}
				}
			}
			finally
			{
				if (br != null)
				{
					br.Close();
				}
			}
			// error, no median found
			return -1;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			ToolRunner.Run(new Configuration(), new WordMedian(), args);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: wordmedian <in> <out>");
				return 0;
			}
			SetConf(new Configuration());
			Configuration conf = GetConf();
			Job job = Job.GetInstance(conf, "word median");
			job.SetJarByClass(typeof(WordMedian));
			job.SetMapperClass(typeof(WordMedian.WordMedianMapper));
			job.SetCombinerClass(typeof(WordMedian.WordMedianReducer));
			job.SetReducerClass(typeof(WordMedian.WordMedianReducer));
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(IntWritable));
			FileInputFormat.AddInputPath(job, new Path(args[0]));
			FileOutputFormat.SetOutputPath(job, new Path(args[1]));
			bool result = job.WaitForCompletion(true);
			// Wait for JOB 1 -- get middle value to check for Median
			long totalWords = job.GetCounters().GetGroup(typeof(TaskCounter).GetCanonicalName
				()).FindCounter("MAP_OUTPUT_RECORDS", "Map output records").GetValue();
			int medianIndex1 = (int)Math.Ceil((totalWords / 2.0));
			int medianIndex2 = (int)Math.Floor((totalWords / 2.0));
			median = ReadAndFindMedian(args[1], medianIndex1, medianIndex2, conf);
			return (result ? 0 : 1);
		}

		public virtual double GetMedian()
		{
			return median;
		}
	}
}
