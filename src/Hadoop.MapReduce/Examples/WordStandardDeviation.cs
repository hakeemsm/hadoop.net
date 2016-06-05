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
	public class WordStandardDeviation : Configured, Tool
	{
		private double stddev = 0;

		private static readonly Text Length = new Text("length");

		private static readonly Text Square = new Text("square");

		private static readonly Text Count = new Text("count");

		private static readonly LongWritable One = new LongWritable(1);

		/// <summary>
		/// Maps words from line of text into 3 key-value pairs; one key-value pair for
		/// counting the word, one for counting its length, and one for counting the
		/// square of its length.
		/// </summary>
		public class WordStandardDeviationMapper : Mapper<object, Text, Text, LongWritable
			>
		{
			private LongWritable wordLen = new LongWritable();

			private LongWritable wordLenSq = new LongWritable();

			/// <summary>
			/// Emits 3 key-value pairs for counting the word, its length, and the
			/// squares of its length.
			/// </summary>
			/// <remarks>
			/// Emits 3 key-value pairs for counting the word, its length, and the
			/// squares of its length. Outputs are (Text, LongWritable).
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
					this.wordLen.Set(@string.Length);
					// the square of an integer is an integer...
					this.wordLenSq.Set((long)Math.Pow(@string.Length, 2.0));
					context.Write(Length, this.wordLen);
					context.Write(Square, this.wordLenSq);
					context.Write(Count, One);
				}
			}
		}

		/// <summary>Performs integer summation of all the values for each key.</summary>
		public class WordStandardDeviationReducer : Reducer<Text, LongWritable, Text, LongWritable
			>
		{
			private LongWritable val = new LongWritable();

			/// <summary>
			/// Sums all the individual values within the iterator and writes them to the
			/// same key.
			/// </summary>
			/// <param name="key">
			/// This will be one of 2 constants: LENGTH_STR, COUNT_STR, or
			/// SQUARE_STR.
			/// </param>
			/// <param name="values">
			/// This will be an iterator of all the values associated with that
			/// key.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<LongWritable> values, Reducer.Context
				 context)
			{
				int sum = 0;
				foreach (LongWritable value in values)
				{
					sum += value.Get();
				}
				val.Set(sum);
				context.Write(key, val);
			}
		}

		/// <summary>
		/// Reads the output file and parses the summation of lengths, the word count,
		/// and the lengths squared, to perform a quick calculation of the standard
		/// deviation.
		/// </summary>
		/// <param name="path">
		/// The path to find the output file in. Set in main to the output
		/// directory.
		/// </param>
		/// <exception cref="System.IO.IOException">If it cannot access the output directory, we throw an exception.
		/// 	</exception>
		private double ReadAndCalcStdDev(Path path, Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			Path file = new Path(path, "part-r-00000");
			if (!fs.Exists(file))
			{
				throw new IOException("Output not found!");
			}
			double stddev = 0;
			BufferedReader br = null;
			try
			{
				br = new BufferedReader(new InputStreamReader(fs.Open(file), Charsets.Utf8));
				long count = 0;
				long length = 0;
				long square = 0;
				string line;
				while ((line = br.ReadLine()) != null)
				{
					StringTokenizer st = new StringTokenizer(line);
					// grab type
					string type = st.NextToken();
					// differentiate
					if (type.Equals(Count.ToString()))
					{
						string countLit = st.NextToken();
						count = long.Parse(countLit);
					}
					else
					{
						if (type.Equals(Length.ToString()))
						{
							string lengthLit = st.NextToken();
							length = long.Parse(lengthLit);
						}
						else
						{
							if (type.Equals(Square.ToString()))
							{
								string squareLit = st.NextToken();
								square = long.Parse(squareLit);
							}
						}
					}
				}
				// average = total sum / number of elements;
				double mean = (((double)length) / ((double)count));
				// standard deviation = sqrt((sum(lengths ^ 2)/count) - (mean ^ 2))
				mean = Math.Pow(mean, 2.0);
				double term = (((double)square / ((double)count)));
				stddev = Math.Sqrt((term - mean));
				System.Console.Out.WriteLine("The standard deviation is: " + stddev);
			}
			finally
			{
				if (br != null)
				{
					br.Close();
				}
			}
			return stddev;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			ToolRunner.Run(new Configuration(), new WordStandardDeviation(), args);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: wordstddev <in> <out>");
				return 0;
			}
			Configuration conf = GetConf();
			Job job = Job.GetInstance(conf, "word stddev");
			job.SetJarByClass(typeof(WordStandardDeviation));
			job.SetMapperClass(typeof(WordStandardDeviation.WordStandardDeviationMapper));
			job.SetCombinerClass(typeof(WordStandardDeviation.WordStandardDeviationReducer));
			job.SetReducerClass(typeof(WordStandardDeviation.WordStandardDeviationReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			FileInputFormat.AddInputPath(job, new Path(args[0]));
			Path outputpath = new Path(args[1]);
			FileOutputFormat.SetOutputPath(job, outputpath);
			bool result = job.WaitForCompletion(true);
			// read output and calculate standard deviation
			stddev = ReadAndCalcStdDev(outputpath, conf);
			return (result ? 0 : 1);
		}

		public virtual double GetStandardDeviation()
		{
			return stddev;
		}
	}
}
