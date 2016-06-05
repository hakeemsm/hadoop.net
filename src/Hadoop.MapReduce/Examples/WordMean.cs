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
	public class WordMean : Configured, Tool
	{
		private double mean = 0;

		private static readonly Text Count = new Text("count");

		private static readonly Text Length = new Text("length");

		private static readonly LongWritable One = new LongWritable(1);

		/// <summary>
		/// Maps words from line of text into 2 key-value pairs; one key-value pair for
		/// counting the word, another for counting its length.
		/// </summary>
		public class WordMeanMapper : Mapper<object, Text, Text, LongWritable>
		{
			private LongWritable wordLen = new LongWritable();

			/// <summary>Emits 2 key-value pairs for counting the word and its length.</summary>
			/// <remarks>
			/// Emits 2 key-value pairs for counting the word and its length. Outputs are
			/// (Text, LongWritable).
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
					context.Write(Length, this.wordLen);
					context.Write(Count, One);
				}
			}
		}

		/// <summary>Performs integer summation of all the values for each key.</summary>
		public class WordMeanReducer : Reducer<Text, LongWritable, Text, LongWritable>
		{
			private LongWritable sum = new LongWritable();

			/// <summary>
			/// Sums all the individual values within the iterator and writes them to the
			/// same key.
			/// </summary>
			/// <param name="key">This will be one of 2 constants: LENGTH_STR or COUNT_STR.</param>
			/// <param name="values">
			/// This will be an iterator of all the values associated with that
			/// key.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<LongWritable> values, Reducer.Context
				 context)
			{
				int theSum = 0;
				foreach (LongWritable val in values)
				{
					theSum += val.Get();
				}
				sum.Set(theSum);
				context.Write(key, sum);
			}
		}

		/// <summary>
		/// Reads the output file and parses the summation of lengths, and the word
		/// count, to perform a quick calculation of the mean.
		/// </summary>
		/// <param name="path">
		/// The path to find the output file in. Set in main to the output
		/// directory.
		/// </param>
		/// <exception cref="System.IO.IOException">If it cannot access the output directory, we throw an exception.
		/// 	</exception>
		private double ReadAndCalcMean(Path path, Configuration conf)
		{
			FileSystem fs = FileSystem.Get(conf);
			Path file = new Path(path, "part-r-00000");
			if (!fs.Exists(file))
			{
				throw new IOException("Output not found!");
			}
			BufferedReader br = null;
			// average = total sum / number of elements;
			try
			{
				br = new BufferedReader(new InputStreamReader(fs.Open(file), Charsets.Utf8));
				long count = 0;
				long length = 0;
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
					}
				}
				double theMean = (((double)length) / ((double)count));
				System.Console.Out.WriteLine("The mean is: " + theMean);
				return theMean;
			}
			finally
			{
				if (br != null)
				{
					br.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			ToolRunner.Run(new Configuration(), new WordMean(), args);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: wordmean <in> <out>");
				return 0;
			}
			Configuration conf = GetConf();
			Job job = Job.GetInstance(conf, "word mean");
			job.SetJarByClass(typeof(WordMean));
			job.SetMapperClass(typeof(WordMean.WordMeanMapper));
			job.SetCombinerClass(typeof(WordMean.WordMeanReducer));
			job.SetReducerClass(typeof(WordMean.WordMeanReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(LongWritable));
			FileInputFormat.AddInputPath(job, new Path(args[0]));
			Path outputpath = new Path(args[1]);
			FileOutputFormat.SetOutputPath(job, outputpath);
			bool result = job.WaitForCompletion(true);
			mean = ReadAndCalcMean(outputpath, conf);
			return (result ? 0 : 1);
		}

		/// <summary>Only valuable after run() called.</summary>
		/// <returns>Returns the mean value.</returns>
		public virtual double GetMean()
		{
			return mean;
		}
	}
}
