using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// This program uses map/reduce to just run a distributed job where there is
	/// no interaction between the tasks and each task write a large unsorted
	/// random binary sequence file of BytesWritable.
	/// </summary>
	/// <remarks>
	/// This program uses map/reduce to just run a distributed job where there is
	/// no interaction between the tasks and each task write a large unsorted
	/// random binary sequence file of BytesWritable.
	/// In order for this program to generate data for terasort with 10-byte keys
	/// and 90-byte values, have the following config:
	/// <pre>
	/// <c>
	/// &lt;?xml version="1.0"?&gt;
	/// &lt;?xml-stylesheet type="text/xsl" href="configuration.xsl"?&gt;
	/// &lt;configuration&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomwriter.minkey&lt;/name&gt;
	/// &lt;value&gt;10&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomwriter.maxkey&lt;/name&gt;
	/// &lt;value&gt;10&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomwriter.minvalue&lt;/name&gt;
	/// &lt;value&gt;90&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomwriter.maxvalue&lt;/name&gt;
	/// &lt;value&gt;90&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;property&gt;
	/// &lt;name&gt;mapreduce.randomwriter.totalbytes&lt;/name&gt;
	/// &lt;value&gt;1099511627776&lt;/value&gt;
	/// &lt;/property&gt;
	/// &lt;/configuration&gt;
	/// </c>
	/// </pre>
	/// Equivalently,
	/// <see cref="RandomWriter"/>
	/// also supports all the above options
	/// and ones supported by
	/// <see cref="Org.Apache.Hadoop.Util.GenericOptionsParser"/>
	/// via the command-line.
	/// </remarks>
	public class RandomWriter : Configured, Tool
	{
		public const string TotalBytes = "mapreduce.randomwriter.totalbytes";

		public const string BytesPerMap = "mapreduce.randomwriter.bytespermap";

		public const string MapsPerHost = "mapreduce.randomwriter.mapsperhost";

		public const string MaxValue = "mapreduce.randomwriter.maxvalue";

		public const string MinValue = "mapreduce.randomwriter.minvalue";

		public const string MinKey = "mapreduce.randomwriter.minkey";

		public const string MaxKey = "mapreduce.randomwriter.maxkey";

		/// <summary>User counters</summary>
		internal enum Counters
		{
			RecordsWritten,
			BytesWritten
		}

		/// <summary>
		/// A custom input format that creates virtual inputs of a single string
		/// for each map.
		/// </summary>
		internal class RandomInputFormat : InputFormat<Text, Text>
		{
			/// <summary>
			/// Generate the requested number of file splits, with the filename
			/// set to the filename of the output file.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override IList<InputSplit> GetSplits(JobContext job)
			{
				IList<InputSplit> result = new AList<InputSplit>();
				Path outDir = FileOutputFormat.GetOutputPath(job);
				int numSplits = job.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
				for (int i = 0; i < numSplits; ++i)
				{
					result.AddItem(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, (string[]
						)null));
				}
				return result;
			}

			/// <summary>
			/// Return a single record (filename, "") where the filename is taken from
			/// the file split.
			/// </summary>
			internal class RandomRecordReader : RecordReader<Text, Text>
			{
				internal Path name;

				internal Text key = null;

				internal Text value = new Text();

				public RandomRecordReader(Path p)
				{
					name = p;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
				}

				public override bool NextKeyValue()
				{
					if (name != null)
					{
						key = new Text();
						key.Set(name.GetName());
						name = null;
						return true;
					}
					return false;
				}

				public override Text GetCurrentKey()
				{
					return key;
				}

				public override Text GetCurrentValue()
				{
					return value;
				}

				public override void Close()
				{
				}

				public override float GetProgress()
				{
					return 0.0f;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
				 context)
			{
				return new RandomWriter.RandomInputFormat.RandomRecordReader(((FileSplit)split).GetPath
					());
			}
		}

		internal class RandomMapper : Mapper<WritableComparable, Writable, BytesWritable, 
			BytesWritable>
		{
			private long numBytesToWrite;

			private int minKeySize;

			private int keySizeRange;

			private int minValueSize;

			private int valueSizeRange;

			private Random random = new Random();

			private BytesWritable randomKey = new BytesWritable();

			private BytesWritable randomValue = new BytesWritable();

			private void RandomizeBytes(byte[] data, int offset, int length)
			{
				for (int i = offset + length - 1; i >= offset; --i)
				{
					data[i] = unchecked((byte)random.Next(256));
				}
			}

			/// <summary>Given an output filename, write a bunch of random records to it.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(WritableComparable key, Writable value, Mapper.Context
				 context)
			{
				int itemCount = 0;
				while (numBytesToWrite > 0)
				{
					int keyLength = minKeySize + (keySizeRange != 0 ? random.Next(keySizeRange) : 0);
					randomKey.SetSize(keyLength);
					RandomizeBytes(randomKey.GetBytes(), 0, randomKey.GetLength());
					int valueLength = minValueSize + (valueSizeRange != 0 ? random.Next(valueSizeRange
						) : 0);
					randomValue.SetSize(valueLength);
					RandomizeBytes(randomValue.GetBytes(), 0, randomValue.GetLength());
					context.Write(randomKey, randomValue);
					numBytesToWrite -= keyLength + valueLength;
					context.GetCounter(RandomWriter.Counters.BytesWritten).Increment(keyLength + valueLength
						);
					context.GetCounter(RandomWriter.Counters.RecordsWritten).Increment(1);
					if (++itemCount % 200 == 0)
					{
						context.SetStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left."
							);
					}
				}
				context.SetStatus("done with " + itemCount + " records.");
			}

			/// <summary>
			/// Save the values out of the configuaration that we need to write
			/// the data.
			/// </summary>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				numBytesToWrite = conf.GetLong(BytesPerMap, 1 * 1024 * 1024 * 1024);
				minKeySize = conf.GetInt(MinKey, 10);
				keySizeRange = conf.GetInt(MaxKey, 1000) - minKeySize;
				minValueSize = conf.GetInt(MinValue, 0);
				valueSizeRange = conf.GetInt(MaxValue, 20000) - minValueSize;
			}
		}

		/// <summary>This is the main routine for launching a distributed random write job.</summary>
		/// <remarks>
		/// This is the main routine for launching a distributed random write job.
		/// It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
		/// The reduce doesn't do anything.
		/// </remarks>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("Usage: writer <out-dir>");
				ToolRunner.PrintGenericCommandUsage(System.Console.Out);
				return 2;
			}
			Path outDir = new Path(args[0]);
			Configuration conf = GetConf();
			JobClient client = new JobClient(conf);
			ClusterStatus cluster = client.GetClusterStatus();
			int numMapsPerHost = conf.GetInt(MapsPerHost, 10);
			long numBytesToWritePerMap = conf.GetLong(BytesPerMap, 1 * 1024 * 1024 * 1024);
			if (numBytesToWritePerMap == 0)
			{
				System.Console.Error.WriteLine("Cannot have" + BytesPerMap + " set to 0");
				return -2;
			}
			long totalBytesToWrite = conf.GetLong(TotalBytes, numMapsPerHost * numBytesToWritePerMap
				 * cluster.GetTaskTrackers());
			int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
			if (numMaps == 0 && totalBytesToWrite > 0)
			{
				numMaps = 1;
				conf.SetLong(BytesPerMap, totalBytesToWrite);
			}
			conf.SetInt(MRJobConfig.NumMaps, numMaps);
			Job job = Job.GetInstance(conf);
			job.SetJarByClass(typeof(RandomWriter));
			job.SetJobName("random-writer");
			FileOutputFormat.SetOutputPath(job, outDir);
			job.SetOutputKeyClass(typeof(BytesWritable));
			job.SetOutputValueClass(typeof(BytesWritable));
			job.SetInputFormatClass(typeof(RandomWriter.RandomInputFormat));
			job.SetMapperClass(typeof(RandomWriter.RandomMapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
			System.Console.Out.WriteLine("Running " + numMaps + " maps.");
			// reducer NONE
			job.SetNumReduceTasks(0);
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			DateTime endTime = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + endTime);
			System.Console.Out.WriteLine("The job took " + (endTime.GetTime() - startTime.GetTime
				()) / 1000 + " seconds.");
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new RandomWriter(), args);
			System.Environment.Exit(res);
		}
	}
}
