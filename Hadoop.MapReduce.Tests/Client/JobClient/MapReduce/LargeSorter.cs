using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// A sample MR job that helps with testing large sorts in the MapReduce
	/// framework.
	/// </summary>
	/// <remarks>
	/// A sample MR job that helps with testing large sorts in the MapReduce
	/// framework. Mapper generates the specified number of bytes and pipes them
	/// to the reducers.
	/// <code>mapreduce.large-sorter.mbs-per-map</code> specifies the amount
	/// of data (in MBs) to generate per map. By default, this is twice the value
	/// of <code>mapreduce.task.io.sort.mb</code> or 1 GB if that is not specified
	/// either.
	/// <code>mapreduce.large-sorter.map-tasks</code> specifies the number of map
	/// tasks to run.
	/// <code>mapreduce.large-sorter.reduce-tasks</code> specifies the number of
	/// reduce tasks to run.
	/// </remarks>
	public class LargeSorter : Configured, Tool
	{
		private const string LsPrefix = "mapreduce.large-sorter.";

		public const string MbsPerMap = LsPrefix + "mbs-per-map";

		public const string NumMapTasks = LsPrefix + "map-tasks";

		public const string NumReduceTasks = LsPrefix + "reduce-tasks";

		private const string MaxValue = LsPrefix + "max-value";

		private const string MinValue = LsPrefix + "min-value";

		private const string MinKey = LsPrefix + "min-key";

		private const string MaxKey = LsPrefix + "max-key";

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
					result.AddItem(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, null));
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
				return new LargeSorter.RandomInputFormat.RandomRecordReader(((FileSplit)split).GetPath
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

			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				numBytesToWrite = 1024 * 1024 * conf.GetLong(MbsPerMap, 2 * conf.GetInt(MRJobConfig
					.IoSortMb, 512));
				minKeySize = conf.GetInt(MinKey, 10);
				keySizeRange = conf.GetInt(MaxKey, 1000) - minKeySize;
				minValueSize = conf.GetInt(MinValue, 0);
				valueSizeRange = conf.GetInt(MaxValue, 20000) - minValueSize;
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
					context.GetCounter(LargeSorter.Counters.BytesWritten).Increment(keyLength + valueLength
						);
					context.GetCounter(LargeSorter.Counters.RecordsWritten).Increment(1);
					if (++itemCount % 200 == 0)
					{
						context.SetStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left."
							);
					}
				}
				context.SetStatus("done with " + itemCount + " records.");
			}
		}

		internal class Discarder : Reducer<BytesWritable, BytesWritable, WritableComparable
			, Writable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(BytesWritable key, IEnumerable<BytesWritable> values
				, Reducer.Context context)
			{
			}
			// Do nothing
		}

		private void VerifyNotZero(Configuration conf, string config)
		{
			if (conf.GetInt(config, 1) <= 0)
			{
				throw new ArgumentException(config + "should be > 0");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Path outDir = new Path(typeof(LargeSorter).FullName + Runtime.CurrentTimeMillis()
				);
			Configuration conf = GetConf();
			VerifyNotZero(conf, MbsPerMap);
			VerifyNotZero(conf, NumMapTasks);
			conf.SetInt(MRJobConfig.NumMaps, conf.GetInt(NumMapTasks, 2));
			int ioSortMb = conf.GetInt(MRJobConfig.IoSortMb, 512);
			int mapMb = Math.Max(2 * ioSortMb, conf.GetInt(MRJobConfig.MapMemoryMb, MRJobConfig
				.DefaultMapMemoryMb));
			conf.SetInt(MRJobConfig.MapMemoryMb, mapMb);
			conf.Set(MRJobConfig.MapJavaOpts, "-Xmx" + (mapMb - 200) + "m");
			Job job = Job.GetInstance(conf);
			job.SetJarByClass(typeof(LargeSorter));
			job.SetJobName("large-sorter");
			FileOutputFormat.SetOutputPath(job, outDir);
			job.SetOutputKeyClass(typeof(BytesWritable));
			job.SetOutputValueClass(typeof(BytesWritable));
			job.SetInputFormatClass(typeof(LargeSorter.RandomInputFormat));
			job.SetMapperClass(typeof(LargeSorter.RandomMapper));
			job.SetReducerClass(typeof(LargeSorter.Discarder));
			job.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
			job.SetNumReduceTasks(conf.GetInt(NumReduceTasks, 1));
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			int ret = 1;
			try
			{
				ret = job.WaitForCompletion(true) ? 0 : 1;
			}
			finally
			{
				FileSystem.Get(conf).Delete(outDir, true);
			}
			DateTime endTime = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + endTime);
			System.Console.Out.WriteLine("The job took " + (endTime.GetTime() - startTime.GetTime
				()) / 1000 + " seconds.");
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new LargeSorter(), args);
			System.Environment.Exit(res);
		}
	}
}
