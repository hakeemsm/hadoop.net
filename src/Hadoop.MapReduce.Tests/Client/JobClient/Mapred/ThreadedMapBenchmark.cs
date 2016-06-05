using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Distributed threaded map benchmark.</summary>
	/// <remarks>
	/// Distributed threaded map benchmark.
	/// <p>
	/// This benchmark generates random data per map and tests the performance
	/// of having multiple spills (using multiple threads) over having just one
	/// spill. Following are the parameters that can be specified
	/// <li>File size per map.
	/// <li>Number of spills per map.
	/// <li>Number of maps per host.
	/// <p>
	/// Sort is used for benchmarking the performance.
	/// </remarks>
	public class ThreadedMapBenchmark : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ThreadedMapBenchmark));

		private static Path BaseDir = new Path(Runtime.GetProperty("test.build.data", FilePath
			.separator + "benchmarks" + FilePath.separator + "ThreadedMapBenchmark"));

		private static Path InputDir = new Path(BaseDir, "input");

		private static Path OutputDir = new Path(BaseDir, "output");

		private const float Factor = 2.3f;

		internal enum Counters
		{
			RecordsWritten,
			BytesWritten
		}

		/// <summary>
		/// Generates random input data of given size with keys and values of given
		/// sizes.
		/// </summary>
		/// <remarks>
		/// Generates random input data of given size with keys and values of given
		/// sizes. By default it generates 128mb input data with 10 byte keys and 10
		/// byte values.
		/// </remarks>
		public class Map : MapReduceBase, Mapper<WritableComparable, Writable, BytesWritable
			, BytesWritable>
		{
			private long numBytesToWrite;

			private int minKeySize;

			private int keySizeRange;

			private int minValueSize;

			private int valueSizeRange;

			private Random random = new Random();

			private BytesWritable randomKey = new BytesWritable();

			private BytesWritable randomValue = new BytesWritable();

			// mapreduce.task.io.sort.mb set to 
			// (FACTOR * data_size) should 
			// result in only 1 spill
			private void RandomizeBytes(byte[] data, int offset, int length)
			{
				for (int i = offset + length - 1; i >= offset; --i)
				{
					data[i] = unchecked((byte)random.Next(256));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable value, OutputCollector<BytesWritable
				, BytesWritable> output, Reporter reporter)
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
					output.Collect(randomKey, randomValue);
					numBytesToWrite -= keyLength + valueLength;
					reporter.IncrCounter(ThreadedMapBenchmark.Counters.BytesWritten, 1);
					reporter.IncrCounter(ThreadedMapBenchmark.Counters.RecordsWritten, 1);
					if (++itemCount % 200 == 0)
					{
						reporter.SetStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left."
							);
					}
				}
				reporter.SetStatus("done with " + itemCount + " records.");
			}

			public override void Configure(JobConf job)
			{
				numBytesToWrite = job.GetLong("test.tmb.bytes_per_map", 128 * 1024 * 1024);
				minKeySize = job.GetInt("test.tmb.min_key", 10);
				keySizeRange = job.GetInt("test.tmb.max_key", 10) - minKeySize;
				minValueSize = job.GetInt("test.tmb.min_value", 10);
				valueSizeRange = job.GetInt("test.tmb.max_value", 10) - minValueSize;
			}
		}

		/// <summary>Generate input data for the benchmark</summary>
		/// <exception cref="System.Exception"/>
		public static void GenerateInputData(int dataSizePerMap, int numSpillsPerMap, int
			 numMapsPerHost, JobConf masterConf)
		{
			JobConf job = new JobConf(masterConf, typeof(ThreadedMapBenchmark));
			job.SetJobName("threaded-map-benchmark-random-writer");
			job.SetJarByClass(typeof(ThreadedMapBenchmark));
			job.SetInputFormat(typeof(UtilsForTests.RandomInputFormat));
			job.SetOutputFormat(typeof(SequenceFileOutputFormat));
			job.SetMapperClass(typeof(ThreadedMapBenchmark.Map));
			job.SetReducerClass(typeof(IdentityReducer));
			job.SetOutputKeyClass(typeof(BytesWritable));
			job.SetOutputValueClass(typeof(BytesWritable));
			JobClient client = new JobClient(job);
			ClusterStatus cluster = client.GetClusterStatus();
			long totalDataSize = dataSizePerMap * numMapsPerHost * cluster.GetTaskTrackers();
			job.Set("test.tmb.bytes_per_map", (dataSizePerMap * 1024 * 1024).ToString());
			job.SetNumReduceTasks(0);
			// none reduce
			job.SetNumMapTasks(numMapsPerHost * cluster.GetTaskTrackers());
			FileOutputFormat.SetOutputPath(job, InputDir);
			FileSystem fs = FileSystem.Get(job);
			fs.Delete(BaseDir, true);
			Log.Info("Generating random input for the benchmark");
			Log.Info("Total data : " + totalDataSize + " mb");
			Log.Info("Data per map: " + dataSizePerMap + " mb");
			Log.Info("Number of spills : " + numSpillsPerMap);
			Log.Info("Number of maps per host : " + numMapsPerHost);
			Log.Info("Number of hosts : " + cluster.GetTaskTrackers());
			JobClient.RunJob(job);
		}

		// generates the input for the benchmark
		/// <summary>This is the main routine for launching the benchmark.</summary>
		/// <remarks>
		/// This is the main routine for launching the benchmark. It generates random
		/// input data. The input is non-splittable. Sort is used for benchmarking.
		/// This benchmark reports the effect of having multiple sort and spill
		/// cycles over a single sort and spill.
		/// </remarks>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Log.Info("Starting the benchmark for threaded spills");
			string version = "ThreadedMapBenchmark.0.0.1";
			System.Console.Out.WriteLine(version);
			string usage = "Usage: threadedmapbenchmark " + "[-dataSizePerMap <data size (in mb) per map, default is 128 mb>] "
				 + "[-numSpillsPerMap <number of spills per map, default is 2>] " + "[-numMapsPerHost <number of maps per host, default is 1>]";
			int dataSizePerMap = 128;
			// in mb
			int numSpillsPerMap = 2;
			int numMapsPerHost = 1;
			JobConf masterConf = new JobConf(GetConf());
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-dataSizePerMap"))
				{
					dataSizePerMap = System.Convert.ToInt32(args[++i]);
				}
				else
				{
					if (args[i].Equals("-numSpillsPerMap"))
					{
						numSpillsPerMap = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-numMapsPerHost"))
						{
							numMapsPerHost = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							System.Console.Error.WriteLine(usage);
							System.Environment.Exit(-1);
						}
					}
				}
			}
			if (dataSizePerMap < 1 || numSpillsPerMap < 1 || numMapsPerHost < 1)
			{
				// verify arguments
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			FileSystem fs = null;
			try
			{
				// using random-writer to generate the input data
				GenerateInputData(dataSizePerMap, numSpillsPerMap, numMapsPerHost, masterConf);
				// configure job for sorting
				JobConf job = new JobConf(masterConf, typeof(ThreadedMapBenchmark));
				job.SetJobName("threaded-map-benchmark-unspilled");
				job.SetJarByClass(typeof(ThreadedMapBenchmark));
				job.SetInputFormat(typeof(SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat
					));
				job.SetOutputFormat(typeof(SequenceFileOutputFormat));
				job.SetOutputKeyClass(typeof(BytesWritable));
				job.SetOutputValueClass(typeof(BytesWritable));
				job.SetMapperClass(typeof(IdentityMapper));
				job.SetReducerClass(typeof(IdentityReducer));
				FileInputFormat.AddInputPath(job, InputDir);
				FileOutputFormat.SetOutputPath(job, OutputDir);
				JobClient client = new JobClient(job);
				ClusterStatus cluster = client.GetClusterStatus();
				job.SetNumMapTasks(numMapsPerHost * cluster.GetTaskTrackers());
				job.SetNumReduceTasks(1);
				// set mapreduce.task.io.sort.mb to avoid spill
				int ioSortMb = (int)Math.Ceil(Factor * dataSizePerMap);
				job.Set(JobContext.IoSortMb, ioSortMb.ToString());
				fs = FileSystem.Get(job);
				Log.Info("Running sort with 1 spill per map");
				long startTime = Runtime.CurrentTimeMillis();
				JobClient.RunJob(job);
				long endTime = Runtime.CurrentTimeMillis();
				Log.Info("Total time taken : " + (endTime - startTime).ToString() + " millisec");
				fs.Delete(OutputDir, true);
				// set mapreduce.task.io.sort.mb to have multiple spills
				JobConf spilledJob = new JobConf(job, typeof(ThreadedMapBenchmark));
				ioSortMb = (int)Math.Ceil(Factor * Math.Ceil((double)dataSizePerMap / numSpillsPerMap
					));
				spilledJob.Set(JobContext.IoSortMb, ioSortMb.ToString());
				spilledJob.SetJobName("threaded-map-benchmark-spilled");
				spilledJob.SetJarByClass(typeof(ThreadedMapBenchmark));
				Log.Info("Running sort with " + numSpillsPerMap + " spills per map");
				startTime = Runtime.CurrentTimeMillis();
				JobClient.RunJob(spilledJob);
				endTime = Runtime.CurrentTimeMillis();
				Log.Info("Total time taken : " + (endTime - startTime).ToString() + " millisec");
			}
			finally
			{
				if (fs != null)
				{
					fs.Delete(BaseDir, true);
				}
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new ThreadedMapBenchmark(), args);
			System.Environment.Exit(res);
		}
	}
}
