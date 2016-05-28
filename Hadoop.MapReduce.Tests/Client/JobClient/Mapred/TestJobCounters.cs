using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This is an wordcount application that tests the count of records
	/// got spilled to disk.
	/// </summary>
	/// <remarks>
	/// This is an wordcount application that tests the count of records
	/// got spilled to disk. It generates simple text input files. Then
	/// runs the wordcount map/reduce application on (1) 3 i/p files(with 3 maps
	/// and 1 reduce) and verifies the counters and (2) 4 i/p files(with 4 maps
	/// and 1 reduce) and verifies counters. Wordcount application reads the
	/// text input files, breaks each line into words and counts them. The output
	/// is a locally sorted list of words and the count of how often they occurred.
	/// </remarks>
	public class TestJobCounters
	{
		private void ValidateFileCounters(Counters counter, long fileBytesRead, long fileBytesWritten
			, long mapOutputBytes, long mapOutputMaterializedBytes)
		{
			NUnit.Framework.Assert.IsTrue(counter.FindCounter(FileInputFormatCounter.BytesRead
				).GetValue() != 0);
			NUnit.Framework.Assert.AreEqual(fileBytesRead, counter.FindCounter(FileInputFormatCounter
				.BytesRead).GetValue());
			NUnit.Framework.Assert.IsTrue(counter.FindCounter(FileOutputFormatCounter.BytesWritten
				).GetValue() != 0);
			if (mapOutputBytes >= 0)
			{
				NUnit.Framework.Assert.IsTrue(counter.FindCounter(TaskCounter.MapOutputBytes).GetValue
					() != 0);
			}
			if (mapOutputMaterializedBytes >= 0)
			{
				NUnit.Framework.Assert.IsTrue(counter.FindCounter(TaskCounter.MapOutputMaterializedBytes
					).GetValue() != 0);
			}
		}

		private void ValidateOldFileCounters(Counters counter, long fileBytesRead, long fileBytesWritten
			, long mapOutputBytes, long mapOutputMaterializedBytes)
		{
			NUnit.Framework.Assert.AreEqual(fileBytesRead, counter.FindCounter(FileInputFormat.Counter
				.BytesRead).GetValue());
			NUnit.Framework.Assert.AreEqual(fileBytesRead, counter.FindCounter(FileInputFormat.Counter
				.BytesRead).GetValue());
			NUnit.Framework.Assert.AreEqual(fileBytesWritten, counter.FindCounter(FileOutputFormat.Counter
				.BytesWritten).GetValue());
			NUnit.Framework.Assert.AreEqual(fileBytesWritten, counter.FindCounter(FileOutputFormat.Counter
				.BytesWritten).GetValue());
			if (mapOutputBytes >= 0)
			{
				NUnit.Framework.Assert.IsTrue(counter.FindCounter(TaskCounter.MapOutputBytes).GetValue
					() != 0);
			}
			if (mapOutputMaterializedBytes >= 0)
			{
				NUnit.Framework.Assert.IsTrue(counter.FindCounter(TaskCounter.MapOutputMaterializedBytes
					).GetValue() != 0);
			}
		}

		private void ValidateCounters(Counters counter, long spillRecCnt, long mapInputRecords
			, long mapOutputRecords)
		{
			// Check if the numer of Spilled Records is same as expected
			NUnit.Framework.Assert.AreEqual(spillRecCnt, counter.FindCounter(TaskCounter.SpilledRecords
				).GetCounter());
			NUnit.Framework.Assert.AreEqual(mapInputRecords, counter.FindCounter(TaskCounter.
				MapInputRecords).GetCounter());
			NUnit.Framework.Assert.AreEqual(mapOutputRecords, counter.FindCounter(TaskCounter
				.MapOutputRecords).GetCounter());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RemoveWordsFile(Path inpFile, Configuration conf)
		{
			FileSystem fs = inpFile.GetFileSystem(conf);
			if (fs.Exists(inpFile) && !fs.Delete(inpFile, false))
			{
				throw new IOException("Failed to delete " + inpFile);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateWordsFile(Path inpFile, Configuration conf)
		{
			FileSystem fs = inpFile.GetFileSystem(conf);
			if (fs.Exists(inpFile))
			{
				return;
			}
			FSDataOutputStream @out = fs.Create(inpFile);
			try
			{
				// 1024*4 unique words --- repeated 5 times => 5*2K words
				int Replicas = 5;
				int Numlines = 1024;
				int Numwordsperline = 4;
				string Word = "zymurgy";
				// 7 bytes + 4 id bytes
				Formatter fmt = new Formatter(new StringBuilder());
				for (int i = 0; i < Replicas; i++)
				{
					for (int j = 1; j <= Numlines * Numwordsperline; j += Numwordsperline)
					{
						((StringBuilder)fmt.Out()).Length = 0;
						for (int k = 0; k < Numwordsperline; ++k)
						{
							fmt.Format("%s%04d ", Word, j + k);
						}
						((StringBuilder)fmt.Out()).Append("\n");
						@out.WriteBytes(fmt.ToString());
					}
				}
			}
			finally
			{
				@out.Close();
			}
		}

		private static Path InDir = null;

		private static Path OutDir = null;

		private static Path testdir = null;

		private static Path[] inFiles = new Path[5];

		/// <exception cref="System.IO.IOException"/>
		private static long GetFileSize(Path path)
		{
			FileSystem fs = FileSystem.GetLocal(new Configuration());
			long len = 0;
			len += fs.GetFileStatus(path).GetLen();
			Path crcPath = new Path(path.GetParent(), "." + path.GetName() + ".crc");
			if (fs.Exists(crcPath))
			{
				len += fs.GetFileStatus(crcPath).GetLen();
			}
			return len;
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void InitPaths()
		{
			Configuration conf = new Configuration();
			Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"));
			testdir = new Path(TestRootDir, "spilledRecords.countertest");
			InDir = new Path(testdir, "in");
			OutDir = new Path(testdir, "out");
			FileSystem fs = FileSystem.GetLocal(conf);
			testdir = new Path(TestRootDir, "spilledRecords.countertest");
			if (fs.Exists(testdir) && !fs.Delete(testdir, true))
			{
				throw new IOException("Could not delete " + testdir);
			}
			if (!fs.Mkdirs(InDir))
			{
				throw new IOException("Mkdirs failed to create " + InDir);
			}
			for (int i = 0; i < inFiles.Length; i++)
			{
				inFiles[i] = new Path(InDir, "input5_2k_" + i);
			}
			// create 3 input files each with 5*2k words
			CreateWordsFile(inFiles[0], conf);
			CreateWordsFile(inFiles[1], conf);
			CreateWordsFile(inFiles[2], conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Cleanup()
		{
			//clean up the input and output files
			Configuration conf = new Configuration();
			FileSystem fs = testdir.GetFileSystem(conf);
			if (fs.Exists(testdir))
			{
				fs.Delete(testdir, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static JobConf CreateConfiguration()
		{
			JobConf baseConf = new JobConf(typeof(TestJobCounters));
			baseConf.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			baseConf.SetOutputValueClass(typeof(IntWritable));
			baseConf.SetMapperClass(typeof(WordCount.MapClass));
			baseConf.SetCombinerClass(typeof(WordCount.Reduce));
			baseConf.SetReducerClass(typeof(WordCount.Reduce));
			baseConf.SetNumReduceTasks(1);
			baseConf.SetInt(JobContext.IoSortMb, 1);
			baseConf.Set(JobContext.MapSortSpillPercent, "0.50");
			baseConf.SetInt(JobContext.MapCombineMinSpills, 3);
			return baseConf;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Job CreateJob()
		{
			Configuration conf = new Configuration();
			Job baseJob = Job.GetInstance(conf);
			baseJob.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			baseJob.SetOutputValueClass(typeof(IntWritable));
			baseJob.SetMapperClass(typeof(TestJobCounters.NewMapTokenizer));
			baseJob.SetCombinerClass(typeof(TestJobCounters.NewSummer));
			baseJob.SetReducerClass(typeof(TestJobCounters.NewSummer));
			baseJob.SetNumReduceTasks(1);
			baseJob.GetConfiguration().SetInt(JobContext.IoSortMb, 1);
			baseJob.GetConfiguration().Set(JobContext.MapSortSpillPercent, "0.50");
			baseJob.GetConfiguration().SetInt(JobContext.MapCombineMinSpills, 3);
			FileInputFormat.SetMinInputSplitSize(baseJob, long.MaxValue);
			return baseJob;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldCounterA()
		{
			JobConf conf = CreateConfiguration();
			conf.SetNumMapTasks(3);
			conf.SetInt(JobContext.IoSortFactor, 2);
			RemoveWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			FileInputFormat.SetInputPaths(conf, InDir);
			FileOutputFormat.SetOutputPath(conf, new Path(OutDir, "outputO0"));
			RunningJob myJob = JobClient.RunJob(conf);
			Counters c1 = myJob.GetCounters();
			// Each record requires 16 bytes of metadata, 16 bytes per serialized rec
			// (vint word len + word + IntWritable) = (1 + 11 + 4)
			// (2^20 buf * .5 spill pcnt) / 32 bytes/record = 2^14 recs per spill
			// Each file contains 5 replicas of 4096 words, so the first spill will
			// contain 4 (2^14 rec / 2^12 rec/replica) replicas, the second just one.
			// Each map spills twice, emitting 4096 records per spill from the
			// combiner per spill. The merge adds an additional 8192 records, as
			// there are too few spills to combine (2 < 3)
			// Each map spills 2^14 records, so maps spill 49152 records, combined.
			// The combiner has emitted 24576 records to the reducer; these are all
			// fetched straight to memory from the map side. The intermediate merge
			// adds 8192 records per segment read; again, there are too few spills to
			// combine, so all Total spilled records in the reduce
			// is 8192 records / map * 3 maps = 24576.
			// Total: map + reduce = 49152 + 24576 = 73728
			// 3 files, 5120 = 5 * 1024 rec/file = 15360 input records
			// 4 records/line = 61440 output records
			ValidateCounters(c1, 73728, 15360, 61440);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
			ValidateOldFileCounters(c1, inputSize, 61928, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldCounterB()
		{
			JobConf conf = CreateConfiguration();
			CreateWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			inputSize += GetFileSize(inFiles[3]);
			conf.SetNumMapTasks(4);
			conf.SetInt(JobContext.IoSortFactor, 2);
			FileInputFormat.SetInputPaths(conf, InDir);
			FileOutputFormat.SetOutputPath(conf, new Path(OutDir, "outputO1"));
			RunningJob myJob = JobClient.RunJob(conf);
			Counters c1 = myJob.GetCounters();
			// As above, each map spills 2^14 records, so 4 maps spill 2^16 records
			// In the reduce, there are two intermediate merges before the reduce.
			// 1st merge: read + write = 8192 * 4
			// 2nd merge: read + write = 8192 * 4
			// final merge: 0
			// Total reduce: 32768
			// Total: map + reduce = 2^16 + 2^15 = 98304
			// 4 files, 5120 = 5 * 1024 rec/file = 15360 input records
			// 4 records/line = 81920 output records
			ValidateCounters(c1, 98304, 20480, 81920);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldCounterC()
		{
			JobConf conf = CreateConfiguration();
			CreateWordsFile(inFiles[3], conf);
			CreateWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			inputSize += GetFileSize(inFiles[3]);
			inputSize += GetFileSize(inFiles[4]);
			conf.SetNumMapTasks(4);
			conf.SetInt(JobContext.IoSortFactor, 3);
			FileInputFormat.SetInputPaths(conf, InDir);
			FileOutputFormat.SetOutputPath(conf, new Path(OutDir, "outputO2"));
			RunningJob myJob = JobClient.RunJob(conf);
			Counters c1 = myJob.GetCounters();
			// As above, each map spills 2^14 records, so 5 maps spill 81920
			// 1st merge: read + write = 6 * 8192
			// final merge: unmerged = 2 * 8192
			// Total reduce: 45056
			// 5 files, 5120 = 5 * 1024 rec/file = 15360 input records
			// 4 records/line = 102400 output records
			ValidateCounters(c1, 122880, 25600, 102400);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldCounterD()
		{
			JobConf conf = CreateConfiguration();
			conf.SetNumMapTasks(3);
			conf.SetInt(JobContext.IoSortFactor, 2);
			conf.SetNumReduceTasks(0);
			RemoveWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			FileInputFormat.SetInputPaths(conf, InDir);
			FileOutputFormat.SetOutputPath(conf, new Path(OutDir, "outputO3"));
			RunningJob myJob = JobClient.RunJob(conf);
			Counters c1 = myJob.GetCounters();
			// No Reduces. Will go through the direct output collector. Spills=0
			ValidateCounters(c1, 0, 15360, 61440);
			ValidateFileCounters(c1, inputSize, 0, -1, -1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewCounterA()
		{
			Job job = CreateJob();
			Configuration conf = job.GetConfiguration();
			conf.SetInt(JobContext.IoSortFactor, 2);
			RemoveWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			FileInputFormat.SetInputPaths(job, InDir);
			FileOutputFormat.SetOutputPath(job, new Path(OutDir, "outputN0"));
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
			Counters c1 = Counters.Downgrade(job.GetCounters());
			ValidateCounters(c1, 73728, 15360, 61440);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewCounterB()
		{
			Job job = CreateJob();
			Configuration conf = job.GetConfiguration();
			conf.SetInt(JobContext.IoSortFactor, 2);
			CreateWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			inputSize += GetFileSize(inFiles[3]);
			FileInputFormat.SetInputPaths(job, InDir);
			FileOutputFormat.SetOutputPath(job, new Path(OutDir, "outputN1"));
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
			Counters c1 = Counters.Downgrade(job.GetCounters());
			ValidateCounters(c1, 98304, 20480, 81920);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewCounterC()
		{
			Job job = CreateJob();
			Configuration conf = job.GetConfiguration();
			conf.SetInt(JobContext.IoSortFactor, 3);
			CreateWordsFile(inFiles[3], conf);
			CreateWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			inputSize += GetFileSize(inFiles[3]);
			inputSize += GetFileSize(inFiles[4]);
			FileInputFormat.SetInputPaths(job, InDir);
			FileOutputFormat.SetOutputPath(job, new Path(OutDir, "outputN2"));
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
			Counters c1 = Counters.Downgrade(job.GetCounters());
			ValidateCounters(c1, 122880, 25600, 102400);
			ValidateFileCounters(c1, inputSize, 0, 0, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewCounterD()
		{
			Job job = CreateJob();
			Configuration conf = job.GetConfiguration();
			conf.SetInt(JobContext.IoSortFactor, 2);
			job.SetNumReduceTasks(0);
			RemoveWordsFile(inFiles[3], conf);
			RemoveWordsFile(inFiles[4], conf);
			long inputSize = 0;
			inputSize += GetFileSize(inFiles[0]);
			inputSize += GetFileSize(inFiles[1]);
			inputSize += GetFileSize(inFiles[2]);
			FileInputFormat.SetInputPaths(job, InDir);
			FileOutputFormat.SetOutputPath(job, new Path(OutDir, "outputN3"));
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
			Counters c1 = Counters.Downgrade(job.GetCounters());
			ValidateCounters(c1, 0, 15360, 61440);
			ValidateFileCounters(c1, inputSize, 0, -1, -1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldCounters()
		{
			Counters c1 = new Counters();
			c1.IncrCounter(FileInputFormat.Counter.BytesRead, 100);
			c1.IncrCounter(FileOutputFormat.Counter.BytesWritten, 200);
			c1.IncrCounter(TaskCounter.MapOutputBytes, 100);
			c1.IncrCounter(TaskCounter.MapOutputMaterializedBytes, 100);
			ValidateFileCounters(c1, 100, 200, 100, 100);
			ValidateOldFileCounters(c1, 100, 200, 100, 100);
		}

		/// <summary>Increases the JVM's heap usage to the specified target value.</summary>
		internal class MemoryLoader
		{
			private const int DefaultUnitLoadSize = 10 * 1024 * 1024;

			private long targetValue;

			private IList<string> loadObjects = new AList<string>();

			internal MemoryLoader(long targetValue)
			{
				// 10mb
				// the target value to reach
				// a list to hold the load objects
				this.targetValue = targetValue;
			}

			/// <summary>Loads the memory to the target value.</summary>
			internal virtual void Load()
			{
				while (Runtime.GetRuntime().TotalMemory() < targetValue)
				{
					System.Console.Out.WriteLine("Loading memory with " + DefaultUnitLoadSize + " characters. Current usage : "
						 + Runtime.GetRuntime().TotalMemory());
					// load some objects in the memory
					loadObjects.AddItem(RandomStringUtils.Random(DefaultUnitLoadSize));
					// sleep for 100ms
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
				}
			}
		}

		/// <summary>
		/// A mapper that increases the JVM's heap usage to a target value configured
		/// via
		/// <see cref="TargetValue"/>
		/// using a
		/// <see cref="MemoryLoader"/>
		/// .
		/// </summary>
		internal class MemoryLoaderMapper : MapReduceBase, Mapper<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			internal const string TargetValue = "map.memory-loader.target-value";

			private static TestJobCounters.MemoryLoader loader = null;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable val, OutputCollector<WritableComparable
				, Writable> output, Reporter reporter)
			{
				NUnit.Framework.Assert.IsNotNull("Mapper not configured!", loader);
				// load the memory
				loader.Load();
				// work as identity mapper
				output.Collect(key, val);
			}

			public override void Configure(JobConf conf)
			{
				loader = new TestJobCounters.MemoryLoader(conf.GetLong(TargetValue, -1));
			}
		}

		/// <summary>
		/// A reducer that increases the JVM's heap usage to a target value configured
		/// via
		/// <see cref="TargetValue"/>
		/// using a
		/// <see cref="MemoryLoader"/>
		/// .
		/// </summary>
		internal class MemoryLoaderReducer : MapReduceBase, Reducer<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			internal const string TargetValue = "reduce.memory-loader.target-value";

			private static TestJobCounters.MemoryLoader loader = null;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(WritableComparable key, IEnumerator<Writable> val, OutputCollector
				<WritableComparable, Writable> output, Reporter reporter)
			{
				NUnit.Framework.Assert.IsNotNull("Reducer not configured!", loader);
				// load the memory
				loader.Load();
				// work as identity reducer
				output.Collect(key, key);
			}

			public override void Configure(JobConf conf)
			{
				loader = new TestJobCounters.MemoryLoader(conf.GetLong(TargetValue, -1));
			}
		}

		/// <exception cref="System.Exception"/>
		private long GetTaskCounterUsage(JobClient client, JobID id, int numReports, int 
			taskId, TaskType type)
		{
			TaskReport[] reports = null;
			if (TaskType.Map.Equals(type))
			{
				reports = client.GetMapTaskReports(id);
			}
			else
			{
				if (TaskType.Reduce.Equals(type))
				{
					reports = client.GetReduceTaskReports(id);
				}
			}
			NUnit.Framework.Assert.IsNotNull("No reports found for task type '" + type.ToString
				() + "' in job " + id, reports);
			// make sure that the total number of reports match the expected
			NUnit.Framework.Assert.AreEqual("Mismatch in task id", numReports, reports.Length
				);
			Counters counters = reports[taskId].GetCounters();
			return counters.GetCounter(TaskCounter.CommittedHeapBytes);
		}

		// set up heap options, target value for memory loader and the output 
		// directory before running the job
		/// <exception cref="System.IO.IOException"/>
		private static RunningJob RunHeapUsageTestJob(JobConf conf, Path testRootDir, string
			 heapOptions, long targetMapValue, long targetReduceValue, FileSystem fs, JobClient
			 client, Path inDir)
		{
			// define a job
			JobConf jobConf = new JobConf(conf);
			// configure the jobs
			jobConf.SetNumMapTasks(1);
			jobConf.SetNumReduceTasks(1);
			jobConf.SetMapperClass(typeof(TestJobCounters.MemoryLoaderMapper));
			jobConf.SetReducerClass(typeof(TestJobCounters.MemoryLoaderReducer));
			jobConf.SetInputFormat(typeof(TextInputFormat));
			jobConf.SetOutputKeyClass(typeof(LongWritable));
			jobConf.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			jobConf.SetMaxMapAttempts(1);
			jobConf.SetMaxReduceAttempts(1);
			jobConf.Set(JobConf.MapredTaskJavaOpts, heapOptions);
			// set the targets
			jobConf.SetLong(TestJobCounters.MemoryLoaderMapper.TargetValue, targetMapValue);
			jobConf.SetLong(TestJobCounters.MemoryLoaderReducer.TargetValue, targetReduceValue
				);
			// set the input directory for the job
			FileInputFormat.SetInputPaths(jobConf, inDir);
			// define job output folder
			Path outDir = new Path(testRootDir, "out");
			fs.Delete(outDir, true);
			FileOutputFormat.SetOutputPath(jobConf, outDir);
			// run the job
			RunningJob job = client.SubmitJob(jobConf);
			job.WaitForCompletion();
			JobID jobID = job.GetID();
			NUnit.Framework.Assert.IsTrue("Job " + jobID + " failed!", job.IsSuccessful());
			return job;
		}

		/// <summary>
		/// Tests
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskCounter"/>
		/// 's
		/// <see cref="TaskCounter.COMMITTED_HEAP_BYTES"/>
		/// .
		/// The test consists of running a low-memory job which consumes less heap
		/// memory and then running a high-memory job which consumes more heap memory,
		/// and then ensuring that COMMITTED_HEAP_BYTES of low-memory job is smaller
		/// than that of the high-memory job.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeapUsageCounter()
		{
			JobConf conf = new JobConf();
			// create a local filesystem handle
			FileSystem fileSystem = FileSystem.GetLocal(conf);
			// define test root directories
			Path rootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"));
			Path testRootDir = new Path(rootDir, "testHeapUsageCounter");
			// cleanup the test root directory
			fileSystem.Delete(testRootDir, true);
			// set the current working directory
			fileSystem.SetWorkingDirectory(testRootDir);
			fileSystem.DeleteOnExit(testRootDir);
			// create a mini cluster using the local file system
			MiniMRCluster mrCluster = new MiniMRCluster(1, fileSystem.GetUri().ToString(), 1);
			try
			{
				conf = mrCluster.CreateJobConf();
				JobClient jobClient = new JobClient(conf);
				// define job input
				Path inDir = new Path(testRootDir, "in");
				// create input data
				CreateWordsFile(inDir, conf);
				// configure and run a low memory job which will run without loading the
				// jvm's heap
				RunningJob lowMemJob = RunHeapUsageTestJob(conf, testRootDir, "-Xms32m -Xmx1G", 0
					, 0, fileSystem, jobClient, inDir);
				JobID lowMemJobID = lowMemJob.GetID();
				long lowMemJobMapHeapUsage = GetTaskCounterUsage(jobClient, lowMemJobID, 1, 0, TaskType
					.Map);
				System.Console.Out.WriteLine("Job1 (low memory job) map task heap usage: " + lowMemJobMapHeapUsage
					);
				long lowMemJobReduceHeapUsage = GetTaskCounterUsage(jobClient, lowMemJobID, 1, 0, 
					TaskType.Reduce);
				System.Console.Out.WriteLine("Job1 (low memory job) reduce task heap usage: " + lowMemJobReduceHeapUsage
					);
				// configure and run a high memory job which will load the jvm's heap
				RunningJob highMemJob = RunHeapUsageTestJob(conf, testRootDir, "-Xms32m -Xmx1G", 
					lowMemJobMapHeapUsage + 256 * 1024 * 1024, lowMemJobReduceHeapUsage + 256 * 1024
					 * 1024, fileSystem, jobClient, inDir);
				JobID highMemJobID = highMemJob.GetID();
				long highMemJobMapHeapUsage = GetTaskCounterUsage(jobClient, highMemJobID, 1, 0, 
					TaskType.Map);
				System.Console.Out.WriteLine("Job2 (high memory job) map task heap usage: " + highMemJobMapHeapUsage
					);
				long highMemJobReduceHeapUsage = GetTaskCounterUsage(jobClient, highMemJobID, 1, 
					0, TaskType.Reduce);
				System.Console.Out.WriteLine("Job2 (high memory job) reduce task heap usage: " + 
					highMemJobReduceHeapUsage);
				NUnit.Framework.Assert.IsTrue("Incorrect map heap usage reported by the map task"
					, lowMemJobMapHeapUsage < highMemJobMapHeapUsage);
				NUnit.Framework.Assert.IsTrue("Incorrect reduce heap usage reported by the reduce task"
					, lowMemJobReduceHeapUsage < highMemJobReduceHeapUsage);
			}
			finally
			{
				// shutdown the mr cluster
				mrCluster.Shutdown();
				try
				{
					fileSystem.Delete(testRootDir, true);
				}
				catch (IOException)
				{
				}
			}
		}

		public class NewMapTokenizer : Mapper<object, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			, IntWritable>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Org.Apache.Hadoop.IO.Text word = new Org.Apache.Hadoop.IO.Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Org.Apache.Hadoop.IO.Text value, Mapper.Context
				 context)
			{
				StringTokenizer itr = new StringTokenizer(value.ToString());
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					context.Write(word, one);
				}
			}
		}

		public class NewSummer : Reducer<Org.Apache.Hadoop.IO.Text, IntWritable, Org.Apache.Hadoop.IO.Text
			, IntWritable>
		{
			private IntWritable result = new IntWritable();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerable<IntWritable
				> values, Reducer.Context context)
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
	}
}
