using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Stress tests for the LocalJobRunner</summary>
	public class TestLocalRunner : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLocalRunner));

		private static int[] InputSizes = new int[] { 50000, 500, 500, 20, 5000, 500 };

		private static int[] OutputSizes = new int[] { 1, 500, 500, 500, 500, 500 };

		private static int[] SleepIntervals = new int[] { 10000, 15, 15, 20, 250, 60 };

		private class StressMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			private int threadId;

			public long exposedState;

			// Different map tasks operate at different speeds.
			// We define behavior for 6 threads.
			// Used to ensure that the compiler doesn't optimize away
			// some code.
			protected override void Setup(Mapper.Context context)
			{
				// Get the thread num from the file number.
				FileSplit split = (FileSplit)context.GetInputSplit();
				Path filePath = split.GetPath();
				string name = filePath.GetName();
				this.threadId = Sharpen.Extensions.ValueOf(name);
				Log.Info("Thread " + threadId + " : " + context.GetInputSplit());
			}

			/// <summary>Map method with different behavior based on the thread id</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text val, Mapper.Context c)
			{
				// Write many values quickly.
				for (int i = 0; i < OutputSizes[threadId]; i++)
				{
					c.Write(new LongWritable(0), val);
					if (i % SleepIntervals[threadId] == 1)
					{
						Sharpen.Thread.Sleep(1);
					}
				}
			}

			protected override void Cleanup(Mapper.Context context)
			{
				// Output this here, to ensure that the incrementing done in map()
				// cannot be optimized away.
				Log.Debug("Busy loop counter: " + this.exposedState);
			}
		}

		private class CountingReducer : Reducer<LongWritable, Text, LongWritable, LongWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> vals, Reducer.Context
				 context)
			{
				long @out = 0;
				foreach (Text val in vals)
				{
					@out++;
				}
				context.Write(key, new LongWritable(@out));
			}
		}

		private class GCMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text val, Mapper.Context c)
			{
				// Create a whole bunch of objects.
				IList<int> lst = new AList<int>();
				for (int i = 0; i < 20000; i++)
				{
					lst.AddItem(i);
				}
				// Actually use this list, to ensure that it isn't just optimized away.
				int sum = 0;
				foreach (int x in lst)
				{
					sum += x;
				}
				// throw away the list and run a GC.
				lst = null;
				System.GC.Collect();
				c.Write(new LongWritable(sum), val);
			}
		}

		/// <summary>Create a single input file in the input directory.</summary>
		/// <param name="dirPath">the directory in which the file resides</param>
		/// <param name="id">the file id number</param>
		/// <param name="numRecords">how many records to write to each file.</param>
		/// <exception cref="System.IO.IOException"/>
		private void CreateInputFile(Path dirPath, int id, int numRecords)
		{
			string Message = "This is a line in a file: ";
			Path filePath = new Path(dirPath, string.Empty + id);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			OutputStream os = fs.Create(filePath);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
			for (int i = 0; i < numRecords; i++)
			{
				w.Write(Message + id + " " + i + "\n");
			}
			w.Close();
		}

		private static int TotalRecords = 0;

		static TestLocalRunner()
		{
			// This is the total number of map output records we expect to generate,
			// based on input file sizes (see createMultiMapsInput()) and the behavior
			// of the different StressMapper threads.
			for (int i = 0; i < 6; i++)
			{
				TotalRecords += InputSizes[i] * OutputSizes[i];
			}
		}

		private readonly string InputDir = "multiMapInput";

		private readonly string OutputDir = "multiMapOutput";

		private Path GetInputPath()
		{
			string dataDir = Runtime.GetProperty("test.build.data");
			if (null == dataDir)
			{
				return new Path(InputDir);
			}
			else
			{
				return new Path(new Path(dataDir), InputDir);
			}
		}

		private Path GetOutputPath()
		{
			string dataDir = Runtime.GetProperty("test.build.data");
			if (null == dataDir)
			{
				return new Path(OutputDir);
			}
			else
			{
				return new Path(new Path(dataDir), OutputDir);
			}
		}

		/// <summary>Create the inputs for the MultiMaps test.</summary>
		/// <returns>the path to the input directory.</returns>
		/// <exception cref="System.IO.IOException"/>
		private Path CreateMultiMapsInput()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path inputPath = GetInputPath();
			// Clear the input directory if it exists, first.
			if (fs.Exists(inputPath))
			{
				fs.Delete(inputPath, true);
			}
			// Create input files, with sizes calibrated based on
			// the amount of work done in each mapper.
			for (int i = 0; i < 6; i++)
			{
				CreateInputFile(inputPath, i, InputSizes[i]);
			}
			return inputPath;
		}

		/// <summary>Verify that we got the correct amount of output.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyOutput(Path outputPath)
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path outputFile = new Path(outputPath, "part-r-00000");
			InputStream @is = fs.Open(outputFile);
			BufferedReader r = new BufferedReader(new InputStreamReader(@is));
			// Should get a single line of the form "0\t(count)"
			string line = r.ReadLine().Trim();
			NUnit.Framework.Assert.IsTrue("Line does not have correct key", line.StartsWith("0\t"
				));
			int count = Sharpen.Extensions.ValueOf(Sharpen.Runtime.Substring(line, 2));
			NUnit.Framework.Assert.AreEqual("Incorrect count generated!", TotalRecords, count
				);
			r.Close();
		}

		/// <summary>
		/// Test that the GC counter actually increments when we know that we've
		/// spent some time in the GC during the mapper.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGcCounter()
		{
			Path inputPath = GetInputPath();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			// Clear input/output dirs.
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			if (fs.Exists(inputPath))
			{
				fs.Delete(inputPath, true);
			}
			// Create one input file
			CreateInputFile(inputPath, 0, 20);
			// Now configure and run the job.
			Job job = Job.GetInstance();
			job.SetMapperClass(typeof(TestLocalRunner.GCMapper));
			job.SetNumReduceTasks(0);
			job.GetConfiguration().Set(MRJobConfig.IoSortMb, "25");
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			bool ret = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("job failed", ret);
			// This job should have done *some* gc work.
			// It had to clean up 400,000 objects.
			// We strongly suspect this will result in a few milliseconds effort.
			Counter gcCounter = job.GetCounters().FindCounter(TaskCounter.GcTimeMillis);
			NUnit.Framework.Assert.IsNotNull(gcCounter);
			NUnit.Framework.Assert.IsTrue("No time spent in gc", gcCounter.GetValue() > 0);
		}

		/// <summary>
		/// Run a test with several mappers in parallel, operating at different
		/// speeds.
		/// </summary>
		/// <remarks>
		/// Run a test with several mappers in parallel, operating at different
		/// speeds. Verify that the correct amount of output is created.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestMultiMaps()
		{
			Job job = Job.GetInstance();
			Path inputPath = CreateMultiMapsInput();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetMapperClass(typeof(TestLocalRunner.StressMapper));
			job.SetReducerClass(typeof(TestLocalRunner.CountingReducer));
			job.SetNumReduceTasks(1);
			LocalJobRunner.SetLocalMaxRunningMaps(job, 6);
			job.GetConfiguration().Set(MRJobConfig.IoSortMb, "25");
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			Sharpen.Thread toInterrupt = Sharpen.Thread.CurrentThread();
			Sharpen.Thread interrupter = new _Thread_311(toInterrupt);
			// 2m
			Log.Info("Submitting job...");
			job.Submit();
			Log.Info("Starting thread to interrupt main thread in 2 minutes");
			interrupter.Start();
			Log.Info("Waiting for job to complete...");
			try
			{
				job.WaitForCompletion(true);
			}
			catch (Exception ie)
			{
				Log.Fatal("Interrupted while waiting for job completion", ie);
				for (int i = 0; i < 10; i++)
				{
					Log.Fatal("Dumping stacks");
					ReflectionUtils.LogThreadInfo(Log, "multimap threads", 0);
					Sharpen.Thread.Sleep(1000);
				}
				throw;
			}
			Log.Info("Job completed, stopping interrupter");
			interrupter.Interrupt();
			try
			{
				interrupter.Join();
			}
			catch (Exception)
			{
			}
			// it might interrupt us right as we interrupt it
			Log.Info("Verifying output");
			VerifyOutput(outputPath);
		}

		private sealed class _Thread_311 : Sharpen.Thread
		{
			public _Thread_311(Sharpen.Thread toInterrupt)
			{
				this.toInterrupt = toInterrupt;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(120 * 1000);
					toInterrupt.Interrupt();
				}
				catch (Exception)
				{
				}
			}

			private readonly Sharpen.Thread toInterrupt;
		}

		/// <summary>Run a test with a misconfigured number of mappers.</summary>
		/// <remarks>
		/// Run a test with a misconfigured number of mappers.
		/// Expect failure.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidMultiMapParallelism()
		{
			Job job = Job.GetInstance();
			Path inputPath = CreateMultiMapsInput();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetMapperClass(typeof(TestLocalRunner.StressMapper));
			job.SetReducerClass(typeof(TestLocalRunner.CountingReducer));
			job.SetNumReduceTasks(1);
			LocalJobRunner.SetLocalMaxRunningMaps(job, -6);
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			bool success = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsFalse("Job succeeded somehow", success);
		}

		/// <summary>An IF that creates no splits</summary>
		private class EmptyInputFormat : InputFormat<object, object>
		{
			public override IList<InputSplit> GetSplits(JobContext context)
			{
				return new AList<InputSplit>();
			}

			public override RecordReader<object, object> CreateRecordReader(InputSplit split, 
				TaskAttemptContext context)
			{
				return new TestLocalRunner.EmptyRecordReader();
			}
		}

		private class EmptyRecordReader : RecordReader<object, object>
		{
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
			}

			public override object GetCurrentKey()
			{
				return new object();
			}

			public override object GetCurrentValue()
			{
				return new object();
			}

			public override float GetProgress()
			{
				return 0.0f;
			}

			public override void Close()
			{
			}

			public override bool NextKeyValue()
			{
				return false;
			}
		}

		/// <summary>Test case for zero mappers</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyMaps()
		{
			Job job = Job.GetInstance();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetInputFormatClass(typeof(TestLocalRunner.EmptyInputFormat));
			job.SetNumReduceTasks(1);
			FileOutputFormat.SetOutputPath(job, outputPath);
			bool success = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Empty job should work", success);
		}

		/// <returns>the directory where numberfiles are written (mapper inputs)</returns>
		private Path GetNumberDirPath()
		{
			return new Path(GetInputPath(), "numberfiles");
		}

		/// <summary>Write out an input file containing an integer.</summary>
		/// <param name="fileNum">the file number to write to.</param>
		/// <param name="value">the value to write to the file</param>
		/// <returns>the path of the written file.</returns>
		/// <exception cref="System.IO.IOException"/>
		private Path MakeNumberFile(int fileNum, int value)
		{
			Path workDir = GetNumberDirPath();
			Path filePath = new Path(workDir, "file" + fileNum);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			OutputStream os = fs.Create(filePath);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
			w.Write(string.Empty + value);
			w.Close();
			return filePath;
		}

		/// <summary>Each record received by this mapper is a number 'n'.</summary>
		/// <remarks>
		/// Each record received by this mapper is a number 'n'.
		/// Emit the values [0..n-1]
		/// </remarks>
		public class SequenceMapper : Mapper<LongWritable, Text, Text, NullWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable k, Text v, Mapper.Context c)
			{
				int max = Sharpen.Extensions.ValueOf(v.ToString());
				for (int i = 0; i < max; i++)
				{
					c.Write(new Text(string.Empty + i), NullWritable.Get());
				}
			}
		}

		private const int NumberFileVal = 100;

		/// <summary>
		/// Tally up the values and ensure that we got as much data
		/// out as we put in.
		/// </summary>
		/// <remarks>
		/// Tally up the values and ensure that we got as much data
		/// out as we put in.
		/// Each mapper generated 'NUMBER_FILE_VAL' values (0..NUMBER_FILE_VAL-1).
		/// Verify that across all our reducers we got exactly this much
		/// data back.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private void VerifyNumberJob(int numMaps)
		{
			Path outputDir = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			FileStatus[] stats = fs.ListStatus(outputDir);
			int valueSum = 0;
			foreach (FileStatus f in stats)
			{
				FSDataInputStream istream = fs.Open(f.GetPath());
				BufferedReader r = new BufferedReader(new InputStreamReader(istream));
				string line = null;
				while ((line = r.ReadLine()) != null)
				{
					valueSum += Sharpen.Extensions.ValueOf(line.Trim());
				}
				r.Close();
			}
			int maxVal = NumberFileVal - 1;
			int expectedPerMapper = maxVal * (maxVal + 1) / 2;
			int expectedSum = expectedPerMapper * numMaps;
			Log.Info("expected sum: " + expectedSum + ", got " + valueSum);
			NUnit.Framework.Assert.AreEqual("Didn't get all our results back", expectedSum, valueSum
				);
		}

		/// <summary>
		/// Run a test which creates a SequenceMapper / IdentityReducer
		/// job over a set of generated number files.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void DoMultiReducerTest(int numMaps, int numReduces, int parallelMaps, int
			 parallelReduces)
		{
			Path @in = GetNumberDirPath();
			Path @out = GetOutputPath();
			// Clear data from any previous tests.
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(@out))
			{
				fs.Delete(@out, true);
			}
			if (fs.Exists(@in))
			{
				fs.Delete(@in, true);
			}
			for (int i = 0; i < numMaps; i++)
			{
				MakeNumberFile(i, 100);
			}
			Job job = Job.GetInstance();
			job.SetNumReduceTasks(numReduces);
			job.SetMapperClass(typeof(TestLocalRunner.SequenceMapper));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(NullWritable));
			FileInputFormat.AddInputPath(job, @in);
			FileOutputFormat.SetOutputPath(job, @out);
			LocalJobRunner.SetLocalMaxRunningMaps(job, parallelMaps);
			LocalJobRunner.SetLocalMaxRunningReduces(job, parallelReduces);
			bool result = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed!!", result);
			VerifyNumberJob(numMaps);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneMapMultiReduce()
		{
			DoMultiReducerTest(1, 2, 1, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneMapMultiParallelReduce()
		{
			DoMultiReducerTest(1, 2, 1, 2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiMapOneReduce()
		{
			DoMultiReducerTest(4, 1, 2, 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiMapMultiReduce()
		{
			DoMultiReducerTest(4, 4, 2, 2);
		}
	}
}
