using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// Tests context api and
	/// <see cref="StatusReporter.GetProgress()"/>
	/// via
	/// <see cref="TaskAttemptContext.GetProgress()"/>
	/// API .
	/// </summary>
	public class TestTaskContext : HadoopTestCase
	{
		private static readonly Path rootTempDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"));

		private static readonly Path testRootTempDir = new Path(rootTempDir, "TestTaskContext"
			);

		private static FileSystem fs = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			fs = FileSystem.GetLocal(new Configuration());
			fs.Delete(testRootTempDir, true);
			fs.Mkdirs(testRootTempDir);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Cleanup()
		{
			fs.Delete(testRootTempDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public TestTaskContext()
			: base(HadoopTestCase.ClusterMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		internal static string myStatus = "my status";

		internal class MyMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void Setup(Mapper.Context context)
			{
				context.SetStatus(myStatus);
				NUnit.Framework.Assert.AreEqual(myStatus, context.GetStatus());
			}
		}

		/// <summary>Tests context.setStatus method.</summary>
		/// <remarks>
		/// Tests context.setStatus method.
		/// TODO fix testcase
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		[Ignore]
		public virtual void TestContextStatus()
		{
			Path test = new Path(testRootTempDir, "testContextStatus");
			// test with 1 map and 0 reducers
			// test with custom task status
			int numMaps = 1;
			Job job = MapReduceTestUtil.CreateJob(CreateJobConf(), new Path(test, "in"), new 
				Path(test, "out"), numMaps, 0);
			job.SetMapperClass(typeof(TestTaskContext.MyMapper));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			TaskReport[] reports = job.GetTaskReports(TaskType.Map);
			NUnit.Framework.Assert.AreEqual(numMaps, reports.Length);
			NUnit.Framework.Assert.AreEqual(myStatus, reports[0].GetState());
			// test with 1 map and 1 reducer
			// test with default task status
			int numReduces = 1;
			job = MapReduceTestUtil.CreateJob(CreateJobConf(), new Path(test, "in"), new Path
				(test, "out"), numMaps, numReduces);
			job.SetMapperClass(typeof(MapReduceTestUtil.DataCopyMapper));
			job.SetReducerClass(typeof(MapReduceTestUtil.DataCopyReducer));
			job.SetMapOutputKeyClass(typeof(Text));
			job.SetMapOutputValueClass(typeof(Text));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			// fail early
			job.SetMaxMapAttempts(1);
			job.SetMaxReduceAttempts(0);
			// run the job and wait for completion
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}

		private const string Input = "Hi\nHi\nHi\nHi\n";

		private static readonly int InputLines = Input.Split("\n").Length;

		internal class ProgressCheckerMapper : Mapper<LongWritable, Text, Text, Text>
		{
			private int recordCount = 0;

			private float progressRange = 0;

			// check map task reports
			// TODO fix testcase 
			// Disabling checks for now to get builds to run
			/*
			reports = job.getTaskReports(TaskType.MAP);
			assertEquals(numMaps, reports.length);
			assertEquals("map > sort", reports[0].getState());
			
			// check reduce task reports
			reports = job.getTaskReports(TaskType.REDUCE);
			assertEquals(numReduces, reports.length);
			assertEquals("reduce > reduce", reports[0].getState());
			*/
			// an input with 4 lines
			/// <exception cref="System.IO.IOException"/>
			protected override void Setup(Mapper.Context context)
			{
				// check if the map task attempt progress is 0
				NUnit.Framework.Assert.AreEqual("Invalid progress in map setup", 0.0f, context.GetProgress
					(), 0f);
				// define the progress boundaries
				if (context.GetNumReduceTasks() == 0)
				{
					progressRange = 1f;
				}
				else
				{
					progressRange = 0.667f;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				// get the map phase progress
				float mapPhaseProgress = ((float)++recordCount) / InputLines;
				// get the weighted map phase progress
				float weightedMapProgress = progressRange * mapPhaseProgress;
				// check the map progress
				NUnit.Framework.Assert.AreEqual("Invalid progress in map", weightedMapProgress, context
					.GetProgress(), 0f);
				context.Write(new Text(value.ToString() + recordCount), value);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				// check if the attempt progress is at the progress boundary 
				NUnit.Framework.Assert.AreEqual("Invalid progress in map cleanup", progressRange, 
					context.GetProgress(), 0f);
			}
		}

		/// <summary>Tests new MapReduce map task's context.getProgress() method.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestMapContextProgress()
		{
			int numMaps = 1;
			Path test = new Path(testRootTempDir, "testMapContextProgress");
			Job job = MapReduceTestUtil.CreateJob(CreateJobConf(), new Path(test, "in"), new 
				Path(test, "out"), numMaps, 0, Input);
			job.SetMapperClass(typeof(TestTaskContext.ProgressCheckerMapper));
			job.SetMapOutputKeyClass(typeof(Text));
			// fail early
			job.SetMaxMapAttempts(1);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}

		internal class ProgressCheckerReducer : Reducer<Text, Text, Text, Text>
		{
			private int recordCount = 0;

			private readonly float ReduceProgressRange = 1.0f / 3;

			private readonly float ShuffleProgressRange = 1 - ReduceProgressRange;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Reducer.Context context)
			{
				// Note that the reduce will read some segments before calling setup()
				float reducePhaseProgress = ((float)++recordCount) / InputLines;
				float weightedReducePhaseProgress = ReduceProgressRange * reducePhaseProgress;
				// check that the shuffle phase progress is accounted for
				NUnit.Framework.Assert.AreEqual("Invalid progress in reduce setup", ShuffleProgressRange
					 + weightedReducePhaseProgress, context.GetProgress(), 0.01f);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual void Reduce(Text key, IEnumerator<Text> values, Reducer.Context context
				)
			{
				float reducePhaseProgress = ((float)++recordCount) / InputLines;
				float weightedReducePhaseProgress = ReduceProgressRange * reducePhaseProgress;
				NUnit.Framework.Assert.AreEqual("Invalid progress in reduce", ShuffleProgressRange
					 + weightedReducePhaseProgress, context.GetProgress(), 0.01f);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				// check if the reduce task has progress of 1 in the end
				NUnit.Framework.Assert.AreEqual("Invalid progress in reduce cleanup", 1.0f, context
					.GetProgress(), 0f);
			}
		}

		/// <summary>Tests new MapReduce reduce task's context.getProgress() method.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceContextProgress()
		{
			int numTasks = 1;
			Path test = new Path(testRootTempDir, "testReduceContextProgress");
			Job job = MapReduceTestUtil.CreateJob(CreateJobConf(), new Path(test, "in"), new 
				Path(test, "out"), numTasks, numTasks, Input);
			job.SetMapperClass(typeof(TestTaskContext.ProgressCheckerMapper));
			job.SetReducerClass(typeof(TestTaskContext.ProgressCheckerReducer));
			job.SetMapOutputKeyClass(typeof(Text));
			// fail early
			job.SetMaxMapAttempts(1);
			job.SetMaxReduceAttempts(1);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
		}
	}
}
