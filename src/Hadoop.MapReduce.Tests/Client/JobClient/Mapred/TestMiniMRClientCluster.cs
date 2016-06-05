using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Basic testing for the MiniMRClientCluster.</summary>
	/// <remarks>
	/// Basic testing for the MiniMRClientCluster. This test shows an example class
	/// that can be used in MR1 or MR2, without any change to the test. The test will
	/// use MiniMRYarnCluster in MR2, and MiniMRCluster in MR1.
	/// </remarks>
	public class TestMiniMRClientCluster
	{
		private static Path inDir = null;

		private static Path outDir = null;

		private static Path testdir = null;

		private static Path[] inFiles = new Path[5];

		private static MiniMRClientCluster mrCluster;

		private class InternalClass
		{
			internal InternalClass(TestMiniMRClientCluster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMiniMRClientCluster _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			Configuration conf = new Configuration();
			Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"));
			testdir = new Path(TestRootDir, "TestMiniMRClientCluster");
			inDir = new Path(testdir, "in");
			outDir = new Path(testdir, "out");
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(testdir) && !fs.Delete(testdir, true))
			{
				throw new IOException("Could not delete " + testdir);
			}
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir);
			}
			for (int i = 0; i < inFiles.Length; i++)
			{
				inFiles[i] = new Path(inDir, "part_" + i);
				CreateFile(inFiles[i], conf);
			}
			// create the mini cluster to be used for the tests
			mrCluster = MiniMRClientClusterFactory.Create(typeof(TestMiniMRClientCluster.InternalClass
				), 1, new Configuration());
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Cleanup()
		{
			// clean up the input and output files
			Configuration conf = new Configuration();
			FileSystem fs = testdir.GetFileSystem(conf);
			if (fs.Exists(testdir))
			{
				fs.Delete(testdir, true);
			}
			// stopping the mini cluster
			mrCluster.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestart()
		{
			string rmAddress1 = mrCluster.GetConfig().Get(YarnConfiguration.RmAddress);
			string rmAdminAddress1 = mrCluster.GetConfig().Get(YarnConfiguration.RmAdminAddress
				);
			string rmSchedAddress1 = mrCluster.GetConfig().Get(YarnConfiguration.RmSchedulerAddress
				);
			string rmRstrackerAddress1 = mrCluster.GetConfig().Get(YarnConfiguration.RmResourceTrackerAddress
				);
			string rmWebAppAddress1 = mrCluster.GetConfig().Get(YarnConfiguration.RmWebappAddress
				);
			string mrHistAddress1 = mrCluster.GetConfig().Get(JHAdminConfig.MrHistoryAddress);
			string mrHistWebAppAddress1 = mrCluster.GetConfig().Get(JHAdminConfig.MrHistoryWebappAddress
				);
			mrCluster.Restart();
			string rmAddress2 = mrCluster.GetConfig().Get(YarnConfiguration.RmAddress);
			string rmAdminAddress2 = mrCluster.GetConfig().Get(YarnConfiguration.RmAdminAddress
				);
			string rmSchedAddress2 = mrCluster.GetConfig().Get(YarnConfiguration.RmSchedulerAddress
				);
			string rmRstrackerAddress2 = mrCluster.GetConfig().Get(YarnConfiguration.RmResourceTrackerAddress
				);
			string rmWebAppAddress2 = mrCluster.GetConfig().Get(YarnConfiguration.RmWebappAddress
				);
			string mrHistAddress2 = mrCluster.GetConfig().Get(JHAdminConfig.MrHistoryAddress);
			string mrHistWebAppAddress2 = mrCluster.GetConfig().Get(JHAdminConfig.MrHistoryWebappAddress
				);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + rmAddress1 + " is different from new address: "
				 + rmAddress2, rmAddress1, rmAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + rmAdminAddress1 + " is different from new address: "
				 + rmAdminAddress2, rmAdminAddress1, rmAdminAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + rmSchedAddress1 + " is different from new address: "
				 + rmSchedAddress2, rmSchedAddress1, rmSchedAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + rmRstrackerAddress1 
				+ " is different from new address: " + rmRstrackerAddress2, rmRstrackerAddress1, 
				rmRstrackerAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + rmWebAppAddress1 + " is different from new address: "
				 + rmWebAppAddress2, rmWebAppAddress1, rmWebAppAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + mrHistAddress1 + " is different from new address: "
				 + mrHistAddress2, mrHistAddress1, mrHistAddress2);
			NUnit.Framework.Assert.AreEqual("Address before restart: " + mrHistWebAppAddress1
				 + " is different from new address: " + mrHistWebAppAddress2, mrHistWebAppAddress1
				, mrHistWebAppAddress2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJob()
		{
			Job job = CreateJob();
			FileInputFormat.SetInputPaths(job, inDir);
			FileOutputFormat.SetOutputPath(job, new Path(outDir, "testJob"));
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
			ValidateCounters(job.GetCounters(), 5, 25, 5, 5);
		}

		private void ValidateCounters(Counters counters, long mapInputRecords, long mapOutputRecords
			, long reduceInputGroups, long reduceOutputRecords)
		{
			NUnit.Framework.Assert.AreEqual("MapInputRecords", mapInputRecords, counters.FindCounter
				("MyCounterGroup", "MAP_INPUT_RECORDS").GetValue());
			NUnit.Framework.Assert.AreEqual("MapOutputRecords", mapOutputRecords, counters.FindCounter
				("MyCounterGroup", "MAP_OUTPUT_RECORDS").GetValue());
			NUnit.Framework.Assert.AreEqual("ReduceInputGroups", reduceInputGroups, counters.
				FindCounter("MyCounterGroup", "REDUCE_INPUT_GROUPS").GetValue());
			NUnit.Framework.Assert.AreEqual("ReduceOutputRecords", reduceOutputRecords, counters
				.FindCounter("MyCounterGroup", "REDUCE_OUTPUT_RECORDS").GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateFile(Path inFile, Configuration conf)
		{
			FileSystem fs = inFile.GetFileSystem(conf);
			if (fs.Exists(inFile))
			{
				return;
			}
			FSDataOutputStream @out = fs.Create(inFile);
			@out.WriteBytes("This is a test file");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static Job CreateJob()
		{
			Job baseJob = Job.GetInstance(mrCluster.GetConfig());
			baseJob.SetOutputKeyClass(typeof(Text));
			baseJob.SetOutputValueClass(typeof(IntWritable));
			baseJob.SetMapperClass(typeof(TestMiniMRClientCluster.MyMapper));
			baseJob.SetReducerClass(typeof(TestMiniMRClientCluster.MyReducer));
			baseJob.SetNumReduceTasks(1);
			return baseJob;
		}

		public class MyMapper : Mapper<object, Text, Text, IntWritable>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Text value, Mapper.Context context)
			{
				context.GetCounter("MyCounterGroup", "MAP_INPUT_RECORDS").Increment(1);
				StringTokenizer iter = new StringTokenizer(value.ToString());
				while (iter.HasMoreTokens())
				{
					word.Set(iter.NextToken());
					context.Write(word, one);
					context.GetCounter("MyCounterGroup", "MAP_OUTPUT_RECORDS").Increment(1);
				}
			}
		}

		public class MyReducer : Reducer<Text, IntWritable, Text, IntWritable>
		{
			private IntWritable result = new IntWritable();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<IntWritable> values, Reducer.Context
				 context)
			{
				context.GetCounter("MyCounterGroup", "REDUCE_INPUT_GROUPS").Increment(1);
				int sum = 0;
				foreach (IntWritable val in values)
				{
					sum += val.Get();
				}
				result.Set(sum);
				context.Write(key, result);
				context.GetCounter("MyCounterGroup", "REDUCE_OUTPUT_RECORDS").Increment(1);
			}
		}
	}
}
