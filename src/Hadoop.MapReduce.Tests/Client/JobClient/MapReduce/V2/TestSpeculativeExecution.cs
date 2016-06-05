using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Speculate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestSpeculativeExecution
	{
		public class TestSpecEstimator : LegacyTaskRuntimeEstimator
		{
			private const long SpeculateThis = 999999L;

			public TestSpecEstimator()
				: base()
			{
			}

			/*
			* This class is used to control when speculative execution happens.
			*/
			/*
			* This will only be called if speculative execution is turned on.
			*
			* If either mapper or reducer speculation is turned on, this will be
			* called.
			*
			* This will cause speculation to engage for the first mapper or first
			* reducer (that is, attempt ID "*_m_000000_0" or "*_r_000000_0")
			*
			* If this attempt is killed, the retry will have attempt id 1, so it
			* will not engage speculation again.
			*/
			public override long EstimatedRuntime(TaskAttemptId id)
			{
				if ((id.GetTaskId().GetId() == 0) && (id.GetId() == 0))
				{
					return SpeculateThis;
				}
				return base.EstimatedRuntime(id);
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestSpeculativeExecution
			));

		protected internal static MiniMRYarnCluster mrCluster;

		private static Configuration initialConf = new Configuration();

		private static FileSystem localFs;

		static TestSpeculativeExecution()
		{
			try
			{
				localFs = FileSystem.GetLocal(initialConf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static Path TestRootDir = new Path("target", typeof(TestSpeculativeExecution
			).FullName + "-tmpDir").MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory
			());

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		private static Path TestOutDir = new Path(TestRootDir, "test.out.dir");

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(typeof(TestSpeculativeExecution).FullName, 4);
				Configuration conf = new Configuration();
				mrCluster.Init(conf);
				mrCluster.Start();
			}
			// workaround the absent public distcache.
			localFs.CopyFromLocalFile(new Path(MiniMRYarnCluster.Appjar), AppJar);
			localFs.SetPermission(AppJar, new FsPermission("700"));
		}

		[AfterClass]
		public static void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
				mrCluster = null;
			}
		}

		public class SpeculativeMapper : Mapper<object, Text, Text, IntWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Text value, Mapper.Context context)
			{
				// Make one mapper slower for speculative execution
				TaskAttemptID taid = context.GetTaskAttemptID();
				long sleepTime = 100;
				Configuration conf = context.GetConfiguration();
				bool test_speculate_map = conf.GetBoolean(MRJobConfig.MapSpeculative, false);
				// IF TESTING MAPPER SPECULATIVE EXECUTION:
				//   Make the "*_m_000000_0" attempt take much longer than the others.
				//   When speculative execution is enabled, this should cause the attempt
				//   to be killed and restarted. At that point, the attempt ID will be
				//   "*_m_000000_1", so sleepTime will still remain 100ms.
				if ((taid.GetTaskType() == TaskType.Map) && test_speculate_map && (taid.GetTaskID
					().GetId() == 0) && (taid.GetId() == 0))
				{
					sleepTime = 10000;
				}
				try
				{
					Sharpen.Thread.Sleep(sleepTime);
				}
				catch (Exception)
				{
				}
				// Ignore
				context.Write(value, new IntWritable(1));
			}
		}

		public class SpeculativeReducer : Reducer<Text, IntWritable, Text, IntWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<IntWritable> values, Reducer.Context
				 context)
			{
				// Make one reducer slower for speculative execution
				TaskAttemptID taid = context.GetTaskAttemptID();
				long sleepTime = 100;
				Configuration conf = context.GetConfiguration();
				bool test_speculate_reduce = conf.GetBoolean(MRJobConfig.ReduceSpeculative, false
					);
				// IF TESTING REDUCE SPECULATIVE EXECUTION:
				//   Make the "*_r_000000_0" attempt take much longer than the others.
				//   When speculative execution is enabled, this should cause the attempt
				//   to be killed and restarted. At that point, the attempt ID will be
				//   "*_r_000000_1", so sleepTime will still remain 100ms.
				if ((taid.GetTaskType() == TaskType.Reduce) && test_speculate_reduce && (taid.GetTaskID
					().GetId() == 0) && (taid.GetId() == 0))
				{
					sleepTime = 10000;
				}
				try
				{
					Sharpen.Thread.Sleep(sleepTime);
				}
				catch (Exception)
				{
				}
				// Ignore
				context.Write(key, new IntWritable(0));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpeculativeExecution()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			/*------------------------------------------------------------------
			* Test that Map/Red does not speculate if MAP_SPECULATIVE and
			* REDUCE_SPECULATIVE are both false.
			* -----------------------------------------------------------------
			*/
			Job job = RunSpecTest(false, false);
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedReduces
				).GetValue());
			NUnit.Framework.Assert.AreEqual(0, counters.FindCounter(JobCounter.NumFailedMaps)
				.GetValue());
			/*----------------------------------------------------------------------
			* Test that Mapper speculates if MAP_SPECULATIVE is true and
			* REDUCE_SPECULATIVE is false.
			* ---------------------------------------------------------------------
			*/
			job = RunSpecTest(true, false);
			succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
			counters = job.GetCounters();
			// The long-running map will be killed and a new one started.
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedReduces
				).GetValue());
			NUnit.Framework.Assert.AreEqual(0, counters.FindCounter(JobCounter.NumFailedMaps)
				.GetValue());
			NUnit.Framework.Assert.AreEqual(1, counters.FindCounter(JobCounter.NumKilledMaps)
				.GetValue());
			/*----------------------------------------------------------------------
			* Test that Reducer speculates if REDUCE_SPECULATIVE is true and
			* MAP_SPECULATIVE is false.
			* ---------------------------------------------------------------------
			*/
			job = RunSpecTest(false, true);
			succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
			counters = job.GetCounters();
			// The long-running map will be killed and a new one started.
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.TotalLaunchedReduces
				).GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateTempFile(string filename, string contents)
		{
			Path path = new Path(TestRootDir, filename);
			FSDataOutputStream os = localFs.Create(path);
			os.WriteBytes(contents);
			os.Close();
			localFs.SetPermission(path, new FsPermission("700"));
			return path;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		private Job RunSpecTest(bool mapspec, bool redspec)
		{
			Path first = CreateTempFile("specexec_map_input1", "a\nz");
			Path secnd = CreateTempFile("specexec_map_input2", "a\nz");
			Configuration conf = mrCluster.GetConfig();
			conf.SetBoolean(MRJobConfig.MapSpeculative, mapspec);
			conf.SetBoolean(MRJobConfig.ReduceSpeculative, redspec);
			conf.SetClass(MRJobConfig.MrAmTaskEstimator, typeof(TestSpeculativeExecution.TestSpecEstimator
				), typeof(TaskRuntimeEstimator));
			Job job = Job.GetInstance(conf);
			job.SetJarByClass(typeof(TestSpeculativeExecution));
			job.SetMapperClass(typeof(TestSpeculativeExecution.SpeculativeMapper));
			job.SetReducerClass(typeof(TestSpeculativeExecution.SpeculativeReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			job.SetNumReduceTasks(2);
			FileInputFormat.SetInputPaths(job, first);
			FileInputFormat.AddInputPath(job, secnd);
			FileOutputFormat.SetOutputPath(job, TestOutDir);
			// Delete output directory if it exists.
			try
			{
				localFs.Delete(TestOutDir, true);
			}
			catch (IOException)
			{
			}
			// ignore
			// Creates the Job Configuration
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.SetMaxMapAttempts(2);
			job.Submit();
			return job;
		}
	}
}
