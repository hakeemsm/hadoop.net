using NUnit.Framework;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class TestJobConf
	{
		[NUnit.Framework.Test]
		public virtual void TestProfileParamsDefaults()
		{
			JobConf configuration = new JobConf();
			string result = configuration.GetProfileParams();
			NUnit.Framework.Assert.IsNotNull(result);
			NUnit.Framework.Assert.IsTrue(result.Contains("file=%s"));
			NUnit.Framework.Assert.IsTrue(result.StartsWith("-agentlib:hprof"));
		}

		[NUnit.Framework.Test]
		public virtual void TestProfileParamsSetter()
		{
			JobConf configuration = new JobConf();
			configuration.SetProfileParams("test");
			NUnit.Framework.Assert.AreEqual("test", configuration.Get(MRJobConfig.TaskProfileParams
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestProfileParamsGetter()
		{
			JobConf configuration = new JobConf();
			configuration.Set(MRJobConfig.TaskProfileParams, "test");
			NUnit.Framework.Assert.AreEqual("test", configuration.GetProfileParams());
		}

		/// <summary>Testing mapred.task.maxvmem replacement with new values</summary>
		[NUnit.Framework.Test]
		public virtual void TestMemoryConfigForMapOrReduceTask()
		{
			JobConf configuration = new JobConf();
			configuration.Set(MRJobConfig.MapMemoryMb, 300.ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, 300.ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 300);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 300);
			configuration.Set("mapred.task.maxvmem", (2 * 1024 * 1024).ToString());
			configuration.Set(MRJobConfig.MapMemoryMb, 300.ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, 300.ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 2);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 2);
			configuration = new JobConf();
			configuration.Set("mapred.task.maxvmem", "-1");
			configuration.Set(MRJobConfig.MapMemoryMb, 300.ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, 400.ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 300);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 400);
			configuration = new JobConf();
			configuration.Set("mapred.task.maxvmem", (2 * 1024 * 1024).ToString());
			configuration.Set(MRJobConfig.MapMemoryMb, "-1");
			configuration.Set(MRJobConfig.ReduceMemoryMb, "-1");
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 2);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 2);
			configuration = new JobConf();
			configuration.Set("mapred.task.maxvmem", (-1).ToString());
			configuration.Set(MRJobConfig.MapMemoryMb, "-1");
			configuration.Set(MRJobConfig.ReduceMemoryMb, "-1");
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), -1);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), -1);
			configuration = new JobConf();
			configuration.Set("mapred.task.maxvmem", (2 * 1024 * 1024).ToString());
			configuration.Set(MRJobConfig.MapMemoryMb, "3");
			configuration.Set(MRJobConfig.ReduceMemoryMb, "3");
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 2);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 2);
		}

		/// <summary>
		/// Test that negative values for MAPRED_TASK_MAXVMEM_PROPERTY cause
		/// new configuration keys' values to be used.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestNegativeValueForTaskVmem()
		{
			JobConf configuration = new JobConf();
			configuration.Set(JobConf.MapredTaskMaxvmemProperty, "-3");
			NUnit.Framework.Assert.AreEqual(MRJobConfig.DefaultMapMemoryMb, configuration.GetMemoryForMapTask
				());
			NUnit.Framework.Assert.AreEqual(MRJobConfig.DefaultReduceMemoryMb, configuration.
				GetMemoryForReduceTask());
			configuration.Set(MRJobConfig.MapMemoryMb, "4");
			configuration.Set(MRJobConfig.ReduceMemoryMb, "5");
			NUnit.Framework.Assert.AreEqual(4, configuration.GetMemoryForMapTask());
			NUnit.Framework.Assert.AreEqual(5, configuration.GetMemoryForReduceTask());
		}

		/// <summary>Test that negative values for new configuration keys get passed through.
		/// 	</summary>
		[NUnit.Framework.Test]
		public virtual void TestNegativeValuesForMemoryParams()
		{
			JobConf configuration = new JobConf();
			configuration.Set(MRJobConfig.MapMemoryMb, "-5");
			configuration.Set(MRJobConfig.ReduceMemoryMb, "-6");
			NUnit.Framework.Assert.AreEqual(-5, configuration.GetMemoryForMapTask());
			NUnit.Framework.Assert.AreEqual(-6, configuration.GetMemoryForReduceTask());
		}

		/// <summary>Test deprecated accessor and mutator method for mapred.task.maxvmem</summary>
		[NUnit.Framework.Test]
		public virtual void TestMaxVirtualMemoryForTask()
		{
			JobConf configuration = new JobConf();
			//get test case
			configuration.Set(MRJobConfig.MapMemoryMb, 300.ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, (-1).ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMaxVirtualMemoryForTask(), 300 *
				 1024 * 1024);
			configuration = new JobConf();
			configuration.Set(MRJobConfig.MapMemoryMb, (-1).ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, 200.ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMaxVirtualMemoryForTask(), 200 *
				 1024 * 1024);
			configuration = new JobConf();
			configuration.Set(MRJobConfig.MapMemoryMb, (-1).ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, (-1).ToString());
			configuration.Set("mapred.task.maxvmem", (1 * 1024 * 1024).ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMaxVirtualMemoryForTask(), 1 * 1024
				 * 1024);
			configuration = new JobConf();
			configuration.Set("mapred.task.maxvmem", (1 * 1024 * 1024).ToString());
			NUnit.Framework.Assert.AreEqual(configuration.GetMaxVirtualMemoryForTask(), 1 * 1024
				 * 1024);
			//set test case
			configuration = new JobConf();
			configuration.SetMaxVirtualMemoryForTask(2 * 1024 * 1024);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 2);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 2);
			configuration = new JobConf();
			configuration.Set(MRJobConfig.MapMemoryMb, 300.ToString());
			configuration.Set(MRJobConfig.ReduceMemoryMb, 400.ToString());
			configuration.SetMaxVirtualMemoryForTask(2 * 1024 * 1024);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForMapTask(), 2);
			NUnit.Framework.Assert.AreEqual(configuration.GetMemoryForReduceTask(), 2);
		}

		/// <summary>
		/// Ensure that by default JobContext.MAX_TASK_FAILURES_PER_TRACKER is less
		/// JobContext.MAP_MAX_ATTEMPTS and JobContext.REDUCE_MAX_ATTEMPTS so that
		/// failed tasks will be retried on other nodes
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestMaxTaskFailuresPerTracker()
		{
			JobConf jobConf = new JobConf(true);
			NUnit.Framework.Assert.IsTrue("By default JobContext.MAX_TASK_FAILURES_PER_TRACKER was "
				 + "not less than JobContext.MAP_MAX_ATTEMPTS and REDUCE_MAX_ATTEMPTS", jobConf.
				GetMaxTaskFailuresPerTracker() < jobConf.GetMaxMapAttempts() && jobConf.GetMaxTaskFailuresPerTracker
				() < jobConf.GetMaxReduceAttempts());
		}
	}
}
