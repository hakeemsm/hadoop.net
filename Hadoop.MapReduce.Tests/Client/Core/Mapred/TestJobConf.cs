using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>test JobConf</summary>
	public class TestJobConf
	{
		/// <summary>test getters and setters of JobConf</summary>
		public virtual void TestJobConf()
		{
			JobConf conf = new JobConf();
			// test default value
			Sharpen.Pattern pattern = conf.GetJarUnpackPattern();
			NUnit.Framework.Assert.AreEqual(Sharpen.Pattern.Compile("(?:classes/|lib/).*").ToString
				(), pattern.ToString());
			// default value
			NUnit.Framework.Assert.IsFalse(conf.GetKeepFailedTaskFiles());
			conf.SetKeepFailedTaskFiles(true);
			NUnit.Framework.Assert.IsTrue(conf.GetKeepFailedTaskFiles());
			// default value
			NUnit.Framework.Assert.IsNull(conf.GetKeepTaskFilesPattern());
			conf.SetKeepTaskFilesPattern("123454");
			NUnit.Framework.Assert.AreEqual("123454", conf.GetKeepTaskFilesPattern());
			// default value
			NUnit.Framework.Assert.IsNotNull(conf.GetWorkingDirectory());
			conf.SetWorkingDirectory(new Path("test"));
			NUnit.Framework.Assert.IsTrue(conf.GetWorkingDirectory().ToString().EndsWith("test"
				));
			// default value
			NUnit.Framework.Assert.AreEqual(1, conf.GetNumTasksToExecutePerJvm());
			// default value
			NUnit.Framework.Assert.IsNull(conf.GetKeyFieldComparatorOption());
			conf.SetKeyFieldComparatorOptions("keySpec");
			NUnit.Framework.Assert.AreEqual("keySpec", conf.GetKeyFieldComparatorOption());
			// default value
			NUnit.Framework.Assert.IsFalse(conf.GetUseNewReducer());
			conf.SetUseNewReducer(true);
			NUnit.Framework.Assert.IsTrue(conf.GetUseNewReducer());
			// default
			NUnit.Framework.Assert.IsTrue(conf.GetMapSpeculativeExecution());
			NUnit.Framework.Assert.IsTrue(conf.GetReduceSpeculativeExecution());
			NUnit.Framework.Assert.IsTrue(conf.GetSpeculativeExecution());
			conf.SetReduceSpeculativeExecution(false);
			NUnit.Framework.Assert.IsTrue(conf.GetSpeculativeExecution());
			conf.SetMapSpeculativeExecution(false);
			NUnit.Framework.Assert.IsFalse(conf.GetSpeculativeExecution());
			NUnit.Framework.Assert.IsFalse(conf.GetMapSpeculativeExecution());
			NUnit.Framework.Assert.IsFalse(conf.GetReduceSpeculativeExecution());
			conf.SetSessionId("ses");
			NUnit.Framework.Assert.AreEqual("ses", conf.GetSessionId());
			NUnit.Framework.Assert.AreEqual(3, conf.GetMaxTaskFailuresPerTracker());
			conf.SetMaxTaskFailuresPerTracker(2);
			NUnit.Framework.Assert.AreEqual(2, conf.GetMaxTaskFailuresPerTracker());
			NUnit.Framework.Assert.AreEqual(0, conf.GetMaxMapTaskFailuresPercent());
			conf.SetMaxMapTaskFailuresPercent(50);
			NUnit.Framework.Assert.AreEqual(50, conf.GetMaxMapTaskFailuresPercent());
			NUnit.Framework.Assert.AreEqual(0, conf.GetMaxReduceTaskFailuresPercent());
			conf.SetMaxReduceTaskFailuresPercent(70);
			NUnit.Framework.Assert.AreEqual(70, conf.GetMaxReduceTaskFailuresPercent());
			// by default
			NUnit.Framework.Assert.AreEqual(JobPriority.Normal.ToString(), conf.GetJobPriority
				().ToString());
			conf.SetJobPriority(JobPriority.High);
			NUnit.Framework.Assert.AreEqual(JobPriority.High.ToString(), conf.GetJobPriority(
				).ToString());
			NUnit.Framework.Assert.IsNull(conf.GetJobSubmitHostName());
			conf.SetJobSubmitHostName("hostname");
			NUnit.Framework.Assert.AreEqual("hostname", conf.GetJobSubmitHostName());
			// default
			NUnit.Framework.Assert.IsNull(conf.GetJobSubmitHostAddress());
			conf.SetJobSubmitHostAddress("ww");
			NUnit.Framework.Assert.AreEqual("ww", conf.GetJobSubmitHostAddress());
			// default value
			NUnit.Framework.Assert.IsFalse(conf.GetProfileEnabled());
			conf.SetProfileEnabled(true);
			NUnit.Framework.Assert.IsTrue(conf.GetProfileEnabled());
			// default value
			NUnit.Framework.Assert.AreEqual(conf.GetProfileTaskRange(true).ToString(), "0-2");
			NUnit.Framework.Assert.AreEqual(conf.GetProfileTaskRange(false).ToString(), "0-2"
				);
			conf.SetProfileTaskRange(true, "0-3");
			NUnit.Framework.Assert.AreEqual(conf.GetProfileTaskRange(false).ToString(), "0-2"
				);
			NUnit.Framework.Assert.AreEqual(conf.GetProfileTaskRange(true).ToString(), "0-3");
			// default value
			NUnit.Framework.Assert.IsNull(conf.GetMapDebugScript());
			conf.SetMapDebugScript("mDbgScript");
			NUnit.Framework.Assert.AreEqual("mDbgScript", conf.GetMapDebugScript());
			// default value
			NUnit.Framework.Assert.IsNull(conf.GetReduceDebugScript());
			conf.SetReduceDebugScript("rDbgScript");
			NUnit.Framework.Assert.AreEqual("rDbgScript", conf.GetReduceDebugScript());
			// default value
			NUnit.Framework.Assert.IsNull(conf.GetJobLocalDir());
			NUnit.Framework.Assert.AreEqual("default", conf.GetQueueName());
			conf.SetQueueName("qname");
			NUnit.Framework.Assert.AreEqual("qname", conf.GetQueueName());
			conf.SetMemoryForMapTask(100 * 1000);
			NUnit.Framework.Assert.AreEqual(100 * 1000, conf.GetMemoryForMapTask());
			conf.SetMemoryForReduceTask(1000 * 1000);
			NUnit.Framework.Assert.AreEqual(1000 * 1000, conf.GetMemoryForReduceTask());
			NUnit.Framework.Assert.AreEqual(-1, conf.GetMaxPhysicalMemoryForTask());
			NUnit.Framework.Assert.AreEqual("The variable key is no longer used.", JobConf.DeprecatedString
				("key"));
			// make sure mapreduce.map|reduce.java.opts are not set by default
			// so that they won't override mapred.child.java.opts
			NUnit.Framework.Assert.AreEqual("mapreduce.map.java.opts should not be set by default"
				, null, conf.Get(JobConf.MapredMapTaskJavaOpts));
			NUnit.Framework.Assert.AreEqual("mapreduce.reduce.java.opts should not be set by default"
				, null, conf.Get(JobConf.MapredReduceTaskJavaOpts));
		}

		/// <summary>
		/// Ensure that M/R 1.x applications can get and set task virtual memory with
		/// old property names
		/// </summary>
		public virtual void TestDeprecatedPropertyNameForTaskVmem()
		{
			JobConf configuration = new JobConf();
			configuration.SetLong(JobConf.MapredJobMapMemoryMbProperty, 1024);
			configuration.SetLong(JobConf.MapredJobReduceMemoryMbProperty, 1024);
			NUnit.Framework.Assert.AreEqual(1024, configuration.GetMemoryForMapTask());
			NUnit.Framework.Assert.AreEqual(1024, configuration.GetMemoryForReduceTask());
			// Make sure new property names aren't broken by the old ones
			configuration.SetLong(JobConf.MapreduceJobMapMemoryMbProperty, 1025);
			configuration.SetLong(JobConf.MapreduceJobReduceMemoryMbProperty, 1025);
			NUnit.Framework.Assert.AreEqual(1025, configuration.GetMemoryForMapTask());
			NUnit.Framework.Assert.AreEqual(1025, configuration.GetMemoryForReduceTask());
			configuration.SetMemoryForMapTask(2048);
			configuration.SetMemoryForReduceTask(2048);
			NUnit.Framework.Assert.AreEqual(2048, configuration.GetLong(JobConf.MapredJobMapMemoryMbProperty
				, -1));
			NUnit.Framework.Assert.AreEqual(2048, configuration.GetLong(JobConf.MapredJobReduceMemoryMbProperty
				, -1));
			// Make sure new property names aren't broken by the old ones
			NUnit.Framework.Assert.AreEqual(2048, configuration.GetLong(JobConf.MapreduceJobMapMemoryMbProperty
				, -1));
			NUnit.Framework.Assert.AreEqual(2048, configuration.GetLong(JobConf.MapreduceJobReduceMemoryMbProperty
				, -1));
		}
	}
}
