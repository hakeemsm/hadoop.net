using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class TestMapReduceChildJVM
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMapReduceChildJVM)
			);

		/// <exception cref="System.Exception"/>
		public virtual void TestCommandLine()
		{
			TestMapReduceChildJVM.MyMRApp app = new TestMapReduceChildJVM.MyMRApp(1, 0, true, 
				this.GetType().FullName, true);
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.AreEqual("[" + MRApps.CrossPlatformify("JAVA_HOME") + "/bin/java"
				 + " -Djava.net.preferIPv4Stack=true" + " -Dhadoop.metrics.log.level=WARN" + "  -Xmx200m -Djava.io.tmpdir="
				 + MRApps.CrossPlatformify("PWD") + "/tmp" + " -Dlog4j.configuration=container-log4j.properties"
				 + " -Dyarn.app.container.log.dir=<LOG_DIR>" + " -Dyarn.app.container.log.filesize=0"
				 + " -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog" + " org.apache.hadoop.mapred.YarnChild 127.0.0.1"
				 + " 54321" + " attempt_0_0000_m_000000_0" + " 0" + " 1><LOG_DIR>/stdout" + " 2><LOG_DIR>/stderr ]"
				, app.myCommandLine);
			NUnit.Framework.Assert.IsTrue("HADOOP_ROOT_LOGGER not set for job", app.cmdEnvironment
				.Contains("HADOOP_ROOT_LOGGER"));
			NUnit.Framework.Assert.AreEqual("INFO,console", app.cmdEnvironment["HADOOP_ROOT_LOGGER"
				]);
			NUnit.Framework.Assert.IsTrue("HADOOP_CLIENT_OPTS not set for job", app.cmdEnvironment
				.Contains("HADOOP_CLIENT_OPTS"));
			NUnit.Framework.Assert.AreEqual(string.Empty, app.cmdEnvironment["HADOOP_CLIENT_OPTS"
				]);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReduceCommandLineWithSeparateShuffle()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.ReduceSeparateShuffleLog, true);
			TestReduceCommandLine(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReduceCommandLineWithSeparateCRLAShuffle()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.ReduceSeparateShuffleLog, true);
			conf.SetLong(MRJobConfig.ShuffleLogKb, 1L);
			conf.SetInt(MRJobConfig.ShuffleLogBackups, 3);
			TestReduceCommandLine(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReduceCommandLine()
		{
			Configuration conf = new Configuration();
			TestReduceCommandLine(conf);
		}

		/// <exception cref="System.Exception"/>
		private void TestReduceCommandLine(Configuration conf)
		{
			TestMapReduceChildJVM.MyMRApp app = new TestMapReduceChildJVM.MyMRApp(0, 1, true, 
				this.GetType().FullName, true);
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			long shuffleLogSize = conf.GetLong(MRJobConfig.ShuffleLogKb, 0L) * 1024L;
			int shuffleBackups = conf.GetInt(MRJobConfig.ShuffleLogBackups, 0);
			string appenderName = shuffleLogSize > 0L && shuffleBackups > 0 ? "shuffleCRLA" : 
				"shuffleCLA";
			NUnit.Framework.Assert.AreEqual("[" + MRApps.CrossPlatformify("JAVA_HOME") + "/bin/java"
				 + " -Djava.net.preferIPv4Stack=true" + " -Dhadoop.metrics.log.level=WARN" + "  -Xmx200m -Djava.io.tmpdir="
				 + MRApps.CrossPlatformify("PWD") + "/tmp" + " -Dlog4j.configuration=container-log4j.properties"
				 + " -Dyarn.app.container.log.dir=<LOG_DIR>" + " -Dyarn.app.container.log.filesize=0"
				 + " -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog" + " -Dyarn.app.mapreduce.shuffle.logger=INFO,"
				 + appenderName + " -Dyarn.app.mapreduce.shuffle.logfile=syslog.shuffle" + " -Dyarn.app.mapreduce.shuffle.log.filesize="
				 + shuffleLogSize + " -Dyarn.app.mapreduce.shuffle.log.backups=" + shuffleBackups
				 + " org.apache.hadoop.mapred.YarnChild 127.0.0.1" + " 54321" + " attempt_0_0000_r_000000_0"
				 + " 0" + " 1><LOG_DIR>/stdout" + " 2><LOG_DIR>/stderr ]", app.myCommandLine);
			NUnit.Framework.Assert.IsTrue("HADOOP_ROOT_LOGGER not set for job", app.cmdEnvironment
				.Contains("HADOOP_ROOT_LOGGER"));
			NUnit.Framework.Assert.AreEqual("INFO,console", app.cmdEnvironment["HADOOP_ROOT_LOGGER"
				]);
			NUnit.Framework.Assert.IsTrue("HADOOP_CLIENT_OPTS not set for job", app.cmdEnvironment
				.Contains("HADOOP_CLIENT_OPTS"));
			NUnit.Framework.Assert.AreEqual(string.Empty, app.cmdEnvironment["HADOOP_CLIENT_OPTS"
				]);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommandLineWithLog4JConifg()
		{
			TestMapReduceChildJVM.MyMRApp app = new TestMapReduceChildJVM.MyMRApp(1, 0, true, 
				this.GetType().FullName, true);
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, true);
			string testLogPropertieFile = "test-log4j.properties";
			string testLogPropertiePath = "../" + "test-log4j.properties";
			conf.Set(MRJobConfig.MapreduceJobLog4jPropertiesFile, testLogPropertiePath);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.AreEqual("[" + MRApps.CrossPlatformify("JAVA_HOME") + "/bin/java"
				 + " -Djava.net.preferIPv4Stack=true" + " -Dhadoop.metrics.log.level=WARN" + "  -Xmx200m -Djava.io.tmpdir="
				 + MRApps.CrossPlatformify("PWD") + "/tmp" + " -Dlog4j.configuration=" + testLogPropertieFile
				 + " -Dyarn.app.container.log.dir=<LOG_DIR>" + " -Dyarn.app.container.log.filesize=0"
				 + " -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog" + " org.apache.hadoop.mapred.YarnChild 127.0.0.1"
				 + " 54321" + " attempt_0_0000_m_000000_0" + " 0" + " 1><LOG_DIR>/stdout" + " 2><LOG_DIR>/stderr ]"
				, app.myCommandLine);
		}

		private sealed class MyMRApp : MRApp
		{
			private string myCommandLine;

			private IDictionary<string, string> cmdEnvironment;

			public MyMRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
				)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
				context)
			{
				return new _MockContainerLauncher_190(this);
			}

			private sealed class _MockContainerLauncher_190 : MRApp.MockContainerLauncher
			{
				public _MockContainerLauncher_190(MyMRApp _enclosing)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(ContainerLauncherEvent @event)
				{
					if (@event.GetType() == ContainerLauncher.EventType.ContainerRemoteLaunch)
					{
						ContainerRemoteLaunchEvent launchEvent = (ContainerRemoteLaunchEvent)@event;
						ContainerLaunchContext launchContext = launchEvent.GetContainerLaunchContext();
						string cmdString = launchContext.GetCommands().ToString();
						TestMapReduceChildJVM.Log.Info("launchContext " + cmdString);
						this._enclosing.myCommandLine = cmdString;
						this._enclosing.cmdEnvironment = launchContext.GetEnvironment();
					}
					base.Handle(@event);
				}

				private readonly MyMRApp _enclosing;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnvironmentVariables()
		{
			TestMapReduceChildJVM.MyMRApp app = new TestMapReduceChildJVM.MyMRApp(1, 0, true, 
				this.GetType().FullName, true);
			Configuration conf = new Configuration();
			conf.Set(JobConf.MapredMapTaskEnv, "HADOOP_CLIENT_OPTS=test");
			conf.SetStrings(MRJobConfig.MapLogLevel, "WARN");
			conf.SetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.IsTrue("HADOOP_ROOT_LOGGER not set for job", app.cmdEnvironment
				.Contains("HADOOP_ROOT_LOGGER"));
			NUnit.Framework.Assert.AreEqual("WARN,console", app.cmdEnvironment["HADOOP_ROOT_LOGGER"
				]);
			NUnit.Framework.Assert.IsTrue("HADOOP_CLIENT_OPTS not set for job", app.cmdEnvironment
				.Contains("HADOOP_CLIENT_OPTS"));
			NUnit.Framework.Assert.AreEqual("test", app.cmdEnvironment["HADOOP_CLIENT_OPTS"]);
			// Try one more.
			app = new TestMapReduceChildJVM.MyMRApp(1, 0, true, this.GetType().FullName, true
				);
			conf = new Configuration();
			conf.Set(JobConf.MapredMapTaskEnv, "HADOOP_ROOT_LOGGER=trace");
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.IsTrue("HADOOP_ROOT_LOGGER not set for job", app.cmdEnvironment
				.Contains("HADOOP_ROOT_LOGGER"));
			NUnit.Framework.Assert.AreEqual("trace", app.cmdEnvironment["HADOOP_ROOT_LOGGER"]
				);
		}
	}
}
