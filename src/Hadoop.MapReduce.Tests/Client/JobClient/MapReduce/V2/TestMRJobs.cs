using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.FS.Viewfs;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Speculate;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMRJobs
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRJobs));

		private static readonly EnumSet<RMAppState> TerminalRmAppStates = EnumSet.Of(RMAppState
			.Finished, RMAppState.Failed, RMAppState.Killed);

		private const int NumNodeMgrs = 3;

		private const string TestIoSortMb = "11";

		private const int DefaultReduces = 2;

		protected internal int numSleepReducers = DefaultReduces;

		protected internal static MiniMRYarnCluster mrCluster;

		protected internal static MiniDFSCluster dfsCluster;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs;

		private static FileSystem remoteFs;

		static TestMRJobs()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static Path TestRootDir = new Path("target", typeof(TestMRJobs).FullName 
			+ "-tmpDir").MakeQualified(localFs);

		internal static Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		private static readonly string OutputRootDir = "/tmp/" + typeof(TestMRJobs).Name;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			try
			{
				dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(true).Racks(
					null).Build();
				remoteFs = dfsCluster.GetFileSystem();
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem starting mini dfs cluster", io);
			}
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster == null)
			{
				mrCluster = new MiniMRYarnCluster(typeof(TestMRJobs).FullName, NumNodeMgrs);
				Configuration conf = new Configuration();
				conf.Set("fs.defaultFS", remoteFs.GetUri().ToString());
				// use HDFS
				conf.Set(MRJobConfig.MrAmStagingDir, "/apps_staging_dir");
				mrCluster.Init(conf);
				mrCluster.Start();
			}
			// Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
			// workaround the absent public discache.
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
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
				dfsCluster = null;
			}
		}

		[NUnit.Framework.TearDown]
		public virtual void ResetInit()
		{
			numSleepReducers = DefaultReduces;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSleepJob()
		{
			TestSleepJobInternal(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSleepJobWithRemoteJar()
		{
			TestSleepJobInternal(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestSleepJobInternal(bool useRemoteJar)
		{
			Log.Info("\n\n\nStarting testSleepJob: useRemoteJar=" + useRemoteJar);
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			Configuration sleepConf = new Configuration(mrCluster.GetConfig());
			// set master address to local to test that local mode applied iff framework == local
			sleepConf.Set(MRConfig.MasterAddress, "local");
			SleepJob sleepJob = new SleepJob();
			sleepJob.SetConf(sleepConf);
			// job with 3 maps (10s) and numReduces reduces (5s), 1 "record" each:
			Job job = sleepJob.CreateJob(3, numSleepReducers, 10000, 1, 5000, 1);
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			if (useRemoteJar)
			{
				Path localJar = new Path(ClassUtil.FindContainingJar(typeof(SleepJob)));
				ConfigUtil.AddLink(job.GetConfiguration(), "/jobjars", localFs.MakeQualified(localJar
					.GetParent()).ToUri());
				job.SetJar("viewfs:///jobjars/" + localJar.GetName());
			}
			else
			{
				job.SetJarByClass(typeof(SleepJob));
			}
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.Submit();
			string trackingUrl = job.GetTrackingURL();
			string jobId = job.GetJobID().ToString();
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
			NUnit.Framework.Assert.IsTrue("Tracking URL was " + trackingUrl + " but didn't Match Job ID "
				 + jobId, trackingUrl.EndsWith(Sharpen.Runtime.Substring(jobId, jobId.LastIndexOf
				("_")) + "/"));
			VerifySleepJobCounters(job);
			VerifyTaskProgress(job);
		}

		// TODO later:  add explicit "isUber()" checks of some sort (extend
		// JobStatus?)--compare against MRJobConfig.JOB_UBERTASK_ENABLE value
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestJobClassloader()
		{
			TestJobClassloader(false);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestJobClassloaderWithCustomClasses()
		{
			TestJobClassloader(true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private void TestJobClassloader(bool useCustomClasses)
		{
			Log.Info("\n\n\nStarting testJobClassloader()" + " useCustomClasses=" + useCustomClasses
				);
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			Configuration sleepConf = new Configuration(mrCluster.GetConfig());
			// set master address to local to test that local mode applied iff framework == local
			sleepConf.Set(MRConfig.MasterAddress, "local");
			sleepConf.SetBoolean(MRJobConfig.MapreduceJobClassloader, true);
			if (useCustomClasses)
			{
				// to test AM loading user classes such as output format class, we want
				// to blacklist them from the system classes (they need to be prepended
				// as the first match wins)
				string systemClasses = ApplicationClassLoader.SystemClassesDefault;
				// exclude the custom classes from system classes
				systemClasses = "-" + typeof(TestMRJobs.CustomOutputFormat).FullName + ",-" + typeof(
					TestMRJobs.CustomSpeculator).FullName + "," + systemClasses;
				sleepConf.Set(MRJobConfig.MapreduceJobClassloaderSystemClasses, systemClasses);
			}
			sleepConf.Set(MRJobConfig.IoSortMb, TestIoSortMb);
			sleepConf.Set(MRJobConfig.MrAmLogLevel, Level.All.ToString());
			sleepConf.Set(MRJobConfig.MapLogLevel, Level.All.ToString());
			sleepConf.Set(MRJobConfig.ReduceLogLevel, Level.All.ToString());
			sleepConf.Set(MRJobConfig.MapJavaOpts, "-verbose:class");
			SleepJob sleepJob = new SleepJob();
			sleepJob.SetConf(sleepConf);
			Job job = sleepJob.CreateJob(1, 1, 10, 1, 10, 1);
			job.SetMapperClass(typeof(TestMRJobs.ConfVerificationMapper));
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.SetJarByClass(typeof(SleepJob));
			job.SetMaxMapAttempts(1);
			// speed up failures
			if (useCustomClasses)
			{
				// set custom output format class and speculator class
				job.SetOutputFormatClass(typeof(TestMRJobs.CustomOutputFormat));
				Configuration jobConf = job.GetConfiguration();
				jobConf.SetClass(MRJobConfig.MrAmJobSpeculator, typeof(TestMRJobs.CustomSpeculator
					), typeof(Speculator));
				// speculation needs to be enabled for the speculator to be loaded
				jobConf.SetBoolean(MRJobConfig.MapSpeculative, true);
			}
			job.Submit();
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job status: " + job.GetStatus().GetFailureInfo(), 
				succeeded);
		}

		public class CustomOutputFormat<K, V> : NullOutputFormat<K, V>
		{
			public CustomOutputFormat()
			{
				VerifyClassLoader(GetType());
			}

			/// <summary>
			/// Verifies that the class was loaded by the job classloader if it is in the
			/// context of the MRAppMaster, and if not throws an exception to fail the
			/// job.
			/// </summary>
			private void VerifyClassLoader(Type cls)
			{
				// to detect that it is instantiated in the context of the MRAppMaster, we
				// inspect the stack trace and determine a caller is MRAppMaster
				foreach (StackTraceElement e in new Exception().GetStackTrace())
				{
					if (e.GetClassName().Equals(typeof(MRAppMaster).FullName) && !(cls.GetClassLoader
						() is ApplicationClassLoader))
					{
						throw new ExceptionInInitializerError("incorrect classloader used");
					}
				}
			}
		}

		public class CustomSpeculator : DefaultSpeculator
		{
			public CustomSpeculator(Configuration conf, AppContext context)
				: base(conf, context)
			{
				VerifyClassLoader(GetType());
			}

			/// <summary>
			/// Verifies that the class was loaded by the job classloader if it is in the
			/// context of the MRAppMaster, and if not throws an exception to fail the
			/// job.
			/// </summary>
			private void VerifyClassLoader(Type cls)
			{
				// to detect that it is instantiated in the context of the MRAppMaster, we
				// inspect the stack trace and determine a caller is MRAppMaster
				foreach (StackTraceElement e in new Exception().GetStackTrace())
				{
					if (e.GetClassName().Equals(typeof(MRAppMaster).FullName) && !(cls.GetClassLoader
						() is ApplicationClassLoader))
					{
						throw new ExceptionInInitializerError("incorrect classloader used");
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void VerifySleepJobCounters(Job job)
		{
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.OtherLocalMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(numSleepReducers, counters.FindCounter(JobCounter
				.TotalLaunchedReduces).GetValue());
			NUnit.Framework.Assert.IsTrue(counters.FindCounter(JobCounter.SlotsMillisMaps) !=
				 null && counters.FindCounter(JobCounter.SlotsMillisMaps).GetValue() != 0);
			NUnit.Framework.Assert.IsTrue(counters.FindCounter(JobCounter.SlotsMillisMaps) !=
				 null && counters.FindCounter(JobCounter.SlotsMillisMaps).GetValue() != 0);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void VerifyTaskProgress(Job job)
		{
			foreach (TaskReport taskReport in job.GetTaskReports(TaskType.Map))
			{
				NUnit.Framework.Assert.IsTrue(0.9999f < taskReport.GetProgress() && 1.0001f > taskReport
					.GetProgress());
			}
			foreach (TaskReport taskReport_1 in job.GetTaskReports(TaskType.Reduce))
			{
				NUnit.Framework.Assert.IsTrue(0.9999f < taskReport_1.GetProgress() && 1.0001f > taskReport_1
					.GetProgress());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestRandomWriter()
		{
			Log.Info("\n\n\nStarting testRandomWriter().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			RandomTextWriterJob randomWriterJob = new RandomTextWriterJob();
			mrCluster.GetConfig().Set(RandomTextWriterJob.TotalBytes, "3072");
			mrCluster.GetConfig().Set(RandomTextWriterJob.BytesPerMap, "1024");
			Job job = randomWriterJob.CreateJob(mrCluster.GetConfig());
			Path outputDir = new Path(OutputRootDir, "random-output");
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetSpeculativeExecution(false);
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.SetJarByClass(typeof(RandomTextWriterJob));
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.Submit();
			string trackingUrl = job.GetTrackingURL();
			string jobId = job.GetJobID().ToString();
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(succeeded);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
			NUnit.Framework.Assert.IsTrue("Tracking URL was " + trackingUrl + " but didn't Match Job ID "
				 + jobId, trackingUrl.EndsWith(Sharpen.Runtime.Substring(jobId, jobId.LastIndexOf
				("_")) + "/"));
			// Make sure there are three files in the output-dir
			RemoteIterator<FileStatus> iterator = FileContext.GetFileContext(mrCluster.GetConfig
				()).ListStatus(outputDir);
			int count = 0;
			while (iterator.HasNext())
			{
				FileStatus file = iterator.Next();
				if (!file.GetPath().GetName().Equals(FileOutputCommitter.SucceededFileName))
				{
					count++;
				}
			}
			NUnit.Framework.Assert.AreEqual("Number of part files is wrong!", 3, count);
			VerifyRandomWriterCounters(job);
		}

		// TODO later:  add explicit "isUber()" checks of some sort
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void VerifyRandomWriterCounters(Job job)
		{
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.OtherLocalMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.IsTrue(counters.FindCounter(JobCounter.SlotsMillisMaps) !=
				 null && counters.FindCounter(JobCounter.SlotsMillisMaps).GetValue() != 0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestFailingMapper()
		{
			Log.Info("\n\n\nStarting testFailingMapper().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			Job job = RunFailingMapperJob();
			TaskID taskID = new TaskID(job.GetJobID(), TaskType.Map, 0);
			TaskAttemptID aId = new TaskAttemptID(taskID, 0);
			System.Console.Out.WriteLine("Diagnostics for " + aId + " :");
			foreach (string diag in job.GetTaskDiagnostics(aId))
			{
				System.Console.Out.WriteLine(diag);
			}
			aId = new TaskAttemptID(taskID, 1);
			System.Console.Out.WriteLine("Diagnostics for " + aId + " :");
			foreach (string diag_1 in job.GetTaskDiagnostics(aId))
			{
				System.Console.Out.WriteLine(diag_1);
			}
			TaskCompletionEvent[] events = job.GetTaskCompletionEvents(0, 2);
			NUnit.Framework.Assert.AreEqual(TaskCompletionEvent.Status.Failed, events[0].GetStatus
				());
			NUnit.Framework.Assert.AreEqual(TaskCompletionEvent.Status.Tipfailed, events[1].GetStatus
				());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Failed, job.GetJobState());
			VerifyFailingMapperCounters(job);
		}

		// TODO later:  add explicit "isUber()" checks of some sort
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void VerifyFailingMapperCounters(Job job)
		{
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.OtherLocalMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedMaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.NumFailedMaps)
				.GetValue());
			NUnit.Framework.Assert.IsTrue(counters.FindCounter(JobCounter.SlotsMillisMaps) !=
				 null && counters.FindCounter(JobCounter.SlotsMillisMaps).GetValue() != 0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		protected internal virtual Job RunFailingMapperJob()
		{
			Configuration myConf = new Configuration(mrCluster.GetConfig());
			myConf.SetInt(MRJobConfig.NumMaps, 1);
			myConf.SetInt(MRJobConfig.MapMaxAttempts, 2);
			//reduce the number of attempts
			Job job = Job.GetInstance(myConf);
			job.SetJarByClass(typeof(FailingMapper));
			job.SetJobName("failmapper");
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetInputFormatClass(typeof(RandomTextWriterJob.RandomInputFormat));
			job.SetOutputFormatClass(typeof(TextOutputFormat));
			job.SetMapperClass(typeof(FailingMapper));
			job.SetNumReduceTasks(0);
			FileOutputFormat.SetOutputPath(job, new Path(OutputRootDir, "failmapper-output"));
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.Submit();
			string trackingUrl = job.GetTrackingURL();
			string jobId = job.GetJobID().ToString();
			bool succeeded = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsFalse(succeeded);
			NUnit.Framework.Assert.IsTrue("Tracking URL was " + trackingUrl + " but didn't Match Job ID "
				 + jobId, trackingUrl.EndsWith(Sharpen.Runtime.Substring(jobId, jobId.LastIndexOf
				("_")) + "/"));
			return job;
		}

		//@Test (timeout = 60000)
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestSleepJobWithSecurityOn()
		{
			Log.Info("\n\n\nStarting testSleepJobWithSecurityOn().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				return;
			}
			mrCluster.GetConfig().Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication
				, "kerberos");
			mrCluster.GetConfig().Set(YarnConfiguration.RmKeytab, "/etc/krb5.keytab");
			mrCluster.GetConfig().Set(YarnConfiguration.NmKeytab, "/etc/krb5.keytab");
			mrCluster.GetConfig().Set(YarnConfiguration.RmPrincipal, "rm/sightbusy-lx@LOCALHOST"
				);
			mrCluster.GetConfig().Set(YarnConfiguration.NmPrincipal, "nm/sightbusy-lx@LOCALHOST"
				);
			UserGroupInformation.SetConfiguration(mrCluster.GetConfig());
			// Keep it in here instead of after RM/NM as multiple user logins happen in
			// the same JVM.
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			Log.Info("User name is " + user.GetUserName());
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> str in user.GetTokens
				())
			{
				Log.Info("Token is " + str.EncodeToUrlString());
			}
			user.DoAs(new _PrivilegedExceptionAction_552());
		}

		private sealed class _PrivilegedExceptionAction_552 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_552()
			{
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				SleepJob sleepJob = new SleepJob();
				sleepJob.SetConf(TestMRJobs.mrCluster.GetConfig());
				Job job = sleepJob.CreateJob(3, 0, 10000, 1, 0, 0);
				// //Job with reduces
				// Job job = sleepJob.createJob(3, 2, 10000, 1, 10000, 1);
				job.AddFileToClassPath(TestMRJobs.AppJar);
				// The AppMaster jar itself.
				job.Submit();
				string trackingUrl = job.GetTrackingURL();
				string jobId = job.GetJobID().ToString();
				job.WaitForCompletion(true);
				NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, job.GetJobState());
				NUnit.Framework.Assert.IsTrue("Tracking URL was " + trackingUrl + " but didn't Match Job ID "
					 + jobId, trackingUrl.EndsWith(Sharpen.Runtime.Substring(jobId, jobId.LastIndexOf
					("_")) + "/"));
				return null;
			}
		}

		// TODO later:  add explicit "isUber()" checks of some sort
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void TestContainerRollingLog()
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			SleepJob sleepJob = new SleepJob();
			JobConf sleepConf = new JobConf(mrCluster.GetConfig());
			sleepConf.Set(MRJobConfig.MapLogLevel, Level.All.ToString());
			long userLogKb = 4;
			sleepConf.SetLong(MRJobConfig.TaskUserlogLimit, userLogKb);
			sleepConf.SetInt(MRJobConfig.TaskLogBackups, 3);
			sleepConf.Set(MRJobConfig.MrAmLogLevel, Level.All.ToString());
			long amLogKb = 7;
			sleepConf.SetLong(MRJobConfig.MrAmLogKb, amLogKb);
			sleepConf.SetInt(MRJobConfig.MrAmLogBackups, 7);
			sleepJob.SetConf(sleepConf);
			Job job = sleepJob.CreateJob(1, 0, 1L, 100, 0L, 0);
			job.SetJarByClass(typeof(SleepJob));
			job.AddFileToClassPath(AppJar);
			// The AppMaster jar itself.
			job.WaitForCompletion(true);
			JobId jobId = TypeConverter.ToYarn(job.GetJobID());
			ApplicationId appID = jobId.GetAppId();
			int pollElapsed = 0;
			while (true)
			{
				Sharpen.Thread.Sleep(1000);
				pollElapsed += 1000;
				if (TerminalRmAppStates.Contains(mrCluster.GetResourceManager().GetRMContext().GetRMApps
					()[appID].GetState()))
				{
					break;
				}
				if (pollElapsed >= 60000)
				{
					Log.Warn("application did not reach terminal state within 60 seconds");
					break;
				}
			}
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, mrCluster.GetResourceManager
				().GetRMContext().GetRMApps()[appID].GetState());
			// Job finished, verify logs
			//
			string appIdStr = appID.ToString();
			string appIdSuffix = Sharpen.Runtime.Substring(appIdStr, "application_".Length, appIdStr
				.Length);
			string containerGlob = "container_" + appIdSuffix + "_*_*";
			string syslogGlob = appIdStr + Path.Separator + containerGlob + Path.Separator + 
				TaskLog.LogName.Syslog;
			int numAppMasters = 0;
			int numMapTasks = 0;
			for (int i = 0; i < NumNodeMgrs; i++)
			{
				Configuration nmConf = mrCluster.GetNodeManager(i).GetConfig();
				foreach (string logDir in nmConf.GetTrimmedStrings(YarnConfiguration.NmLogDirs))
				{
					Path absSyslogGlob = new Path(logDir + Path.Separator + syslogGlob);
					Log.Info("Checking for glob: " + absSyslogGlob);
					FileStatus[] syslogs = localFs.GlobStatus(absSyslogGlob);
					foreach (FileStatus slog in syslogs)
					{
						bool foundAppMaster = job.IsUber();
						Path containerPathComponent = slog.GetPath().GetParent();
						if (!foundAppMaster)
						{
							ContainerId cid = ConverterUtils.ToContainerId(containerPathComponent.GetName());
							foundAppMaster = ((cid.GetContainerId() & ContainerId.ContainerIdBitmask) == 1);
						}
						FileStatus[] sysSiblings = localFs.GlobStatus(new Path(containerPathComponent, TaskLog.LogName
							.Syslog + "*"));
						// sort to ensure for i > 0 sysSiblings[i] == "syslog.i"
						Arrays.Sort(sysSiblings);
						if (foundAppMaster)
						{
							numAppMasters++;
						}
						else
						{
							numMapTasks++;
						}
						if (foundAppMaster)
						{
							NUnit.Framework.Assert.AreSame("Unexpected number of AM sylog* files", sleepConf.
								GetInt(MRJobConfig.MrAmLogBackups, 0) + 1, sysSiblings.Length);
							NUnit.Framework.Assert.IsTrue("AM syslog.1 length kb should be >= " + amLogKb, sysSiblings
								[1].GetLen() >= amLogKb * 1024);
						}
						else
						{
							NUnit.Framework.Assert.AreSame("Unexpected number of MR task sylog* files", sleepConf
								.GetInt(MRJobConfig.TaskLogBackups, 0) + 1, sysSiblings.Length);
							NUnit.Framework.Assert.IsTrue("MR syslog.1 length kb should be >= " + userLogKb, 
								sysSiblings[1].GetLen() >= userLogKb * 1024);
						}
					}
				}
			}
			// Make sure we checked non-empty set
			//
			NUnit.Framework.Assert.AreEqual("No AppMaster log found!", 1, numAppMasters);
			if (sleepConf.GetBoolean(MRJobConfig.JobUbertaskEnable, false))
			{
				NUnit.Framework.Assert.AreEqual("MapTask log with uber found!", 0, numMapTasks);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("No MapTask log found!", 1, numMapTasks);
			}
		}

		public class DistributedCacheChecker : Mapper<LongWritable, Text, NullWritable, NullWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				Path[] localFiles = context.GetLocalCacheFiles();
				URI[] files = context.GetCacheFiles();
				Path[] localArchives = context.GetLocalCacheArchives();
				URI[] archives = context.GetCacheArchives();
				// Check that 4 (2 + appjar + DistrubutedCacheChecker jar) files 
				// and 2 archives are present
				NUnit.Framework.Assert.AreEqual(4, localFiles.Length);
				NUnit.Framework.Assert.AreEqual(4, files.Length);
				NUnit.Framework.Assert.AreEqual(2, localArchives.Length);
				NUnit.Framework.Assert.AreEqual(2, archives.Length);
				// Check lengths of the files
				IDictionary<string, Path> filesMap = PathsToMap(localFiles);
				NUnit.Framework.Assert.IsTrue(filesMap.Contains("distributed.first.symlink"));
				NUnit.Framework.Assert.AreEqual(1, localFs.GetFileStatus(filesMap["distributed.first.symlink"
					]).GetLen());
				NUnit.Framework.Assert.IsTrue(filesMap.Contains("distributed.second.jar"));
				NUnit.Framework.Assert.IsTrue(localFs.GetFileStatus(filesMap["distributed.second.jar"
					]).GetLen() > 1);
				// Check extraction of the archive
				IDictionary<string, Path> archivesMap = PathsToMap(localArchives);
				NUnit.Framework.Assert.IsTrue(archivesMap.Contains("distributed.third.jar"));
				NUnit.Framework.Assert.IsTrue(localFs.Exists(new Path(archivesMap["distributed.third.jar"
					], "distributed.jar.inside3")));
				NUnit.Framework.Assert.IsTrue(archivesMap.Contains("distributed.fourth.jar"));
				NUnit.Framework.Assert.IsTrue(localFs.Exists(new Path(archivesMap["distributed.fourth.jar"
					], "distributed.jar.inside4")));
				// Check the class loaders
				Log.Info("Java Classpath: " + Runtime.GetProperty("java.class.path"));
				ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
				// Both the file and the archive should have been added to classpath, so
				// both should be reachable via the class loader.
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("distributed.jar.inside2"));
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("distributed.jar.inside3"));
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("distributed.jar.inside4"));
				// The Job Jar should have been extracted to a folder named "job.jar" and
				// added to the classpath; the two jar files in the lib folder in the Job
				// Jar should have also been added to the classpath
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("job.jar/"));
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("job.jar/lib/lib1.jar"));
				NUnit.Framework.Assert.IsNotNull(cl.GetResource("job.jar/lib/lib2.jar"));
				// Check that the symlink for the renaming was created in the cwd;
				FilePath symlinkFile = new FilePath("distributed.first.symlink");
				NUnit.Framework.Assert.IsTrue(symlinkFile.Exists());
				NUnit.Framework.Assert.AreEqual(1, symlinkFile.Length());
				// Check that the symlink for the Job Jar was created in the cwd and
				// points to the extracted directory
				FilePath jobJarDir = new FilePath("job.jar");
				if (Shell.Windows)
				{
					NUnit.Framework.Assert.IsTrue(IsWindowsSymlinkedDirectory(jobJarDir));
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(FileUtils.IsSymlink(jobJarDir));
					NUnit.Framework.Assert.IsTrue(jobJarDir.IsDirectory());
				}
			}

			/// <summary>
			/// Used on Windows to determine if the specified file is a symlink that
			/// targets a directory.
			/// </summary>
			/// <remarks>
			/// Used on Windows to determine if the specified file is a symlink that
			/// targets a directory.  On most platforms, these checks can be done using
			/// commons-io.  On Windows, the commons-io implementation is unreliable and
			/// always returns false.  Instead, this method checks the output of the dir
			/// command.  After migrating to Java 7, this method can be removed in favor
			/// of the new method java.nio.file.Files.isSymbolicLink, which is expected to
			/// work cross-platform.
			/// </remarks>
			/// <param name="file">File to check</param>
			/// <returns>boolean true if the file is a symlink that targets a directory</returns>
			/// <exception cref="System.IO.IOException">thrown for any I/O error</exception>
			private static bool IsWindowsSymlinkedDirectory(FilePath file)
			{
				string dirOut = Shell.ExecCommand("cmd", "/c", "dir", file.GetAbsoluteFile().GetParent
					());
				StringReader sr = new StringReader(dirOut);
				BufferedReader br = new BufferedReader(sr);
				try
				{
					string line = br.ReadLine();
					while (line != null)
					{
						line = br.ReadLine();
						if (line.Contains(file.GetName()) && line.Contains("<SYMLINKD>"))
						{
							return true;
						}
					}
					return false;
				}
				finally
				{
					IOUtils.CloseStream(br);
					IOUtils.CloseStream(sr);
				}
			}

			/// <summary>
			/// Returns a mapping of the final component of each path to the corresponding
			/// Path instance.
			/// </summary>
			/// <remarks>
			/// Returns a mapping of the final component of each path to the corresponding
			/// Path instance.  This assumes that every given Path has a unique string in
			/// the final path component, which is true for these tests.
			/// </remarks>
			/// <param name="paths">Path[] to map</param>
			/// <returns>
			/// Map<String, Path> mapping the final component of each path to the
			/// corresponding Path instance
			/// </returns>
			private static IDictionary<string, Path> PathsToMap(Path[] paths)
			{
				IDictionary<string, Path> map = new Dictionary<string, Path>();
				foreach (Path path in paths)
				{
					map[path.GetName()] = path;
				}
				return map;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void _testDistributedCache(string jobJarPath)
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			// Create a temporary file of length 1.
			Path first = CreateTempFile("distributed.first", "x");
			// Create two jars with a single file inside them.
			Path second = MakeJar(new Path(TestRootDir, "distributed.second.jar"), 2);
			Path third = MakeJar(new Path(TestRootDir, "distributed.third.jar"), 3);
			Path fourth = MakeJar(new Path(TestRootDir, "distributed.fourth.jar"), 4);
			Job job = Job.GetInstance(mrCluster.GetConfig());
			// Set the job jar to a new "dummy" jar so we can check that its extracted 
			// properly
			job.SetJar(jobJarPath);
			// Because the job jar is a "dummy" jar, we need to include the jar with
			// DistributedCacheChecker or it won't be able to find it
			Path distributedCacheCheckerJar = new Path(JarFinder.GetJar(typeof(TestMRJobs.DistributedCacheChecker
				)));
			job.AddFileToClassPath(distributedCacheCheckerJar.MakeQualified(localFs.GetUri(), 
				distributedCacheCheckerJar.GetParent()));
			job.SetMapperClass(typeof(TestMRJobs.DistributedCacheChecker));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			FileInputFormat.SetInputPaths(job, first);
			// Creates the Job Configuration
			job.AddCacheFile(new URI(first.ToUri().ToString() + "#distributed.first.symlink")
				);
			job.AddFileToClassPath(second);
			// The AppMaster jar itself
			job.AddFileToClassPath(AppJar.MakeQualified(localFs.GetUri(), AppJar.GetParent())
				);
			job.AddArchiveToClassPath(third);
			job.AddCacheArchive(fourth.ToUri());
			job.SetMaxMapAttempts(1);
			// speed up failures
			job.Submit();
			string trackingUrl = job.GetTrackingURL();
			string jobId = job.GetJobID().ToString();
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(false));
			NUnit.Framework.Assert.IsTrue("Tracking URL was " + trackingUrl + " but didn't Match Job ID "
				 + jobId, trackingUrl.EndsWith(Sharpen.Runtime.Substring(jobId, jobId.LastIndexOf
				("_")) + "/"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDistributedCache()
		{
			// Test with a local (file:///) Job Jar
			Path localJobJarPath = MakeJobJarWithLib(TestRootDir.ToUri().ToString());
			_testDistributedCache(localJobJarPath.ToUri().ToString());
			// Test with a remote (hdfs://) Job Jar
			Path remoteJobJarPath = new Path(remoteFs.GetUri().ToString() + "/", localJobJarPath
				.GetName());
			remoteFs.MoveFromLocalFile(localJobJarPath, remoteJobJarPath);
			FilePath localJobJarFile = new FilePath(localJobJarPath.ToUri().ToString());
			if (localJobJarFile.Exists())
			{
				// just to make sure
				localJobJarFile.Delete();
			}
			_testDistributedCache(remoteJobJarPath.ToUri().ToString());
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

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private Path MakeJar(Path p, int index)
		{
			FileOutputStream fos = new FileOutputStream(new FilePath(p.ToUri().GetPath()));
			JarOutputStream jos = new JarOutputStream(fos);
			ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
			jos.PutNextEntry(ze);
			jos.Write(Sharpen.Runtime.GetBytesForString(("inside the jar!" + index)));
			jos.CloseEntry();
			jos.Close();
			localFs.SetPermission(p, new FsPermission("700"));
			return p;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private Path MakeJobJarWithLib(string testDir)
		{
			Path jobJarPath = new Path(testDir, "thejob.jar");
			FileOutputStream fos = new FileOutputStream(new FilePath(jobJarPath.ToUri().GetPath
				()));
			JarOutputStream jos = new JarOutputStream(fos);
			// Have to put in real jar files or it will complain
			CreateAndAddJarToJar(jos, new FilePath(new Path(testDir, "lib1.jar").ToUri().GetPath
				()));
			CreateAndAddJarToJar(jos, new FilePath(new Path(testDir, "lib2.jar").ToUri().GetPath
				()));
			jos.Close();
			localFs.SetPermission(jobJarPath, new FsPermission("700"));
			return jobJarPath;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		private void CreateAndAddJarToJar(JarOutputStream jos, FilePath jarFile)
		{
			FileOutputStream fos2 = new FileOutputStream(jarFile);
			JarOutputStream jos2 = new JarOutputStream(fos2);
			// Have to have at least one entry or it will complain
			ZipEntry ze = new ZipEntry("lib1.inside");
			jos2.PutNextEntry(ze);
			jos2.CloseEntry();
			jos2.Close();
			ze = new ZipEntry("lib/" + jarFile.GetName());
			jos.PutNextEntry(ze);
			FileInputStream @in = new FileInputStream(jarFile);
			byte[] buf = new byte[1024];
			int numRead;
			do
			{
				numRead = @in.Read(buf);
				if (numRead >= 0)
				{
					jos.Write(buf, 0, numRead);
				}
			}
			while (numRead != -1);
			@in.Close();
			jos.CloseEntry();
			jarFile.Delete();
		}

		public class ConfVerificationMapper : SleepJob.SleepMapper
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				base.Setup(context);
				Configuration conf = context.GetConfiguration();
				string ioSortMb = conf.Get(MRJobConfig.IoSortMb);
				if (!TestIoSortMb.Equals(ioSortMb))
				{
					throw new IOException("io.sort.mb expected: " + TestIoSortMb + ", actual: " + ioSortMb
						);
				}
			}
		}
	}
}
