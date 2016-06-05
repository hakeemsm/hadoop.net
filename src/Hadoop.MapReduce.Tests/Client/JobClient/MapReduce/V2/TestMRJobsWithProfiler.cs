using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMRJobsWithProfiler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRJobsWithProfiler
			));

		private static readonly EnumSet<RMAppState> TerminalRmAppStates = EnumSet.Of(RMAppState
			.Finished, RMAppState.Failed, RMAppState.Killed);

		private const int ProfiledTaskId = 1;

		private static MiniMRYarnCluster mrCluster;

		private static readonly Configuration Conf = new Configuration();

		private static readonly FileSystem localFs;

		static TestMRJobsWithProfiler()
		{
			try
			{
				localFs = FileSystem.GetLocal(Conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException("problem getting local fs", io);
			}
		}

		private static readonly Path TestRootDir = new Path("target", typeof(TestMRJobs).
			FullName + "-tmpDir").MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory
			());

		private static readonly Path AppJar = new Path(TestRootDir, "MRAppJar.jar");

		/// <exception cref="System.Exception"/>
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
				mrCluster = new MiniMRYarnCluster(typeof(TestMRJobsWithProfiler).FullName);
				mrCluster.Init(Conf);
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
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			if (mrCluster != null)
			{
				mrCluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDefaultProfiler()
		{
			Log.Info("Starting testDefaultProfiler");
			TestProfilerInternal(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDifferentProfilers()
		{
			Log.Info("Starting testDefaultProfiler");
			TestProfilerInternal(false);
		}

		/// <exception cref="System.Exception"/>
		private void TestProfilerInternal(bool useDefault)
		{
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			SleepJob sleepJob = new SleepJob();
			JobConf sleepConf = new JobConf(mrCluster.GetConfig());
			sleepConf.SetProfileEnabled(true);
			sleepConf.SetProfileTaskRange(true, ProfiledTaskId.ToString());
			sleepConf.SetProfileTaskRange(false, ProfiledTaskId.ToString());
			if (!useDefault)
			{
				// use hprof for map to profile.out
				sleepConf.Set(MRJobConfig.TaskMapProfileParams, "-agentlib:hprof=cpu=times,heap=sites,force=n,thread=y,verbose=n,"
					 + "file=%s");
				// use Xprof for reduce to stdout
				sleepConf.Set(MRJobConfig.TaskReduceProfileParams, "-Xprof");
			}
			sleepJob.SetConf(sleepConf);
			// 2-map-2-reduce SleepJob
			Job job = sleepJob.CreateJob(2, 2, 500, 1, 500, 1);
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
			Configuration nmConf = mrCluster.GetNodeManager(0).GetConfig();
			string appIdStr = appID.ToString();
			string appIdSuffix = Sharpen.Runtime.Substring(appIdStr, "application_".Length, appIdStr
				.Length);
			string containerGlob = "container_" + appIdSuffix + "_*_*";
			IDictionary<TaskAttemptID, Path> taLogDirs = new Dictionary<TaskAttemptID, Path>(
				);
			Sharpen.Pattern taskPattern = Sharpen.Pattern.Compile(".*Task:(attempt_" + appIdSuffix
				 + "_[rm]_" + "[0-9]+_[0-9]+).*");
			foreach (string logDir in nmConf.GetTrimmedStrings(YarnConfiguration.NmLogDirs))
			{
				// filter out MRAppMaster and create attemptId->logDir map
				//
				foreach (FileStatus fileStatus in localFs.GlobStatus(new Path(logDir + Path.Separator
					 + appIdStr + Path.Separator + containerGlob + Path.Separator + TaskLog.LogName.
					Syslog)))
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(localFs.Open(fileStatus
						.GetPath())));
					string line;
					while ((line = br.ReadLine()) != null)
					{
						Matcher m = taskPattern.Matcher(line);
						if (m.Matches())
						{
							// found Task done message
							taLogDirs[TaskAttemptID.ForName(m.Group(1))] = fileStatus.GetPath().GetParent();
							break;
						}
					}
					br.Close();
				}
			}
			NUnit.Framework.Assert.AreEqual(4, taLogDirs.Count);
			// all 4 attempts found
			foreach (KeyValuePair<TaskAttemptID, Path> dirEntry in taLogDirs)
			{
				TaskAttemptID tid = dirEntry.Key;
				Path profilePath = new Path(dirEntry.Value, TaskLog.LogName.Profile.ToString());
				Path stdoutPath = new Path(dirEntry.Value, TaskLog.LogName.Stdout.ToString());
				if (useDefault || tid.GetTaskType() == TaskType.Map)
				{
					if (tid.GetTaskID().GetId() == ProfiledTaskId)
					{
						// verify profile.out
						BufferedReader br = new BufferedReader(new InputStreamReader(localFs.Open(profilePath
							)));
						string line = br.ReadLine();
						NUnit.Framework.Assert.IsTrue("No hprof content found!", line != null && line.StartsWith
							("JAVA PROFILE"));
						br.Close();
						NUnit.Framework.Assert.AreEqual(0L, localFs.GetFileStatus(stdoutPath).GetLen());
					}
					else
					{
						NUnit.Framework.Assert.IsFalse("hprof file should not exist", localFs.Exists(profilePath
							));
					}
				}
				else
				{
					NUnit.Framework.Assert.IsFalse("hprof file should not exist", localFs.Exists(profilePath
						));
					if (tid.GetTaskID().GetId() == ProfiledTaskId)
					{
						// reducer is profiled with Xprof
						BufferedReader br = new BufferedReader(new InputStreamReader(localFs.Open(stdoutPath
							)));
						bool flatProfFound = false;
						string line;
						while ((line = br.ReadLine()) != null)
						{
							if (line.StartsWith("Flat profile"))
							{
								flatProfFound = true;
								break;
							}
						}
						br.Close();
						NUnit.Framework.Assert.IsTrue("Xprof flat profile not found!", flatProfFound);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(0L, localFs.GetFileStatus(stdoutPath).GetLen());
					}
				}
			}
		}
	}
}
