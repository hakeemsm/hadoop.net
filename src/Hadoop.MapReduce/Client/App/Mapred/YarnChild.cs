using System;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>The main() for MapReduce task processes.</summary>
	internal class YarnChild
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(YarnChild));

		internal static volatile TaskAttemptID taskid = null;

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			Log.Debug("Child starting");
			JobConf job = new JobConf(MRJobConfig.JobConfFile);
			// Initing with our JobConf allows us to avoid loading confs twice
			Limits.Init(job);
			UserGroupInformation.SetConfiguration(job);
			string host = args[0];
			int port = System.Convert.ToInt32(args[1]);
			IPEndPoint address = NetUtils.CreateSocketAddrForHost(host, port);
			TaskAttemptID firstTaskid = ((TaskAttemptID)TaskAttemptID.ForName(args[2]));
			long jvmIdLong = long.Parse(args[3]);
			JVMId jvmId = new JVMId(((JobID)firstTaskid.GetJobID()), firstTaskid.GetTaskType(
				) == TaskType.Map, jvmIdLong);
			// initialize metrics
			DefaultMetricsSystem.Initialize(StringUtils.Camelize(firstTaskid.GetTaskType().ToString
				()) + "Task");
			// Security framework already loaded the tokens into current ugi
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			Log.Info("Executing with tokens:");
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in credentials.GetAllTokens
				())
			{
				Log.Info(token);
			}
			// Create TaskUmbilicalProtocol as actual task owner.
			UserGroupInformation taskOwner = UserGroupInformation.CreateRemoteUser(((JobID)firstTaskid
				.GetJobID()).ToString());
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = TokenCache.GetJobToken
				(credentials);
			SecurityUtil.SetTokenService(jt, address);
			taskOwner.AddToken(jt);
			TaskUmbilicalProtocol umbilical = taskOwner.DoAs(new _PrivilegedExceptionAction_108
				(address, job));
			// report non-pid to application master
			JvmContext context = new JvmContext(jvmId, "-1000");
			Log.Debug("PID: " + Sharpen.Runtime.GetEnv()["JVM_PID"]);
			Task task = null;
			UserGroupInformation childUGI = null;
			ScheduledExecutorService logSyncer = null;
			try
			{
				int idleLoopCount = 0;
				JvmTask myTask = null;
				// poll for new task
				for (int idle = 0; null == myTask; ++idle)
				{
					long sleepTimeMilliSecs = Math.Min(idle * 500, 1500);
					Log.Info("Sleeping for " + sleepTimeMilliSecs + "ms before retrying again. Got null now."
						);
					TimeUnit.Milliseconds.Sleep(sleepTimeMilliSecs);
					myTask = umbilical.GetTask(context);
				}
				if (myTask.ShouldDie())
				{
					return;
				}
				task = myTask.GetTask();
				YarnChild.taskid = task.GetTaskID();
				// Create the job-conf and set credentials
				ConfigureTask(job, task, credentials, jt);
				// Initiate Java VM metrics
				JvmMetrics.InitSingleton(jvmId.ToString(), job.GetSessionId());
				childUGI = UserGroupInformation.CreateRemoteUser(Runtime.Getenv(ApplicationConstants.Environment
					.User.ToString()));
				// Add tokens to new user so that it may execute its task correctly.
				childUGI.AddCredentials(credentials);
				// set job classloader if configured before invoking the task
				MRApps.SetJobClassLoader(job);
				logSyncer = TaskLog.CreateLogSyncer();
				// Create a final reference to the task for the doAs block
				Task taskFinal = task;
				childUGI.DoAs(new _PrivilegedExceptionAction_158(taskFinal, job, umbilical));
			}
			catch (FSError e)
			{
				// use job-specified working directory
				// run the task
				Log.Fatal("FSError from child", e);
				if (!ShutdownHookManager.Get().IsShutdownInProgress())
				{
					umbilical.FsError(taskid, e.Message);
				}
			}
			catch (Exception exception)
			{
				Log.Warn("Exception running child : " + StringUtils.StringifyException(exception)
					);
				try
				{
					if (task != null)
					{
						// do cleanup for the task
						if (childUGI == null)
						{
							// no need to job into doAs block
							task.TaskCleanup(umbilical);
						}
						else
						{
							Task taskFinal = task;
							childUGI.DoAs(new _PrivilegedExceptionAction_183(taskFinal, umbilical));
						}
					}
				}
				catch (Exception e)
				{
					Log.Info("Exception cleaning up: " + StringUtils.StringifyException(e));
				}
				// Report back any failures, for diagnostic purposes
				if (taskid != null)
				{
					if (!ShutdownHookManager.Get().IsShutdownInProgress())
					{
						umbilical.FatalError(taskid, StringUtils.StringifyException(exception));
					}
				}
			}
			catch (Exception throwable)
			{
				Log.Fatal("Error running child : " + StringUtils.StringifyException(throwable));
				if (taskid != null)
				{
					if (!ShutdownHookManager.Get().IsShutdownInProgress())
					{
						Exception tCause = throwable.InnerException;
						string cause = tCause == null ? throwable.Message : StringUtils.StringifyException
							(tCause);
						umbilical.FatalError(taskid, cause);
					}
				}
			}
			finally
			{
				RPC.StopProxy(umbilical);
				DefaultMetricsSystem.Shutdown();
				TaskLog.SyncLogsShutdown(logSyncer);
			}
		}

		private sealed class _PrivilegedExceptionAction_108 : PrivilegedExceptionAction<TaskUmbilicalProtocol
			>
		{
			public _PrivilegedExceptionAction_108(IPEndPoint address, JobConf job)
			{
				this.address = address;
				this.job = job;
			}

			/// <exception cref="System.Exception"/>
			public TaskUmbilicalProtocol Run()
			{
				return (TaskUmbilicalProtocol)RPC.GetProxy<TaskUmbilicalProtocol>(TaskUmbilicalProtocol
					.versionID, address, job);
			}

			private readonly IPEndPoint address;

			private readonly JobConf job;
		}

		private sealed class _PrivilegedExceptionAction_158 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_158(Task taskFinal, JobConf job, TaskUmbilicalProtocol
				 umbilical)
			{
				this.taskFinal = taskFinal;
				this.job = job;
				this.umbilical = umbilical;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				YarnChild.SetEncryptedSpillKeyIfRequired(taskFinal);
				FileSystem.Get(job).SetWorkingDirectory(job.GetWorkingDirectory());
				taskFinal.Run(job, umbilical);
				return null;
			}

			private readonly Task taskFinal;

			private readonly JobConf job;

			private readonly TaskUmbilicalProtocol umbilical;
		}

		private sealed class _PrivilegedExceptionAction_183 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_183(Task taskFinal, TaskUmbilicalProtocol umbilical
				)
			{
				this.taskFinal = taskFinal;
				this.umbilical = umbilical;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				taskFinal.TaskCleanup(umbilical);
				return null;
			}

			private readonly Task taskFinal;

			private readonly TaskUmbilicalProtocol umbilical;
		}

		/// <summary>
		/// Utility method to check if the Encrypted Spill Key needs to be set into the
		/// user credentials of the user running the Map / Reduce Task
		/// </summary>
		/// <param name="task">The Map / Reduce task to set the Encrypted Spill information in
		/// 	</param>
		/// <exception cref="System.Exception"/>
		public static void SetEncryptedSpillKeyIfRequired(Task task)
		{
			if ((task != null) && (task.GetEncryptedSpillKey() != null) && (task.GetEncryptedSpillKey
				().Length > 1))
			{
				Credentials creds = UserGroupInformation.GetCurrentUser().GetCredentials();
				TokenCache.SetEncryptedSpillKey(task.GetEncryptedSpillKey(), creds);
				UserGroupInformation.GetCurrentUser().AddCredentials(creds);
			}
		}

		/// <summary>Configure mapred-local dirs.</summary>
		/// <remarks>
		/// Configure mapred-local dirs. This config is used by the task for finding
		/// out an output directory.
		/// </remarks>
		/// <exception cref="System.IO.IOException"></exception>
		private static void ConfigureLocalDirs(Task task, JobConf job)
		{
			string[] localSysDirs = StringUtils.GetTrimmedStrings(Runtime.Getenv(ApplicationConstants.Environment
				.LocalDirs.ToString()));
			job.SetStrings(MRConfig.LocalDir, localSysDirs);
			Log.Info(MRConfig.LocalDir + " for child: " + job.Get(MRConfig.LocalDir));
			LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir);
			Path workDir = null;
			// First, try to find the JOB_LOCAL_DIR on this host.
			try
			{
				workDir = lDirAlloc.GetLocalPathToRead("work", job);
			}
			catch (DiskChecker.DiskErrorException)
			{
			}
			// DiskErrorException means dir not found. If not found, it will
			// be created below.
			if (workDir == null)
			{
				// JOB_LOCAL_DIR doesn't exist on this host -- Create it.
				workDir = lDirAlloc.GetLocalPathForWrite("work", job);
				FileSystem lfs = FileSystem.GetLocal(job).GetRaw();
				bool madeDir = false;
				try
				{
					madeDir = lfs.Mkdirs(workDir);
				}
				catch (FileAlreadyExistsException)
				{
					// Since all tasks will be running in their own JVM, the race condition
					// exists where multiple tasks could be trying to create this directory
					// at the same time. If this task loses the race, it's okay because
					// the directory already exists.
					madeDir = true;
					workDir = lDirAlloc.GetLocalPathToRead("work", job);
				}
				if (!madeDir)
				{
					throw new IOException("Mkdirs failed to create " + workDir.ToString());
				}
			}
			job.Set(MRJobConfig.JobLocalDir, workDir.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ConfigureTask(JobConf job, Task task, Credentials credentials
			, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt)
		{
			job.SetCredentials(credentials);
			ApplicationAttemptId appAttemptId = ConverterUtils.ToContainerId(Runtime.Getenv(ApplicationConstants.Environment
				.ContainerId.ToString())).GetApplicationAttemptId();
			Log.Debug("APPLICATION_ATTEMPT_ID: " + appAttemptId);
			// Set it in conf, so as to be able to be used the the OutputCommitter.
			job.SetInt(MRJobConfig.ApplicationAttemptId, appAttemptId.GetAttemptId());
			// set tcp nodelay
			job.SetBoolean("ipc.client.tcpnodelay", true);
			job.SetClass(MRConfig.TaskLocalOutputClass, typeof(YarnOutputFiles), typeof(MapOutputFile
				));
			// set the jobToken and shuffle secrets into task
			task.SetJobTokenSecret(JobTokenSecretManager.CreateSecretKey(jt.GetPassword()));
			byte[] shuffleSecret = TokenCache.GetShuffleSecretKey(credentials);
			if (shuffleSecret == null)
			{
				Log.Warn("Shuffle secret missing from task credentials." + " Using job token secret as shuffle secret."
					);
				shuffleSecret = jt.GetPassword();
			}
			task.SetShuffleSecret(JobTokenSecretManager.CreateSecretKey(shuffleSecret));
			// setup the child's MRConfig.LOCAL_DIR.
			ConfigureLocalDirs(task, job);
			// setup the child's attempt directories
			// Do the task-type specific localization
			task.LocalizeConfiguration(job);
			// Set up the DistributedCache related configs
			MRApps.SetupDistributedCacheLocal(job);
			// Overwrite the localized task jobconf which is linked to in the current
			// work-dir.
			Path localTaskFile = new Path(MRJobConfig.JobConfFile);
			WriteLocalJobFile(localTaskFile, job);
			task.SetJobFile(localTaskFile.ToString());
			task.SetConf(job);
		}

		private static readonly FsPermission urw_gr = FsPermission.CreateImmutable((short
			)0x1a0);

		/// <summary>Write the task specific job-configuration file.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteLocalJobFile(Path jobFile, JobConf conf)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			localFs.Delete(jobFile);
			OutputStream @out = null;
			try
			{
				@out = FileSystem.Create(localFs, jobFile, urw_gr);
				conf.WriteXml(@out);
			}
			finally
			{
				IOUtils.Cleanup(Log, @out);
			}
		}
	}
}
