using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>Loads and manages the Job history cache.</summary>
	public class JobHistory : AbstractService, HistoryContext
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.JobHistory
			));

		public static readonly Sharpen.Pattern ConfFilenameRegex = Sharpen.Pattern.Compile
			("(" + JobID.JobidRegex + ")_conf.xml(?:\\.[0-9]+\\.old)?");

		public const string OldSuffix = ".old";

		private long moveThreadInterval;

		private Configuration conf;

		private ScheduledThreadPoolExecutor scheduledExecutor = null;

		private HistoryStorage storage = null;

		private HistoryFileManager hsManager = null;

		internal ScheduledFuture<object> futureHistoryCleaner = null;

		private long cleanerInterval;

		// Time interval for the move thread.
		//History job cleaner interval
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Log.Info("JobHistory Init");
			this.conf = conf;
			this.appID = ApplicationId.NewInstance(0, 0);
			this.appAttemptID = RecordFactoryProvider.GetRecordFactory(conf).NewRecordInstance
				<ApplicationAttemptId>();
			moveThreadInterval = conf.GetLong(JHAdminConfig.MrHistoryMoveIntervalMs, JHAdminConfig
				.DefaultMrHistoryMoveIntervalMs);
			hsManager = CreateHistoryFileManager();
			hsManager.Init(conf);
			try
			{
				hsManager.InitExisting();
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Failed to intialize existing directories", e);
			}
			storage = CreateHistoryStorage();
			if (storage is Org.Apache.Hadoop.Service.Service)
			{
				((Org.Apache.Hadoop.Service.Service)storage).Init(conf);
			}
			storage.SetHistoryFileManager(hsManager);
			base.ServiceInit(conf);
		}

		protected internal virtual HistoryStorage CreateHistoryStorage()
		{
			return ReflectionUtils.NewInstance(conf.GetClass<HistoryStorage>(JHAdminConfig.MrHistoryStorage
				, typeof(CachedHistoryStorage)), conf);
		}

		protected internal virtual HistoryFileManager CreateHistoryFileManager()
		{
			return new HistoryFileManager();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			hsManager.Start();
			if (storage is Org.Apache.Hadoop.Service.Service)
			{
				((Org.Apache.Hadoop.Service.Service)storage).Start();
			}
			scheduledExecutor = new ScheduledThreadPoolExecutor(2, new ThreadFactoryBuilder()
				.SetNameFormat("Log Scanner/Cleaner #%d").Build());
			scheduledExecutor.ScheduleAtFixedRate(new JobHistory.MoveIntermediateToDoneRunnable
				(this), moveThreadInterval, moveThreadInterval, TimeUnit.Milliseconds);
			// Start historyCleaner
			ScheduleHistoryCleaner();
			base.ServiceStart();
		}

		protected internal virtual int GetInitDelaySecs()
		{
			return 30;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info("Stopping JobHistory");
			if (scheduledExecutor != null)
			{
				Log.Info("Stopping History Cleaner/Move To Done");
				scheduledExecutor.Shutdown();
				bool interrupted = false;
				long currentTime = Runtime.CurrentTimeMillis();
				while (!scheduledExecutor.IsShutdown() && Runtime.CurrentTimeMillis() > currentTime
					 + 1000l && !interrupted)
				{
					try
					{
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
						interrupted = true;
					}
				}
				if (!scheduledExecutor.IsShutdown())
				{
					Log.Warn("HistoryCleanerService/move to done shutdown may not have " + "succeeded, Forcing a shutdown"
						);
					scheduledExecutor.ShutdownNow();
				}
			}
			if (storage != null && storage is Org.Apache.Hadoop.Service.Service)
			{
				((Org.Apache.Hadoop.Service.Service)storage).Stop();
			}
			if (hsManager != null)
			{
				hsManager.Stop();
			}
			base.ServiceStop();
		}

		public JobHistory()
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.JobHistory).FullName)
		{
		}

		public virtual string GetApplicationName()
		{
			return "Job History Server";
		}

		private class MoveIntermediateToDoneRunnable : Runnable
		{
			public virtual void Run()
			{
				try
				{
					JobHistory.Log.Info("Starting scan to move intermediate done files");
					this._enclosing.hsManager.ScanIntermediateDirectory();
				}
				catch (IOException e)
				{
					JobHistory.Log.Error("Error while scanning intermediate done dir ", e);
				}
			}

			internal MoveIntermediateToDoneRunnable(JobHistory _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JobHistory _enclosing;
		}

		private class HistoryCleaner : Runnable
		{
			public virtual void Run()
			{
				JobHistory.Log.Info("History Cleaner started");
				try
				{
					this._enclosing.hsManager.Clean();
				}
				catch (IOException e)
				{
					JobHistory.Log.Warn("Error trying to clean up ", e);
				}
				JobHistory.Log.Info("History Cleaner complete");
			}

			internal HistoryCleaner(JobHistory _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JobHistory _enclosing;
		}

		/// <summary>Helper method for test cases.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual HistoryFileManager.HistoryFileInfo GetJobFileInfo(JobId jobId)
		{
			return hsManager.GetFileInfo(jobId);
		}

		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobId)
		{
			return storage.GetFullJob(jobId);
		}

		public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
			(ApplicationId appID)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Called getAllJobs(AppId): " + appID);
			}
			// currently there is 1 to 1 mapping between app and job id
			JobID oldJobID = TypeConverter.FromYarn(appID);
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs = new Dictionary
				<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
			JobId jobID = TypeConverter.ToYarn(oldJobID);
			jobs[jobID] = GetJob(jobID);
			return jobs;
		}

		public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
			()
		{
			return storage.GetAllPartialJobs();
		}

		public virtual void RefreshLoadedJobCache()
		{
			if (GetServiceState() == Service.STATE.Started)
			{
				if (storage is CachedHistoryStorage)
				{
					((CachedHistoryStorage)storage).RefreshLoadedJobCache();
				}
				else
				{
					throw new NotSupportedException(storage.GetType().FullName + " is expected to be an instance of "
						 + typeof(CachedHistoryStorage).FullName);
				}
			}
			else
			{
				Log.Warn("Failed to execute refreshLoadedJobCache: JobHistory service is not started"
					);
			}
		}

		[VisibleForTesting]
		internal virtual HistoryStorage GetHistoryStorage()
		{
			return storage;
		}

		/// <summary>Look for a set of partial jobs.</summary>
		/// <param name="offset">the offset into the list of jobs.</param>
		/// <param name="count">the maximum number of jobs to return.</param>
		/// <param name="user">only return jobs for the given user.</param>
		/// <param name="queue">only return jobs for in the given queue.</param>
		/// <param name="sBegin">only return Jobs that started on or after the given time.</param>
		/// <param name="sEnd">only return Jobs that started on or before the given time.</param>
		/// <param name="fBegin">only return Jobs that ended on or after the given time.</param>
		/// <param name="fEnd">only return Jobs that ended on or before the given time.</param>
		/// <param name="jobState">only return jobs that are in the give job state.</param>
		/// <returns>The list of filtered jobs.</returns>
		public virtual JobsInfo GetPartialJobs(long offset, long count, string user, string
			 queue, long sBegin, long sEnd, long fBegin, long fEnd, JobState jobState)
		{
			return storage.GetPartialJobs(offset, count, user, queue, sBegin, sEnd, fBegin, fEnd
				, jobState);
		}

		public virtual void RefreshJobRetentionSettings()
		{
			if (GetServiceState() == Service.STATE.Started)
			{
				conf = CreateConf();
				long maxHistoryAge = conf.GetLong(JHAdminConfig.MrHistoryMaxAgeMs, JHAdminConfig.
					DefaultMrHistoryMaxAge);
				hsManager.SetMaxHistoryAge(maxHistoryAge);
				if (futureHistoryCleaner != null)
				{
					futureHistoryCleaner.Cancel(false);
				}
				futureHistoryCleaner = null;
				ScheduleHistoryCleaner();
			}
			else
			{
				Log.Warn("Failed to execute refreshJobRetentionSettings : Job History service is not started"
					);
			}
		}

		private void ScheduleHistoryCleaner()
		{
			bool startCleanerService = conf.GetBoolean(JHAdminConfig.MrHistoryCleanerEnable, 
				true);
			if (startCleanerService)
			{
				cleanerInterval = conf.GetLong(JHAdminConfig.MrHistoryCleanerIntervalMs, JHAdminConfig
					.DefaultMrHistoryCleanerIntervalMs);
				futureHistoryCleaner = scheduledExecutor.ScheduleAtFixedRate(new JobHistory.HistoryCleaner
					(this), GetInitDelaySecs() * 1000l, cleanerInterval, TimeUnit.Milliseconds);
			}
		}

		protected internal virtual Configuration CreateConf()
		{
			return new Configuration();
		}

		public virtual long GetCleanerInterval()
		{
			return cleanerInterval;
		}

		private ApplicationAttemptId appAttemptID;

		// TODO AppContext - Not Required
		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			// TODO fixme - bogus appAttemptID for now
			return appAttemptID;
		}

		private ApplicationId appID;

		// TODO AppContext - Not Required
		public virtual ApplicationId GetApplicationID()
		{
			// TODO fixme - bogus appID for now
			return appID;
		}

		// TODO AppContext - Not Required
		public virtual EventHandler GetEventHandler()
		{
			// TODO Auto-generated method stub
			return null;
		}

		private string userName;

		// TODO AppContext - Not Required
		public virtual CharSequence GetUser()
		{
			if (userName != null)
			{
				userName = conf.Get(MRJobConfig.UserName, "history-user");
			}
			return userName;
		}

		// TODO AppContext - Not Required
		public virtual Clock GetClock()
		{
			return null;
		}

		// TODO AppContext - Not Required
		public virtual ClusterInfo GetClusterInfo()
		{
			return null;
		}

		// TODO AppContext - Not Required
		public virtual ICollection<string> GetBlacklistedNodes()
		{
			// Not Implemented
			return null;
		}

		public virtual ClientToAMTokenSecretManager GetClientToAMTokenSecretManager()
		{
			// Not implemented.
			return null;
		}

		public virtual bool IsLastAMRetry()
		{
			// bogus - Not Required
			return false;
		}

		public virtual bool HasSuccessfullyUnregistered()
		{
			// bogus - Not Required
			return true;
		}

		public virtual string GetNMHostname()
		{
			// bogus - Not Required
			return null;
		}
	}
}
