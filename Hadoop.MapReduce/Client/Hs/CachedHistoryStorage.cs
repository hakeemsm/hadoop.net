using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>Manages an in memory cache of parsed Job History files.</summary>
	public class CachedHistoryStorage : AbstractService, HistoryStorage
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.CachedHistoryStorage
			));

		private IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> loadedJobCache
			 = null;

		private int loadedJobCacheSize;

		private HistoryFileManager hsManager;

		// The number of loaded jobs.
		public virtual void SetHistoryFileManager(HistoryFileManager hsManager)
		{
			this.hsManager = hsManager;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			Log.Info("CachedHistoryStorage Init");
			CreateLoadedJobCache(conf);
		}

		private void CreateLoadedJobCache(Configuration conf)
		{
			loadedJobCacheSize = conf.GetInt(JHAdminConfig.MrHistoryLoadedJobCacheSize, JHAdminConfig
				.DefaultMrHistoryLoadedJobCacheSize);
			loadedJobCache = Sharpen.Collections.SynchronizedMap(new _LinkedHashMap_78(this, 
				loadedJobCacheSize + 1, 0.75f, true));
		}

		private sealed class _LinkedHashMap_78 : LinkedHashMap<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			>
		{
			public _LinkedHashMap_78(CachedHistoryStorage _enclosing, int baseArg1, float baseArg2
				, bool baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this._enclosing = _enclosing;
			}

			protected override bool RemoveEldestEntry(KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				> eldest)
			{
				return base.Count > this._enclosing.loadedJobCacheSize;
			}

			private readonly CachedHistoryStorage _enclosing;
		}

		public virtual void RefreshLoadedJobCache()
		{
			if (GetServiceState() == Service.STATE.Started)
			{
				SetConfig(CreateConf());
				CreateLoadedJobCache(GetConfig());
			}
			else
			{
				Log.Warn("Failed to execute refreshLoadedJobCache: CachedHistoryStorage is not started"
					);
			}
		}

		[VisibleForTesting]
		internal virtual Configuration CreateConf()
		{
			return new Configuration();
		}

		public CachedHistoryStorage()
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.CachedHistoryStorage).FullName)
		{
		}

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job LoadJob(HistoryFileManager.HistoryFileInfo
			 fileInfo)
		{
			try
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = fileInfo.LoadJob();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Adding " + job.GetID() + " to loaded job cache");
				}
				// We can clobber results here, but that should be OK, because it only
				// means that we may have two identical copies of the same job floating
				// around for a while.
				loadedJobCache[job.GetID()] = job;
				return job;
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Could not find/load job: " + fileInfo.GetJobId(), 
					e);
			}
		}

		[VisibleForTesting]
		internal virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetLoadedJobCache
			()
		{
			return loadedJobCache;
		}

		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetFullJob(JobId jobId)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Looking for Job " + jobId);
			}
			try
			{
				HistoryFileManager.HistoryFileInfo fileInfo = hsManager.GetFileInfo(jobId);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job result = null;
				if (fileInfo != null)
				{
					result = loadedJobCache[jobId];
					if (result == null)
					{
						result = LoadJob(fileInfo);
					}
					else
					{
						if (fileInfo.IsDeleted())
						{
							Sharpen.Collections.Remove(loadedJobCache, jobId);
							result = null;
						}
					}
				}
				else
				{
					Sharpen.Collections.Remove(loadedJobCache, jobId);
				}
				return result;
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllPartialJobs
			()
		{
			Log.Debug("Called getAllPartialJobs()");
			SortedDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> result = new 
				SortedDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
			try
			{
				foreach (HistoryFileManager.HistoryFileInfo mi in hsManager.GetAllFileInfo())
				{
					if (mi != null)
					{
						JobId id = mi.GetJobId();
						result[id] = new PartialJob(mi.GetJobIndexInfo(), id);
					}
				}
			}
			catch (IOException e)
			{
				Log.Warn("Error trying to scan for all FileInfos", e);
				throw new YarnRuntimeException(e);
			}
			return result;
		}

		public virtual JobsInfo GetPartialJobs(long offset, long count, string user, string
			 queue, long sBegin, long sEnd, long fBegin, long fEnd, JobState jobState)
		{
			return GetPartialJobs(GetAllPartialJobs().Values, offset, count, user, queue, sBegin
				, sEnd, fBegin, fEnd, jobState);
		}

		public static JobsInfo GetPartialJobs(ICollection<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			> jobs, long offset, long count, string user, string queue, long sBegin, long sEnd
			, long fBegin, long fEnd, JobState jobState)
		{
			JobsInfo allJobs = new JobsInfo();
			if (sBegin == null || sBegin < 0)
			{
				sBegin = 0l;
			}
			if (sEnd == null)
			{
				sEnd = long.MaxValue;
			}
			if (fBegin == null || fBegin < 0)
			{
				fBegin = 0l;
			}
			if (fEnd == null)
			{
				fEnd = long.MaxValue;
			}
			if (offset == null || offset < 0)
			{
				offset = 0l;
			}
			if (count == null)
			{
				count = long.MaxValue;
			}
			if (offset > jobs.Count)
			{
				return allJobs;
			}
			long at = 0;
			long end = offset + count - 1;
			if (end < 0)
			{
				// due to overflow
				end = long.MaxValue;
			}
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job in jobs)
			{
				if (at > end)
				{
					break;
				}
				// can't really validate queue is a valid one since queues could change
				if (queue != null && !queue.IsEmpty())
				{
					if (!job.GetQueueName().Equals(queue))
					{
						continue;
					}
				}
				if (user != null && !user.IsEmpty())
				{
					if (!job.GetUserName().Equals(user))
					{
						continue;
					}
				}
				JobReport report = job.GetReport();
				if (report.GetStartTime() < sBegin || report.GetStartTime() > sEnd)
				{
					continue;
				}
				if (report.GetFinishTime() < fBegin || report.GetFinishTime() > fEnd)
				{
					continue;
				}
				if (jobState != null && jobState != report.GetJobState())
				{
					continue;
				}
				at++;
				if ((at - 1) < offset)
				{
					continue;
				}
				JobInfo jobInfo = new JobInfo(job);
				allJobs.Add(jobInfo);
			}
			return allJobs;
		}
	}
}
