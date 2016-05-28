using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>Loads the basic job level data upfront.</summary>
	/// <remarks>
	/// Loads the basic job level data upfront.
	/// Data from job history file is loaded lazily.
	/// </remarks>
	public class CompletedJob : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.CompletedJob
			));

		private readonly Configuration conf;

		private readonly JobId jobId;

		private readonly string user;

		private readonly HistoryFileManager.HistoryFileInfo info;

		private JobHistoryParser.JobInfo jobInfo;

		private JobReport report;

		internal AtomicBoolean tasksLoaded = new AtomicBoolean(false);

		private Lock tasksLock = new ReentrantLock();

		private IDictionary<TaskId, Task> tasks = new Dictionary<TaskId, Task>();

		private IDictionary<TaskId, Task> mapTasks = new Dictionary<TaskId, Task>();

		private IDictionary<TaskId, Task> reduceTasks = new Dictionary<TaskId, Task>();

		private IList<TaskAttemptCompletionEvent> completionEvents = null;

		private IList<TaskAttemptCompletionEvent> mapCompletionEvents = null;

		private JobACLsManager aclsMgr;

		/// <exception cref="System.IO.IOException"/>
		public CompletedJob(Configuration conf, JobId jobId, Path historyFile, bool loadTasks
			, string userName, HistoryFileManager.HistoryFileInfo info, JobACLsManager aclsMgr
			)
		{
			//Can be picked from JobInfo with a conversion.
			//Can be picked up from JobInfo
			Log.Info("Loading job: " + jobId + " from file: " + historyFile);
			this.conf = conf;
			this.jobId = jobId;
			this.user = userName;
			this.info = info;
			this.aclsMgr = aclsMgr;
			LoadFullHistoryData(loadTasks, historyFile);
		}

		public virtual int GetCompletedMaps()
		{
			return (int)jobInfo.GetFinishedMaps();
		}

		public virtual int GetCompletedReduces()
		{
			return (int)jobInfo.GetFinishedReduces();
		}

		public virtual Counters GetAllCounters()
		{
			return jobInfo.GetTotalCounters();
		}

		public virtual JobId GetID()
		{
			return jobId;
		}

		public virtual JobReport GetReport()
		{
			lock (this)
			{
				if (report == null)
				{
					ConstructJobReport();
				}
				return report;
			}
		}

		private void ConstructJobReport()
		{
			report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			report.SetJobId(jobId);
			report.SetJobState(JobState.ValueOf(jobInfo.GetJobStatus()));
			report.SetSubmitTime(jobInfo.GetSubmitTime());
			report.SetStartTime(jobInfo.GetLaunchTime());
			report.SetFinishTime(jobInfo.GetFinishTime());
			report.SetJobName(jobInfo.GetJobname());
			report.SetUser(jobInfo.GetUsername());
			if (GetTotalMaps() == 0)
			{
				report.SetMapProgress(1.0f);
			}
			else
			{
				report.SetMapProgress((float)GetCompletedMaps() / GetTotalMaps());
			}
			if (GetTotalReduces() == 0)
			{
				report.SetReduceProgress(1.0f);
			}
			else
			{
				report.SetReduceProgress((float)GetCompletedReduces() / GetTotalReduces());
			}
			report.SetJobFile(GetConfFile().ToString());
			string historyUrl = "N/A";
			try
			{
				historyUrl = MRWebAppUtil.GetApplicationWebURLOnJHSWithoutScheme(conf, jobId.GetAppId
					());
			}
			catch (UnknownHostException)
			{
			}
			//Ignore.
			report.SetTrackingUrl(historyUrl);
			report.SetAMInfos(GetAMInfos());
			report.SetIsUber(IsUber());
		}

		public virtual float GetProgress()
		{
			return 1.0f;
		}

		public virtual JobState GetState()
		{
			return JobState.ValueOf(jobInfo.GetJobStatus());
		}

		public virtual Task GetTask(TaskId taskId)
		{
			if (tasksLoaded.Get())
			{
				return tasks[taskId];
			}
			else
			{
				TaskID oldTaskId = TypeConverter.FromYarn(taskId);
				CompletedTask completedTask = new CompletedTask(taskId, jobInfo.GetAllTasks()[oldTaskId
					]);
				return completedTask;
			}
		}

		public virtual TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
			, int maxEvents)
		{
			lock (this)
			{
				if (completionEvents == null)
				{
					ConstructTaskAttemptCompletionEvents();
				}
				return GetAttemptCompletionEvents(completionEvents, fromEventId, maxEvents);
			}
		}

		public virtual TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
			, int maxEvents)
		{
			lock (this)
			{
				if (mapCompletionEvents == null)
				{
					ConstructTaskAttemptCompletionEvents();
				}
				return TypeConverter.FromYarn(GetAttemptCompletionEvents(mapCompletionEvents, startIndex
					, maxEvents));
			}
		}

		private static TaskAttemptCompletionEvent[] GetAttemptCompletionEvents(IList<TaskAttemptCompletionEvent
			> eventList, int startIndex, int maxEvents)
		{
			TaskAttemptCompletionEvent[] events = new TaskAttemptCompletionEvent[0];
			if (eventList.Count > startIndex)
			{
				int actualMax = Math.Min(maxEvents, (eventList.Count - startIndex));
				events = Sharpen.Collections.ToArray(eventList.SubList(startIndex, actualMax + startIndex
					), events);
			}
			return events;
		}

		private void ConstructTaskAttemptCompletionEvents()
		{
			LoadAllTasks();
			completionEvents = new List<TaskAttemptCompletionEvent>();
			IList<TaskAttempt> allTaskAttempts = new List<TaskAttempt>();
			int numMapAttempts = 0;
			foreach (KeyValuePair<TaskId, Task> taskEntry in tasks)
			{
				Task task = taskEntry.Value;
				foreach (KeyValuePair<TaskAttemptId, TaskAttempt> taskAttemptEntry in task.GetAttempts
					())
				{
					TaskAttempt taskAttempt = taskAttemptEntry.Value;
					allTaskAttempts.AddItem(taskAttempt);
					if (task.GetType() == TaskType.Map)
					{
						++numMapAttempts;
					}
				}
			}
			allTaskAttempts.Sort(new _IComparer_237());
			mapCompletionEvents = new AList<TaskAttemptCompletionEvent>(numMapAttempts);
			int eventId = 0;
			foreach (TaskAttempt taskAttempt_1 in allTaskAttempts)
			{
				TaskAttemptCompletionEvent tace = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskAttemptCompletionEvent
					>();
				int attemptRunTime = -1;
				if (taskAttempt_1.GetLaunchTime() != 0 && taskAttempt_1.GetFinishTime() != 0)
				{
					attemptRunTime = (int)(taskAttempt_1.GetFinishTime() - taskAttempt_1.GetLaunchTime
						());
				}
				// Default to KILLED
				TaskAttemptCompletionEventStatus taceStatus = TaskAttemptCompletionEventStatus.Killed;
				string taStateString = taskAttempt_1.GetState().ToString();
				try
				{
					taceStatus = TaskAttemptCompletionEventStatus.ValueOf(taStateString);
				}
				catch (Exception)
				{
					Log.Warn("Cannot constuct TACEStatus from TaskAtemptState: [" + taStateString + "] for taskAttemptId: ["
						 + taskAttempt_1.GetID() + "]. Defaulting to KILLED");
				}
				tace.SetAttemptId(taskAttempt_1.GetID());
				tace.SetAttemptRunTime(attemptRunTime);
				tace.SetEventId(eventId++);
				tace.SetMapOutputServerAddress(taskAttempt_1.GetAssignedContainerMgrAddress());
				tace.SetStatus(taceStatus);
				completionEvents.AddItem(tace);
				if (taskAttempt_1.GetID().GetTaskId().GetTaskType() == TaskType.Map)
				{
					mapCompletionEvents.AddItem(tace);
				}
			}
		}

		private sealed class _IComparer_237 : IComparer<TaskAttempt>
		{
			public _IComparer_237()
			{
			}

			public int Compare(TaskAttempt o1, TaskAttempt o2)
			{
				if (o1.GetFinishTime() == 0 || o2.GetFinishTime() == 0)
				{
					if (o1.GetFinishTime() == 0 && o2.GetFinishTime() == 0)
					{
						if (o1.GetLaunchTime() == 0 || o2.GetLaunchTime() == 0)
						{
							if (o1.GetLaunchTime() == 0 && o2.GetLaunchTime() == 0)
							{
								return 0;
							}
							else
							{
								long res = o1.GetLaunchTime() - o2.GetLaunchTime();
								return res > 0 ? -1 : 1;
							}
						}
						else
						{
							return (int)(o1.GetLaunchTime() - o2.GetLaunchTime());
						}
					}
					else
					{
						long res = o1.GetFinishTime() - o2.GetFinishTime();
						return res > 0 ? -1 : 1;
					}
				}
				else
				{
					return (int)(o1.GetFinishTime() - o2.GetFinishTime());
				}
			}
		}

		public virtual IDictionary<TaskId, Task> GetTasks()
		{
			LoadAllTasks();
			return tasks;
		}

		private void LoadAllTasks()
		{
			if (tasksLoaded.Get())
			{
				return;
			}
			tasksLock.Lock();
			try
			{
				if (tasksLoaded.Get())
				{
					return;
				}
				foreach (KeyValuePair<TaskID, JobHistoryParser.TaskInfo> entry in jobInfo.GetAllTasks
					())
				{
					TaskId yarnTaskID = TypeConverter.ToYarn(entry.Key);
					JobHistoryParser.TaskInfo taskInfo = entry.Value;
					Task task = new CompletedTask(yarnTaskID, taskInfo);
					tasks[yarnTaskID] = task;
					if (task.GetType() == TaskType.Map)
					{
						mapTasks[task.GetID()] = task;
					}
					else
					{
						if (task.GetType() == TaskType.Reduce)
						{
							reduceTasks[task.GetID()] = task;
						}
					}
				}
				tasksLoaded.Set(true);
			}
			finally
			{
				tasksLock.Unlock();
			}
		}

		//History data is leisurely loaded when task level data is requested
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LoadFullHistoryData(bool loadTasks, Path historyFileAbsolute
			)
		{
			lock (this)
			{
				Log.Info("Loading history file: [" + historyFileAbsolute + "]");
				if (this.jobInfo != null)
				{
					return;
				}
				if (historyFileAbsolute != null)
				{
					JobHistoryParser parser = null;
					try
					{
						parser = new JobHistoryParser(historyFileAbsolute.GetFileSystem(conf), historyFileAbsolute
							);
						this.jobInfo = parser.Parse();
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException("Could not load history file " + historyFileAbsolute
							, e);
					}
					IOException parseException = parser.GetParseException();
					if (parseException != null)
					{
						throw new YarnRuntimeException("Could not parse history file " + historyFileAbsolute
							, parseException);
					}
				}
				else
				{
					throw new IOException("History file not found");
				}
				if (loadTasks)
				{
					LoadAllTasks();
					Log.Info("TaskInfo loaded");
				}
			}
		}

		public virtual IList<string> GetDiagnostics()
		{
			return Sharpen.Collections.SingletonList(jobInfo.GetErrorInfo());
		}

		public virtual string GetName()
		{
			return jobInfo.GetJobname();
		}

		public virtual string GetQueueName()
		{
			return jobInfo.GetJobQueueName();
		}

		public virtual int GetTotalMaps()
		{
			return (int)jobInfo.GetTotalMaps();
		}

		public virtual int GetTotalReduces()
		{
			return (int)jobInfo.GetTotalReduces();
		}

		public virtual bool IsUber()
		{
			return jobInfo.GetUberized();
		}

		public virtual IDictionary<TaskId, Task> GetTasks(TaskType taskType)
		{
			LoadAllTasks();
			if (TaskType.Map.Equals(taskType))
			{
				return mapTasks;
			}
			else
			{
				//we have only two types of tasks
				return reduceTasks;
			}
		}

		public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
			)
		{
			IDictionary<JobACL, AccessControlList> jobACLs = jobInfo.GetJobACLs();
			AccessControlList jobACL = jobACLs[jobOperation];
			if (jobACL == null)
			{
				return true;
			}
			return aclsMgr.CheckAccess(callerUGI, jobOperation, jobInfo.GetUsername(), jobACL
				);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.job.Job#getJobACLs()
		*/
		public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
		{
			return jobInfo.GetJobACLs();
		}

		public virtual string GetUserName()
		{
			return user;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.job.Job#getConfFile()
		*/
		public virtual Path GetConfFile()
		{
			return info.GetConfFile();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.job.Job#loadConfFile()
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual Configuration LoadConfFile()
		{
			return info.LoadConfFile();
		}

		public virtual IList<AMInfo> GetAMInfos()
		{
			IList<AMInfo> amInfos = new List<AMInfo>();
			foreach (JobHistoryParser.AMInfo jhAmInfo in jobInfo.GetAMInfos())
			{
				AMInfo amInfo = MRBuilderUtils.NewAMInfo(jhAmInfo.GetAppAttemptId(), jhAmInfo.GetStartTime
					(), jhAmInfo.GetContainerId(), jhAmInfo.GetNodeManagerHost(), jhAmInfo.GetNodeManagerPort
					(), jhAmInfo.GetNodeManagerHttpPort());
				amInfos.AddItem(amInfo);
			}
			return amInfos;
		}

		public virtual void SetQueueName(string queueName)
		{
			throw new NotSupportedException("Can't set job's queue name in history");
		}
	}
}
