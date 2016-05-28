using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Default Parser for the JobHistory files.</summary>
	/// <remarks>
	/// Default Parser for the JobHistory files. Typical usage is
	/// JobHistoryParser parser = new JobHistoryParser(fs, historyFile);
	/// job = parser.parse();
	/// </remarks>
	public class JobHistoryParser : HistoryEventHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryParser
			));

		private readonly FSDataInputStream @in;

		private JobHistoryParser.JobInfo info = null;

		private IOException parseException = null;

		/// <summary>
		/// Create a job history parser for the given history file using the
		/// given file system
		/// </summary>
		/// <param name="fs"/>
		/// <param name="file"/>
		/// <exception cref="System.IO.IOException"/>
		public JobHistoryParser(FileSystem fs, string file)
			: this(fs, new Path(file))
		{
		}

		/// <summary>
		/// Create the job history parser for the given history file using the
		/// given file system
		/// </summary>
		/// <param name="fs"/>
		/// <param name="historyFile"/>
		/// <exception cref="System.IO.IOException"/>
		public JobHistoryParser(FileSystem fs, Path historyFile)
			: this(fs.Open(historyFile))
		{
		}

		/// <summary>Create the history parser based on the input stream</summary>
		/// <param name="in"/>
		public JobHistoryParser(FSDataInputStream @in)
		{
			this.@in = @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Parse(HistoryEventHandler handler)
		{
			lock (this)
			{
				Parse(new EventReader(@in), handler);
			}
		}

		/// <summary>Only used for unit tests.</summary>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void Parse(EventReader reader, HistoryEventHandler handler)
		{
			lock (this)
			{
				int eventCtr = 0;
				HistoryEvent @event;
				try
				{
					while ((@event = reader.GetNextEvent()) != null)
					{
						handler.HandleEvent(@event);
						++eventCtr;
					}
				}
				catch (IOException ioe)
				{
					Log.Info("Caught exception parsing history file after " + eventCtr + " events", ioe
						);
					parseException = ioe;
				}
				finally
				{
					@in.Close();
				}
			}
		}

		/// <summary>
		/// Parse the entire history file and populate the JobInfo object
		/// The first invocation will populate the object, subsequent calls
		/// will return the already parsed object.
		/// </summary>
		/// <remarks>
		/// Parse the entire history file and populate the JobInfo object
		/// The first invocation will populate the object, subsequent calls
		/// will return the already parsed object.
		/// The input stream is closed on return
		/// This api ignores partial records and stops parsing on encountering one.
		/// <see cref="GetParseException()"/>
		/// can be used to fetch the exception, if any.
		/// </remarks>
		/// <returns>The populated jobInfo object</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="GetParseException()"/>
		public virtual JobHistoryParser.JobInfo Parse()
		{
			lock (this)
			{
				return Parse(new EventReader(@in));
			}
		}

		/// <summary>Only used for unit tests.</summary>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual JobHistoryParser.JobInfo Parse(EventReader reader)
		{
			lock (this)
			{
				if (info != null)
				{
					return info;
				}
				info = new JobHistoryParser.JobInfo();
				Parse(reader, this);
				return info;
			}
		}

		/// <summary>Get the parse exception, if any.</summary>
		/// <returns>the parse exception, if any</returns>
		/// <seealso cref="Parse()"/>
		public virtual IOException GetParseException()
		{
			lock (this)
			{
				return parseException;
			}
		}

		public virtual void HandleEvent(HistoryEvent @event)
		{
			EventType type = @event.GetEventType();
			switch (type)
			{
				case EventType.JobSubmitted:
				{
					HandleJobSubmittedEvent((JobSubmittedEvent)@event);
					break;
				}

				case EventType.JobStatusChanged:
				{
					break;
				}

				case EventType.JobInfoChanged:
				{
					HandleJobInfoChangeEvent((JobInfoChangeEvent)@event);
					break;
				}

				case EventType.JobInited:
				{
					HandleJobInitedEvent((JobInitedEvent)@event);
					break;
				}

				case EventType.JobPriorityChanged:
				{
					HandleJobPriorityChangeEvent((JobPriorityChangeEvent)@event);
					break;
				}

				case EventType.JobQueueChanged:
				{
					HandleJobQueueChangeEvent((JobQueueChangeEvent)@event);
					break;
				}

				case EventType.JobFailed:
				case EventType.JobKilled:
				case EventType.JobError:
				{
					HandleJobFailedEvent((JobUnsuccessfulCompletionEvent)@event);
					break;
				}

				case EventType.JobFinished:
				{
					HandleJobFinishedEvent((JobFinishedEvent)@event);
					break;
				}

				case EventType.TaskStarted:
				{
					HandleTaskStartedEvent((TaskStartedEvent)@event);
					break;
				}

				case EventType.TaskFailed:
				{
					HandleTaskFailedEvent((TaskFailedEvent)@event);
					break;
				}

				case EventType.TaskUpdated:
				{
					HandleTaskUpdatedEvent((TaskUpdatedEvent)@event);
					break;
				}

				case EventType.TaskFinished:
				{
					HandleTaskFinishedEvent((TaskFinishedEvent)@event);
					break;
				}

				case EventType.MapAttemptStarted:
				case EventType.CleanupAttemptStarted:
				case EventType.ReduceAttemptStarted:
				case EventType.SetupAttemptStarted:
				{
					HandleTaskAttemptStartedEvent((TaskAttemptStartedEvent)@event);
					break;
				}

				case EventType.MapAttemptFailed:
				case EventType.CleanupAttemptFailed:
				case EventType.ReduceAttemptFailed:
				case EventType.SetupAttemptFailed:
				case EventType.MapAttemptKilled:
				case EventType.CleanupAttemptKilled:
				case EventType.ReduceAttemptKilled:
				case EventType.SetupAttemptKilled:
				{
					HandleTaskAttemptFailedEvent((TaskAttemptUnsuccessfulCompletionEvent)@event);
					break;
				}

				case EventType.MapAttemptFinished:
				{
					HandleMapAttemptFinishedEvent((MapAttemptFinishedEvent)@event);
					break;
				}

				case EventType.ReduceAttemptFinished:
				{
					HandleReduceAttemptFinishedEvent((ReduceAttemptFinishedEvent)@event);
					break;
				}

				case EventType.SetupAttemptFinished:
				case EventType.CleanupAttemptFinished:
				{
					HandleTaskAttemptFinishedEvent((TaskAttemptFinishedEvent)@event);
					break;
				}

				case EventType.AmStarted:
				{
					HandleAMStartedEvent((AMStartedEvent)@event);
					break;
				}

				default:
				{
					break;
				}
			}
		}

		private void HandleTaskAttemptFinishedEvent(TaskAttemptFinishedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			JobHistoryParser.TaskAttemptInfo attemptInfo = taskInfo.attemptsMap[@event.GetAttemptId
				()];
			attemptInfo.finishTime = @event.GetFinishTime();
			attemptInfo.status = StringInterner.WeakIntern(@event.GetTaskStatus());
			attemptInfo.state = StringInterner.WeakIntern(@event.GetState());
			attemptInfo.counters = @event.GetCounters();
			attemptInfo.hostname = StringInterner.WeakIntern(@event.GetHostname());
			info.completedTaskAttemptsMap[@event.GetAttemptId()] = attemptInfo;
		}

		private void HandleReduceAttemptFinishedEvent(ReduceAttemptFinishedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			JobHistoryParser.TaskAttemptInfo attemptInfo = taskInfo.attemptsMap[@event.GetAttemptId
				()];
			attemptInfo.finishTime = @event.GetFinishTime();
			attemptInfo.status = StringInterner.WeakIntern(@event.GetTaskStatus());
			attemptInfo.state = StringInterner.WeakIntern(@event.GetState());
			attemptInfo.shuffleFinishTime = @event.GetShuffleFinishTime();
			attemptInfo.sortFinishTime = @event.GetSortFinishTime();
			attemptInfo.counters = @event.GetCounters();
			attemptInfo.hostname = StringInterner.WeakIntern(@event.GetHostname());
			attemptInfo.port = @event.GetPort();
			attemptInfo.rackname = StringInterner.WeakIntern(@event.GetRackName());
			info.completedTaskAttemptsMap[@event.GetAttemptId()] = attemptInfo;
		}

		private void HandleMapAttemptFinishedEvent(MapAttemptFinishedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			JobHistoryParser.TaskAttemptInfo attemptInfo = taskInfo.attemptsMap[@event.GetAttemptId
				()];
			attemptInfo.finishTime = @event.GetFinishTime();
			attemptInfo.status = StringInterner.WeakIntern(@event.GetTaskStatus());
			attemptInfo.state = StringInterner.WeakIntern(@event.GetState());
			attemptInfo.mapFinishTime = @event.GetMapFinishTime();
			attemptInfo.counters = @event.GetCounters();
			attemptInfo.hostname = StringInterner.WeakIntern(@event.GetHostname());
			attemptInfo.port = @event.GetPort();
			attemptInfo.rackname = StringInterner.WeakIntern(@event.GetRackName());
			info.completedTaskAttemptsMap[@event.GetAttemptId()] = attemptInfo;
		}

		private void HandleTaskAttemptFailedEvent(TaskAttemptUnsuccessfulCompletionEvent 
			@event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			if (taskInfo == null)
			{
				Log.Warn("TaskInfo is null for TaskAttemptUnsuccessfulCompletionEvent" + " taskId:  "
					 + @event.GetTaskId().ToString());
				return;
			}
			JobHistoryParser.TaskAttemptInfo attemptInfo = taskInfo.attemptsMap[@event.GetTaskAttemptId
				()];
			if (attemptInfo == null)
			{
				Log.Warn("AttemptInfo is null for TaskAttemptUnsuccessfulCompletionEvent" + " taskAttemptId:  "
					 + @event.GetTaskAttemptId().ToString());
				return;
			}
			attemptInfo.finishTime = @event.GetFinishTime();
			attemptInfo.error = StringInterner.WeakIntern(@event.GetError());
			attemptInfo.status = StringInterner.WeakIntern(@event.GetTaskStatus());
			attemptInfo.hostname = StringInterner.WeakIntern(@event.GetHostname());
			attemptInfo.port = @event.GetPort();
			attemptInfo.rackname = StringInterner.WeakIntern(@event.GetRackName());
			attemptInfo.shuffleFinishTime = @event.GetFinishTime();
			attemptInfo.sortFinishTime = @event.GetFinishTime();
			attemptInfo.mapFinishTime = @event.GetFinishTime();
			attemptInfo.counters = @event.GetCounters();
			if (TaskStatus.State.Succeeded.ToString().Equals(taskInfo.status))
			{
				//this is a successful task
				if (attemptInfo.GetAttemptId().Equals(taskInfo.GetSuccessfulAttemptId()))
				{
					// the failed attempt is the one that made this task successful
					// so its no longer successful. Reset fields set in
					// handleTaskFinishedEvent()
					taskInfo.counters = null;
					taskInfo.finishTime = -1;
					taskInfo.status = null;
					taskInfo.successfulAttemptId = null;
				}
			}
			info.completedTaskAttemptsMap[@event.GetTaskAttemptId()] = attemptInfo;
		}

		private void HandleTaskAttemptStartedEvent(TaskAttemptStartedEvent @event)
		{
			TaskAttemptID attemptId = @event.GetTaskAttemptId();
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			JobHistoryParser.TaskAttemptInfo attemptInfo = new JobHistoryParser.TaskAttemptInfo
				();
			attemptInfo.startTime = @event.GetStartTime();
			attemptInfo.attemptId = @event.GetTaskAttemptId();
			attemptInfo.httpPort = @event.GetHttpPort();
			attemptInfo.trackerName = StringInterner.WeakIntern(@event.GetTrackerName());
			attemptInfo.taskType = @event.GetTaskType();
			attemptInfo.shufflePort = @event.GetShufflePort();
			attemptInfo.containerId = @event.GetContainerId();
			taskInfo.attemptsMap[attemptId] = attemptInfo;
		}

		private void HandleTaskFinishedEvent(TaskFinishedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			taskInfo.counters = @event.GetCounters();
			taskInfo.finishTime = @event.GetFinishTime();
			taskInfo.status = TaskStatus.State.Succeeded.ToString();
			taskInfo.successfulAttemptId = @event.GetSuccessfulTaskAttemptId();
		}

		private void HandleTaskUpdatedEvent(TaskUpdatedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			taskInfo.finishTime = @event.GetFinishTime();
		}

		private void HandleTaskFailedEvent(TaskFailedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = info.tasksMap[@event.GetTaskId()];
			taskInfo.status = TaskStatus.State.Failed.ToString();
			taskInfo.finishTime = @event.GetFinishTime();
			taskInfo.error = StringInterner.WeakIntern(@event.GetError());
			taskInfo.failedDueToAttemptId = @event.GetFailedAttemptID();
			taskInfo.counters = @event.GetCounters();
		}

		private void HandleTaskStartedEvent(TaskStartedEvent @event)
		{
			JobHistoryParser.TaskInfo taskInfo = new JobHistoryParser.TaskInfo();
			taskInfo.taskId = @event.GetTaskId();
			taskInfo.startTime = @event.GetStartTime();
			taskInfo.taskType = @event.GetTaskType();
			taskInfo.splitLocations = @event.GetSplitLocations();
			info.tasksMap[@event.GetTaskId()] = taskInfo;
		}

		private void HandleJobFailedEvent(JobUnsuccessfulCompletionEvent @event)
		{
			info.finishTime = @event.GetFinishTime();
			info.finishedMaps = @event.GetFinishedMaps();
			info.finishedReduces = @event.GetFinishedReduces();
			info.jobStatus = StringInterner.WeakIntern(@event.GetStatus());
			info.errorInfo = StringInterner.WeakIntern(@event.GetDiagnostics());
		}

		private void HandleJobFinishedEvent(JobFinishedEvent @event)
		{
			info.finishTime = @event.GetFinishTime();
			info.finishedMaps = @event.GetFinishedMaps();
			info.finishedReduces = @event.GetFinishedReduces();
			info.failedMaps = @event.GetFailedMaps();
			info.failedReduces = @event.GetFailedReduces();
			info.totalCounters = @event.GetTotalCounters();
			info.mapCounters = @event.GetMapCounters();
			info.reduceCounters = @event.GetReduceCounters();
			info.jobStatus = JobStatus.GetJobRunState(JobStatus.Succeeded);
		}

		private void HandleJobPriorityChangeEvent(JobPriorityChangeEvent @event)
		{
			info.priority = @event.GetPriority();
		}

		private void HandleJobQueueChangeEvent(JobQueueChangeEvent @event)
		{
			info.jobQueueName = @event.GetJobQueueName();
		}

		private void HandleJobInitedEvent(JobInitedEvent @event)
		{
			info.launchTime = @event.GetLaunchTime();
			info.totalMaps = @event.GetTotalMaps();
			info.totalReduces = @event.GetTotalReduces();
			info.uberized = @event.GetUberized();
		}

		private void HandleAMStartedEvent(AMStartedEvent @event)
		{
			JobHistoryParser.AMInfo amInfo = new JobHistoryParser.AMInfo();
			amInfo.appAttemptId = @event.GetAppAttemptId();
			amInfo.startTime = @event.GetStartTime();
			amInfo.containerId = @event.GetContainerId();
			amInfo.nodeManagerHost = StringInterner.WeakIntern(@event.GetNodeManagerHost());
			amInfo.nodeManagerPort = @event.GetNodeManagerPort();
			amInfo.nodeManagerHttpPort = @event.GetNodeManagerHttpPort();
			if (info.amInfos == null)
			{
				info.amInfos = new List<JobHistoryParser.AMInfo>();
			}
			info.amInfos.AddItem(amInfo);
			info.latestAmInfo = amInfo;
		}

		private void HandleJobInfoChangeEvent(JobInfoChangeEvent @event)
		{
			info.submitTime = @event.GetSubmitTime();
			info.launchTime = @event.GetLaunchTime();
		}

		private void HandleJobSubmittedEvent(JobSubmittedEvent @event)
		{
			info.jobid = @event.GetJobId();
			info.jobname = @event.GetJobName();
			info.username = StringInterner.WeakIntern(@event.GetUserName());
			info.submitTime = @event.GetSubmitTime();
			info.jobConfPath = @event.GetJobConfPath();
			info.jobACLs = @event.GetJobAcls();
			info.jobQueueName = StringInterner.WeakIntern(@event.GetJobQueueName());
		}

		/// <summary>The class where job information is aggregated into after parsing</summary>
		public class JobInfo
		{
			internal string errorInfo = string.Empty;

			internal long submitTime;

			internal long finishTime;

			internal JobID jobid;

			internal string username;

			internal string jobname;

			internal string jobQueueName;

			internal string jobConfPath;

			internal long launchTime;

			internal int totalMaps;

			internal int totalReduces;

			internal int failedMaps;

			internal int failedReduces;

			internal int finishedMaps;

			internal int finishedReduces;

			internal string jobStatus;

			internal Counters totalCounters;

			internal Counters mapCounters;

			internal Counters reduceCounters;

			internal JobPriority priority;

			internal IDictionary<JobACL, AccessControlList> jobACLs;

			internal IDictionary<TaskID, JobHistoryParser.TaskInfo> tasksMap;

			internal IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> completedTaskAttemptsMap;

			internal IList<JobHistoryParser.AMInfo> amInfos;

			internal JobHistoryParser.AMInfo latestAmInfo;

			internal bool uberized;

			/// <summary>
			/// Create a job info object where job information will be stored
			/// after a parse
			/// </summary>
			public JobInfo()
			{
				submitTime = launchTime = finishTime = -1;
				totalMaps = totalReduces = failedMaps = failedReduces = 0;
				finishedMaps = finishedReduces = 0;
				username = jobname = jobConfPath = jobQueueName = string.Empty;
				tasksMap = new Dictionary<TaskID, JobHistoryParser.TaskInfo>();
				completedTaskAttemptsMap = new Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo
					>();
				jobACLs = new Dictionary<JobACL, AccessControlList>();
				priority = JobPriority.Normal;
			}

			/// <summary>Print all the job information</summary>
			public virtual void PrintAll()
			{
				System.Console.Out.WriteLine("JOBNAME: " + jobname);
				System.Console.Out.WriteLine("USERNAME: " + username);
				System.Console.Out.WriteLine("JOB_QUEUE_NAME: " + jobQueueName);
				System.Console.Out.WriteLine("SUBMIT_TIME" + submitTime);
				System.Console.Out.WriteLine("LAUNCH_TIME: " + launchTime);
				System.Console.Out.WriteLine("JOB_STATUS: " + jobStatus);
				System.Console.Out.WriteLine("PRIORITY: " + priority);
				System.Console.Out.WriteLine("TOTAL_MAPS: " + totalMaps);
				System.Console.Out.WriteLine("TOTAL_REDUCES: " + totalReduces);
				if (mapCounters != null)
				{
					System.Console.Out.WriteLine("MAP_COUNTERS:" + mapCounters.ToString());
				}
				if (reduceCounters != null)
				{
					System.Console.Out.WriteLine("REDUCE_COUNTERS:" + reduceCounters.ToString());
				}
				if (totalCounters != null)
				{
					System.Console.Out.WriteLine("TOTAL_COUNTERS: " + totalCounters.ToString());
				}
				System.Console.Out.WriteLine("UBERIZED: " + uberized);
				if (amInfos != null)
				{
					foreach (JobHistoryParser.AMInfo amInfo in amInfos)
					{
						amInfo.PrintAll();
					}
				}
				foreach (JobHistoryParser.TaskInfo ti in tasksMap.Values)
				{
					ti.PrintAll();
				}
			}

			/// <returns>the job submit time</returns>
			public virtual long GetSubmitTime()
			{
				return submitTime;
			}

			/// <returns>the job finish time</returns>
			public virtual long GetFinishTime()
			{
				return finishTime;
			}

			/// <returns>the job id</returns>
			public virtual JobID GetJobId()
			{
				return jobid;
			}

			/// <returns>the user name</returns>
			public virtual string GetUsername()
			{
				return username;
			}

			/// <returns>the job name</returns>
			public virtual string GetJobname()
			{
				return jobname;
			}

			/// <returns>the job queue name</returns>
			public virtual string GetJobQueueName()
			{
				return jobQueueName;
			}

			/// <returns>the path for the job configuration file</returns>
			public virtual string GetJobConfPath()
			{
				return jobConfPath;
			}

			/// <returns>the job launch time</returns>
			public virtual long GetLaunchTime()
			{
				return launchTime;
			}

			/// <returns>the total number of maps</returns>
			public virtual long GetTotalMaps()
			{
				return totalMaps;
			}

			/// <returns>the total number of reduces</returns>
			public virtual long GetTotalReduces()
			{
				return totalReduces;
			}

			/// <returns>the total number of failed maps</returns>
			public virtual long GetFailedMaps()
			{
				return failedMaps;
			}

			/// <returns>the number of failed reduces</returns>
			public virtual long GetFailedReduces()
			{
				return failedReduces;
			}

			/// <returns>the number of finished maps</returns>
			public virtual long GetFinishedMaps()
			{
				return finishedMaps;
			}

			/// <returns>the number of finished reduces</returns>
			public virtual long GetFinishedReduces()
			{
				return finishedReduces;
			}

			/// <returns>the job status</returns>
			public virtual string GetJobStatus()
			{
				return jobStatus;
			}

			public virtual string GetErrorInfo()
			{
				return errorInfo;
			}

			/// <returns>the counters for the job</returns>
			public virtual Counters GetTotalCounters()
			{
				return totalCounters;
			}

			/// <returns>the map counters for the job</returns>
			public virtual Counters GetMapCounters()
			{
				return mapCounters;
			}

			/// <returns>the reduce counters for the job</returns>
			public virtual Counters GetReduceCounters()
			{
				return reduceCounters;
			}

			/// <returns>the map of all tasks in this job</returns>
			public virtual IDictionary<TaskID, JobHistoryParser.TaskInfo> GetAllTasks()
			{
				return tasksMap;
			}

			/// <returns>the map of all completed task attempts in this job</returns>
			public virtual IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> GetAllCompletedTaskAttempts
				()
			{
				return completedTaskAttemptsMap;
			}

			/// <returns>the priority of this job</returns>
			public virtual string GetPriority()
			{
				return priority.ToString();
			}

			public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
			{
				return jobACLs;
			}

			/// <returns>the uberized status of this job</returns>
			public virtual bool GetUberized()
			{
				return uberized;
			}

			/// <returns>the AMInfo for the job's AppMaster</returns>
			public virtual IList<JobHistoryParser.AMInfo> GetAMInfos()
			{
				return amInfos;
			}

			/// <returns>the AMInfo for the newest AppMaster</returns>
			public virtual JobHistoryParser.AMInfo GetLatestAMInfo()
			{
				return latestAmInfo;
			}
		}

		/// <summary>TaskInformation is aggregated in this class after parsing</summary>
		public class TaskInfo
		{
			internal TaskID taskId;

			internal long startTime;

			internal long finishTime;

			internal TaskType taskType;

			internal string splitLocations;

			internal Counters counters;

			internal string status;

			internal string error;

			internal TaskAttemptID failedDueToAttemptId;

			internal TaskAttemptID successfulAttemptId;

			internal IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attemptsMap;

			public TaskInfo()
			{
				startTime = finishTime = -1;
				error = splitLocations = string.Empty;
				attemptsMap = new Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			}

			public virtual void PrintAll()
			{
				System.Console.Out.WriteLine("TASK_ID:" + taskId.ToString());
				System.Console.Out.WriteLine("START_TIME: " + startTime);
				System.Console.Out.WriteLine("FINISH_TIME:" + finishTime);
				System.Console.Out.WriteLine("TASK_TYPE:" + taskType);
				if (counters != null)
				{
					System.Console.Out.WriteLine("COUNTERS:" + counters.ToString());
				}
				foreach (JobHistoryParser.TaskAttemptInfo tinfo in attemptsMap.Values)
				{
					tinfo.PrintAll();
				}
			}

			/// <returns>the Task ID</returns>
			public virtual TaskID GetTaskId()
			{
				return taskId;
			}

			/// <returns>the start time of this task</returns>
			public virtual long GetStartTime()
			{
				return startTime;
			}

			/// <returns>the finish time of this task</returns>
			public virtual long GetFinishTime()
			{
				return finishTime;
			}

			/// <returns>the task type</returns>
			public virtual TaskType GetTaskType()
			{
				return taskType;
			}

			/// <returns>the split locations</returns>
			public virtual string GetSplitLocations()
			{
				return splitLocations;
			}

			/// <returns>the counters for this task</returns>
			public virtual Counters GetCounters()
			{
				return counters;
			}

			/// <returns>the task status</returns>
			public virtual string GetTaskStatus()
			{
				return status;
			}

			/// <returns>the attempt Id that caused this task to fail</returns>
			public virtual TaskAttemptID GetFailedDueToAttemptId()
			{
				return failedDueToAttemptId;
			}

			/// <returns>the attempt Id that caused this task to succeed</returns>
			public virtual TaskAttemptID GetSuccessfulAttemptId()
			{
				return successfulAttemptId;
			}

			/// <returns>the error</returns>
			public virtual string GetError()
			{
				return error;
			}

			/// <returns>the map of all attempts for this task</returns>
			public virtual IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> GetAllTaskAttempts
				()
			{
				return attemptsMap;
			}
		}

		/// <summary>Task Attempt Information is aggregated in this class after parsing</summary>
		public class TaskAttemptInfo
		{
			internal TaskAttemptID attemptId;

			internal long startTime;

			internal long finishTime;

			internal long shuffleFinishTime;

			internal long sortFinishTime;

			internal long mapFinishTime;

			internal string error;

			internal string status;

			internal string state;

			internal TaskType taskType;

			internal string trackerName;

			internal Counters counters;

			internal int httpPort;

			internal int shufflePort;

			internal string hostname;

			internal int port;

			internal string rackname;

			internal ContainerId containerId;

			/// <summary>
			/// Create a Task Attempt Info which will store attempt level information
			/// on a history parse.
			/// </summary>
			public TaskAttemptInfo()
			{
				startTime = finishTime = shuffleFinishTime = sortFinishTime = mapFinishTime = -1;
				error = state = trackerName = hostname = rackname = string.Empty;
				port = -1;
				httpPort = -1;
				shufflePort = -1;
			}

			/// <summary>Print all the information about this attempt.</summary>
			public virtual void PrintAll()
			{
				System.Console.Out.WriteLine("ATTEMPT_ID:" + attemptId.ToString());
				System.Console.Out.WriteLine("START_TIME: " + startTime);
				System.Console.Out.WriteLine("FINISH_TIME:" + finishTime);
				System.Console.Out.WriteLine("ERROR:" + error);
				System.Console.Out.WriteLine("TASK_STATUS:" + status);
				System.Console.Out.WriteLine("STATE:" + state);
				System.Console.Out.WriteLine("TASK_TYPE:" + taskType);
				System.Console.Out.WriteLine("TRACKER_NAME:" + trackerName);
				System.Console.Out.WriteLine("HTTP_PORT:" + httpPort);
				System.Console.Out.WriteLine("SHUFFLE_PORT:" + shufflePort);
				System.Console.Out.WriteLine("CONTIANER_ID:" + containerId);
				if (counters != null)
				{
					System.Console.Out.WriteLine("COUNTERS:" + counters.ToString());
				}
			}

			/// <returns>the attempt Id</returns>
			public virtual TaskAttemptID GetAttemptId()
			{
				return attemptId;
			}

			/// <returns>the start time of the attempt</returns>
			public virtual long GetStartTime()
			{
				return startTime;
			}

			/// <returns>the finish time of the attempt</returns>
			public virtual long GetFinishTime()
			{
				return finishTime;
			}

			/// <returns>the shuffle finish time. Applicable only for reduce attempts</returns>
			public virtual long GetShuffleFinishTime()
			{
				return shuffleFinishTime;
			}

			/// <returns>the sort finish time. Applicable only for reduce attempts</returns>
			public virtual long GetSortFinishTime()
			{
				return sortFinishTime;
			}

			/// <returns>the map finish time. Applicable only for map attempts</returns>
			public virtual long GetMapFinishTime()
			{
				return mapFinishTime;
			}

			/// <returns>the error string</returns>
			public virtual string GetError()
			{
				return error;
			}

			/// <returns>the state</returns>
			public virtual string GetState()
			{
				return state;
			}

			/// <returns>the task status</returns>
			public virtual string GetTaskStatus()
			{
				return status;
			}

			/// <returns>the task type</returns>
			public virtual TaskType GetTaskType()
			{
				return taskType;
			}

			/// <returns>the tracker name where the attempt executed</returns>
			public virtual string GetTrackerName()
			{
				return trackerName;
			}

			/// <returns>the host name</returns>
			public virtual string GetHostname()
			{
				return hostname;
			}

			/// <returns>the port</returns>
			public virtual int GetPort()
			{
				return port;
			}

			/// <returns>the rack name</returns>
			public virtual string GetRackname()
			{
				return rackname;
			}

			/// <returns>the counters for the attempt</returns>
			public virtual Counters GetCounters()
			{
				return counters;
			}

			/// <returns>the HTTP port for the tracker</returns>
			public virtual int GetHttpPort()
			{
				return httpPort;
			}

			/// <returns>the Shuffle port for the tracker</returns>
			public virtual int GetShufflePort()
			{
				return shufflePort;
			}

			/// <returns>the ContainerId for the tracker</returns>
			public virtual ContainerId GetContainerId()
			{
				return containerId;
			}
		}

		/// <summary>Stores AM information</summary>
		public class AMInfo
		{
			internal ApplicationAttemptId appAttemptId;

			internal long startTime;

			internal ContainerId containerId;

			internal string nodeManagerHost;

			internal int nodeManagerPort;

			internal int nodeManagerHttpPort;

			/// <summary>
			/// Create a AM Info which will store AM level information on a history
			/// parse.
			/// </summary>
			public AMInfo()
			{
				startTime = -1;
				nodeManagerHost = string.Empty;
				nodeManagerHttpPort = -1;
			}

			public AMInfo(ApplicationAttemptId appAttemptId, long startTime, ContainerId containerId
				, string nodeManagerHost, int nodeManagerPort, int nodeManagerHttpPort)
			{
				this.appAttemptId = appAttemptId;
				this.startTime = startTime;
				this.containerId = containerId;
				this.nodeManagerHost = nodeManagerHost;
				this.nodeManagerPort = nodeManagerPort;
				this.nodeManagerHttpPort = nodeManagerHttpPort;
			}

			/// <summary>Print all the information about this AM.</summary>
			public virtual void PrintAll()
			{
				System.Console.Out.WriteLine("APPLICATION_ATTEMPT_ID:" + appAttemptId.ToString());
				System.Console.Out.WriteLine("START_TIME: " + startTime);
				System.Console.Out.WriteLine("CONTAINER_ID: " + containerId.ToString());
				System.Console.Out.WriteLine("NODE_MANAGER_HOST: " + nodeManagerHost);
				System.Console.Out.WriteLine("NODE_MANAGER_PORT: " + nodeManagerPort);
				System.Console.Out.WriteLine("NODE_MANAGER_HTTP_PORT: " + nodeManagerHttpPort);
			}

			/// <returns>the ApplicationAttemptId</returns>
			public virtual ApplicationAttemptId GetAppAttemptId()
			{
				return appAttemptId;
			}

			/// <returns>the start time of the AM</returns>
			public virtual long GetStartTime()
			{
				return startTime;
			}

			/// <returns>the container id for the AM</returns>
			public virtual ContainerId GetContainerId()
			{
				return containerId;
			}

			/// <returns>the host name for the node manager on which the AM is running</returns>
			public virtual string GetNodeManagerHost()
			{
				return nodeManagerHost;
			}

			/// <returns>the port for the node manager running the AM</returns>
			public virtual int GetNodeManagerPort()
			{
				return nodeManagerPort;
			}

			/// <returns>the http port for the node manager running the AM</returns>
			public virtual int GetNodeManagerHttpPort()
			{
				return nodeManagerHttpPort;
			}
		}
	}
}
