using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class MockJobs : MockApps
	{
		internal static readonly IEnumerator<JobState> JobStates = Iterators.Cycle(JobState
			.Values());

		internal static readonly IEnumerator<TaskState> TaskStates = Iterators.Cycle(TaskState
			.Values());

		internal static readonly IEnumerator<TaskAttemptState> TaskAttemptStates = Iterators
			.Cycle(TaskAttemptState.Values());

		internal static readonly IEnumerator<TaskType> TaskTypes = Iterators.Cycle(TaskType
			.Values());

		internal static readonly IEnumerator<JobCounter> JobCounters = Iterators.Cycle(JobCounter
			.Values());

		internal static readonly IEnumerator<FileSystemCounter> FsCounters = Iterators.Cycle
			(FileSystemCounter.Values());

		internal static readonly IEnumerator<TaskCounter> TaskCounters = Iterators.Cycle(
			TaskCounter.Values());

		internal static readonly IEnumerator<string> FsSchemes = Iterators.Cycle("FILE", 
			"HDFS", "LAFS", "CEPH");

		internal static readonly IEnumerator<string> UserCounterGroups = Iterators.Cycle(
			"com.company.project.subproject.component.subcomponent.UserDefinedSpecificSpecialTask$Counters"
			, "PigCounters");

		internal static readonly IEnumerator<string> UserCounters = Iterators.Cycle("counter1"
			, "counter2", "counter3");

		internal static readonly IEnumerator<Phase> Phases = Iterators.Cycle(Phase.Values
			());

		internal static readonly IEnumerator<string> Diags = Iterators.Cycle("Error: java.lang.OutOfMemoryError: Java heap space"
			, "Lost task tracker: tasktracker.domain/127.0.0.1:40879");

		public const string NmHost = "localhost";

		public const int NmPort = 1234;

		public const int NmHttpPort = 8042;

		internal const int Dt = 1000000;

		// ms
		public static string NewJobName()
		{
			return NewAppName();
		}

		/// <summary>Create numJobs in a map with jobs having appId==jobId</summary>
		public static IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> NewJobs
			(int numJobs, int numTasksPerJob, int numAttemptsPerTask)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> map = Maps.NewHashMap
				();
			for (int j = 0; j < numJobs; ++j)
			{
				ApplicationId appID = MockJobs.NewAppID(j);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = NewJob(appID, j, numTasksPerJob, 
					numAttemptsPerTask);
				map[job.GetID()] = job;
			}
			return map;
		}

		public static IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> NewJobs
			(ApplicationId appID, int numJobsPerApp, int numTasksPerJob, int numAttemptsPerTask
			)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> map = Maps.NewHashMap
				();
			for (int j = 0; j < numJobsPerApp; ++j)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = NewJob(appID, j, numTasksPerJob, 
					numAttemptsPerTask);
				map[job.GetID()] = job;
			}
			return map;
		}

		public static IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> NewJobs
			(ApplicationId appID, int numJobsPerApp, int numTasksPerJob, int numAttemptsPerTask
			, bool hasFailedTasks)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> map = Maps.NewHashMap
				();
			for (int j = 0; j < numJobsPerApp; ++j)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = NewJob(appID, j, numTasksPerJob, 
					numAttemptsPerTask, null, hasFailedTasks);
				map[job.GetID()] = job;
			}
			return map;
		}

		public static JobId NewJobID(ApplicationId appID, int i)
		{
			JobId id = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobId>();
			id.SetAppId(appID);
			id.SetId(i);
			return id;
		}

		public static JobReport NewJobReport(JobId id)
		{
			JobReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			report.SetJobId(id);
			report.SetSubmitTime(Runtime.CurrentTimeMillis() - Dt);
			report.SetStartTime(Runtime.CurrentTimeMillis() - (int)(Math.Random() * Dt));
			report.SetFinishTime(Runtime.CurrentTimeMillis() + (int)(Math.Random() * Dt) + 1);
			report.SetMapProgress((float)Math.Random());
			report.SetReduceProgress((float)Math.Random());
			report.SetJobState(JobStates.Next());
			return report;
		}

		public static TaskReport NewTaskReport(TaskId id)
		{
			TaskReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskReport>();
			report.SetTaskId(id);
			report.SetStartTime(Runtime.CurrentTimeMillis() - (int)(Math.Random() * Dt));
			report.SetFinishTime(Runtime.CurrentTimeMillis() + (int)(Math.Random() * Dt) + 1);
			report.SetProgress((float)Math.Random());
			report.SetStatus("Moving average: " + Math.Random());
			report.SetCounters(TypeConverter.ToYarn(NewCounters()));
			report.SetTaskState(TaskStates.Next());
			return report;
		}

		public static TaskAttemptReport NewTaskAttemptReport(TaskAttemptId id)
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(id.GetTaskId
				().GetJobId().GetAppId(), 0);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 0);
			TaskAttemptReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskAttemptReport
				>();
			report.SetTaskAttemptId(id);
			report.SetStartTime(Runtime.CurrentTimeMillis() - (int)(Math.Random() * Dt));
			report.SetFinishTime(Runtime.CurrentTimeMillis() + (int)(Math.Random() * Dt) + 1);
			if (id.GetTaskId().GetTaskType() == TaskType.Reduce)
			{
				report.SetShuffleFinishTime((report.GetFinishTime() + report.GetStartTime()) / 2);
				report.SetSortFinishTime((report.GetFinishTime() + report.GetShuffleFinishTime())
					 / 2);
			}
			report.SetPhase(Phases.Next());
			report.SetTaskAttemptState(TaskAttemptStates.Next());
			report.SetProgress((float)Math.Random());
			report.SetCounters(TypeConverter.ToYarn(NewCounters()));
			report.SetContainerId(containerId);
			report.SetDiagnosticInfo(Diags.Next());
			report.SetStateString("Moving average " + Math.Random());
			return report;
		}

		public static Counters NewCounters()
		{
			Counters hc = new Counters();
			foreach (JobCounter c in JobCounter.Values())
			{
				hc.FindCounter(c).SetValue((long)(Math.Random() * 1000));
			}
			foreach (TaskCounter c_1 in TaskCounter.Values())
			{
				hc.FindCounter(c_1).SetValue((long)(Math.Random() * 1000));
			}
			int nc = FileSystemCounter.Values().Length * 4;
			for (int i = 0; i < nc; ++i)
			{
				foreach (FileSystemCounter c_2 in FileSystemCounter.Values())
				{
					hc.FindCounter(FsSchemes.Next(), c_2).SetValue((long)(Math.Random() * Dt));
				}
			}
			for (int i_1 = 0; i_1 < 2 * 3; ++i_1)
			{
				hc.FindCounter(UserCounterGroups.Next(), UserCounters.Next()).SetValue((long)(Math
					.Random() * 100000));
			}
			return hc;
		}

		public static IDictionary<TaskAttemptId, TaskAttempt> NewTaskAttempts(TaskId tid, 
			int m)
		{
			IDictionary<TaskAttemptId, TaskAttempt> map = Maps.NewHashMap();
			for (int i = 0; i < m; ++i)
			{
				TaskAttempt ta = NewTaskAttempt(tid, i);
				map[ta.GetID()] = ta;
			}
			return map;
		}

		public static TaskAttempt NewTaskAttempt(TaskId tid, int i)
		{
			TaskAttemptId taid = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskAttemptId>
				();
			taid.SetTaskId(tid);
			taid.SetId(i);
			TaskAttemptReport report = NewTaskAttemptReport(taid);
			return new _TaskAttempt_248(taid, report);
		}

		private sealed class _TaskAttempt_248 : TaskAttempt
		{
			public _TaskAttempt_248(TaskAttemptId taid, TaskAttemptReport report)
			{
				this.taid = taid;
				this.report = report;
			}

			/// <exception cref="System.NotSupportedException"/>
			public NodeId GetNodeId()
			{
				throw new NotSupportedException();
			}

			public TaskAttemptId GetID()
			{
				return taid;
			}

			public TaskAttemptReport GetReport()
			{
				return report;
			}

			public long GetLaunchTime()
			{
				return report.GetStartTime();
			}

			public long GetFinishTime()
			{
				return report.GetFinishTime();
			}

			public int GetShufflePort()
			{
				return ShuffleHandler.DefaultShufflePort;
			}

			public Counters GetCounters()
			{
				if (report != null && report.GetCounters() != null)
				{
					return new Counters(TypeConverter.FromYarn(report.GetCounters()));
				}
				return null;
			}

			public float GetProgress()
			{
				return report.GetProgress();
			}

			public Phase GetPhase()
			{
				return report.GetPhase();
			}

			public TaskAttemptState GetState()
			{
				return report.GetTaskAttemptState();
			}

			public bool IsFinished()
			{
				switch (report.GetTaskAttemptState())
				{
					case TaskAttemptState.Succeeded:
					case TaskAttemptState.Failed:
					case TaskAttemptState.Killed:
					{
						return true;
					}
				}
				return false;
			}

			public ContainerId GetAssignedContainerID()
			{
				ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(taid.GetTaskId
					().GetJobId().GetAppId(), 0);
				ContainerId id = ContainerId.NewContainerId(appAttemptId, 0);
				return id;
			}

			public string GetNodeHttpAddress()
			{
				return "localhost:8042";
			}

			public IList<string> GetDiagnostics()
			{
				return Lists.NewArrayList(report.GetDiagnosticInfo());
			}

			public string GetAssignedContainerMgrAddress()
			{
				return "localhost:9998";
			}

			public long GetShuffleFinishTime()
			{
				return report.GetShuffleFinishTime();
			}

			public long GetSortFinishTime()
			{
				return report.GetSortFinishTime();
			}

			public string GetNodeRackName()
			{
				return "/default-rack";
			}

			private readonly TaskAttemptId taid;

			private readonly TaskAttemptReport report;
		}

		public static IDictionary<TaskId, Task> NewTasks(JobId jid, int n, int m, bool hasFailedTasks
			)
		{
			IDictionary<TaskId, Task> map = Maps.NewHashMap();
			for (int i = 0; i < n; ++i)
			{
				Task task = NewTask(jid, i, m, hasFailedTasks);
				map[task.GetID()] = task;
			}
			return map;
		}

		public static Task NewTask(JobId jid, int i, int m, bool hasFailedTasks)
		{
			TaskId tid = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskId>();
			tid.SetJobId(jid);
			tid.SetId(i);
			tid.SetTaskType(TaskTypes.Next());
			TaskReport report = NewTaskReport(tid);
			IDictionary<TaskAttemptId, TaskAttempt> attempts = NewTaskAttempts(tid, m);
			return new _Task_370(tid, report, hasFailedTasks, attempts);
		}

		private sealed class _Task_370 : Task
		{
			public _Task_370(TaskId tid, TaskReport report, bool hasFailedTasks, IDictionary<
				TaskAttemptId, TaskAttempt> attempts)
			{
				this.tid = tid;
				this.report = report;
				this.hasFailedTasks = hasFailedTasks;
				this.attempts = attempts;
			}

			public TaskId GetID()
			{
				return tid;
			}

			public TaskReport GetReport()
			{
				return report;
			}

			public Counters GetCounters()
			{
				if (hasFailedTasks)
				{
					return null;
				}
				return new Counters(TypeConverter.FromYarn(report.GetCounters()));
			}

			public float GetProgress()
			{
				return report.GetProgress();
			}

			public TaskType GetType()
			{
				return tid.GetTaskType();
			}

			public IDictionary<TaskAttemptId, TaskAttempt> GetAttempts()
			{
				return attempts;
			}

			public TaskAttempt GetAttempt(TaskAttemptId attemptID)
			{
				return attempts[attemptID];
			}

			public bool IsFinished()
			{
				switch (report.GetTaskState())
				{
					case TaskState.Succeeded:
					case TaskState.Killed:
					case TaskState.Failed:
					{
						return true;
					}
				}
				return false;
			}

			public bool CanCommit(TaskAttemptId taskAttemptID)
			{
				return false;
			}

			public TaskState GetState()
			{
				return report.GetTaskState();
			}

			private readonly TaskId tid;

			private readonly TaskReport report;

			private readonly bool hasFailedTasks;

			private readonly IDictionary<TaskAttemptId, TaskAttempt> attempts;
		}

		public static Counters GetCounters(ICollection<Task> tasks)
		{
			IList<Task> completedTasks = new AList<Task>();
			foreach (Task task in tasks)
			{
				if (task.GetCounters() != null)
				{
					completedTasks.AddItem(task);
				}
			}
			Counters counters = new Counters();
			return JobImpl.IncrTaskCounters(counters, completedTasks);
		}

		internal class TaskCount
		{
			internal int maps;

			internal int reduces;

			internal int completedMaps;

			internal int completedReduces;

			internal virtual void Incr(Task task)
			{
				TaskType type = task.GetType();
				bool finished = task.IsFinished();
				if (type == TaskType.Map)
				{
					if (finished)
					{
						++completedMaps;
					}
					++maps;
				}
				else
				{
					if (type == TaskType.Reduce)
					{
						if (finished)
						{
							++completedReduces;
						}
						++reduces;
					}
				}
			}
		}

		internal static MockJobs.TaskCount GetTaskCount(ICollection<Task> tasks)
		{
			MockJobs.TaskCount tc = new MockJobs.TaskCount();
			foreach (Task task in tasks)
			{
				tc.Incr(task);
			}
			return tc;
		}

		public static Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job NewJob(ApplicationId appID
			, int i, int n, int m)
		{
			return NewJob(appID, i, n, m, null);
		}

		public static Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job NewJob(ApplicationId appID
			, int i, int n, int m, Path confFile)
		{
			return NewJob(appID, i, n, m, confFile, false);
		}

		public static Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job NewJob(ApplicationId appID
			, int i, int n, int m, Path confFile, bool hasFailedTasks)
		{
			JobId id = NewJobID(appID, i);
			string name = NewJobName();
			JobReport report = NewJobReport(id);
			IDictionary<TaskId, Task> tasks = NewTasks(id, n, m, hasFailedTasks);
			MockJobs.TaskCount taskCount = GetTaskCount(tasks.Values);
			Counters counters = GetCounters(tasks.Values);
			Path configFile = confFile;
			IDictionary<JobACL, AccessControlList> tmpJobACLs = new Dictionary<JobACL, AccessControlList
				>();
			Configuration conf = new Configuration();
			conf.Set(JobACL.ViewJob.GetAclName(), "testuser");
			conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			JobACLsManager aclsManager = new JobACLsManager(conf);
			tmpJobACLs = aclsManager.ConstructJobACLs(conf);
			IDictionary<JobACL, AccessControlList> jobACLs = tmpJobACLs;
			return new _Job_503(id, name, report, counters, tasks, taskCount, configFile, jobACLs
				, conf);
		}

		private sealed class _Job_503 : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
		{
			public _Job_503(JobId id, string name, JobReport report, Counters counters, IDictionary
				<TaskId, Task> tasks, MockJobs.TaskCount taskCount, Path configFile, IDictionary
				<JobACL, AccessControlList> jobACLs, Configuration conf)
			{
				this.id = id;
				this.name = name;
				this.report = report;
				this.counters = counters;
				this.tasks = tasks;
				this.taskCount = taskCount;
				this.configFile = configFile;
				this.jobACLs = jobACLs;
				this.conf = conf;
			}

			public JobId GetID()
			{
				return id;
			}

			public string GetName()
			{
				return name;
			}

			public JobState GetState()
			{
				return report.GetJobState();
			}

			public JobReport GetReport()
			{
				return report;
			}

			public float GetProgress()
			{
				return 0;
			}

			public Counters GetAllCounters()
			{
				return counters;
			}

			public IDictionary<TaskId, Task> GetTasks()
			{
				return tasks;
			}

			public Task GetTask(TaskId taskID)
			{
				return tasks[taskID];
			}

			public int GetTotalMaps()
			{
				return taskCount.maps;
			}

			public int GetTotalReduces()
			{
				return taskCount.reduces;
			}

			public int GetCompletedMaps()
			{
				return taskCount.completedMaps;
			}

			public int GetCompletedReduces()
			{
				return taskCount.completedReduces;
			}

			public bool IsUber()
			{
				return false;
			}

			public TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
				, int maxEvents)
			{
				return null;
			}

			public TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex, int maxEvents
				)
			{
				return null;
			}

			public IDictionary<TaskId, Task> GetTasks(TaskType taskType)
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public IList<string> GetDiagnostics()
			{
				return Sharpen.Collections.EmptyList<string>();
			}

			public bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation)
			{
				return true;
			}

			public string GetUserName()
			{
				return "mock";
			}

			public string GetQueueName()
			{
				return "mockqueue";
			}

			public Path GetConfFile()
			{
				return configFile;
			}

			public IDictionary<JobACL, AccessControlList> GetJobACLs()
			{
				return jobACLs;
			}

			public IList<AMInfo> GetAMInfos()
			{
				IList<AMInfo> amInfoList = new List<AMInfo>();
				amInfoList.AddItem(MockJobs.CreateAMInfo(1));
				amInfoList.AddItem(MockJobs.CreateAMInfo(2));
				return amInfoList;
			}

			/// <exception cref="System.IO.IOException"/>
			public Configuration LoadConfFile()
			{
				FileContext fc = FileContext.GetFileContext(configFile.ToUri(), conf);
				Configuration jobConf = new Configuration(false);
				jobConf.AddResource(fc.Open(configFile), configFile.ToString());
				return jobConf;
			}

			public void SetQueueName(string queueName)
			{
			}

			private readonly JobId id;

			private readonly string name;

			private readonly JobReport report;

			private readonly Counters counters;

			private readonly IDictionary<TaskId, Task> tasks;

			private readonly MockJobs.TaskCount taskCount;

			private readonly Path configFile;

			private readonly IDictionary<JobACL, AccessControlList> jobACLs;

			private readonly Configuration conf;
		}

		// do nothing
		private static AMInfo CreateAMInfo(int attempt)
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(100, 1), attempt);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			return MRBuilderUtils.NewAMInfo(appAttemptId, Runtime.CurrentTimeMillis(), containerId
				, NmHost, NmPort, NmHttpPort);
		}
	}
}
