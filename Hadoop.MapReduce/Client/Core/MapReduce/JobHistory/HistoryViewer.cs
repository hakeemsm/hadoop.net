using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>HistoryViewer is used to parse and view the JobHistory files</summary>
	public class HistoryViewer
	{
		private static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss"
			);

		private FileSystem fs;

		private JobHistoryParser.JobInfo job;

		private string jobId;

		private bool printAll;

		/// <summary>Constructs the HistoryViewer object</summary>
		/// <param name="historyFile">The fully qualified Path of the History File</param>
		/// <param name="conf">The Configuration file</param>
		/// <param name="printAll">Toggle to print all status to only killed/failed status</param>
		/// <exception cref="System.IO.IOException"/>
		public HistoryViewer(string historyFile, Configuration conf, bool printAll)
		{
			this.printAll = printAll;
			string errorMsg = "Unable to initialize History Viewer";
			try
			{
				Path jobFile = new Path(historyFile);
				fs = jobFile.GetFileSystem(conf);
				string[] jobDetails = jobFile.GetName().Split("_");
				if (jobDetails.Length < 2)
				{
					// NOT a valid name
					System.Console.Error.WriteLine("Ignore unrecognized file: " + jobFile.GetName());
					throw new IOException(errorMsg);
				}
				JobHistoryParser parser = new JobHistoryParser(fs, jobFile);
				job = parser.Parse();
				jobId = job.GetJobId().ToString();
			}
			catch (Exception e)
			{
				throw new IOException(errorMsg, e);
			}
		}

		/// <summary>Print the job/task/attempt summary information</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Print()
		{
			PrintJobDetails();
			PrintTaskSummary();
			PrintJobAnalysis();
			PrintTasks(TaskType.JobSetup, TaskStatus.State.Failed.ToString());
			PrintTasks(TaskType.JobSetup, TaskStatus.State.Killed.ToString());
			PrintTasks(TaskType.Map, TaskStatus.State.Failed.ToString());
			PrintTasks(TaskType.Map, TaskStatus.State.Killed.ToString());
			PrintTasks(TaskType.Reduce, TaskStatus.State.Failed.ToString());
			PrintTasks(TaskType.Reduce, TaskStatus.State.Killed.ToString());
			PrintTasks(TaskType.JobCleanup, TaskStatus.State.Failed.ToString());
			PrintTasks(TaskType.JobCleanup, JobStatus.GetJobRunState(JobStatus.Killed));
			if (printAll)
			{
				PrintTasks(TaskType.JobSetup, TaskStatus.State.Succeeded.ToString());
				PrintTasks(TaskType.Map, TaskStatus.State.Succeeded.ToString());
				PrintTasks(TaskType.Reduce, TaskStatus.State.Succeeded.ToString());
				PrintTasks(TaskType.JobCleanup, TaskStatus.State.Succeeded.ToString());
				PrintAllTaskAttempts(TaskType.JobSetup);
				PrintAllTaskAttempts(TaskType.Map);
				PrintAllTaskAttempts(TaskType.Reduce);
				PrintAllTaskAttempts(TaskType.JobCleanup);
			}
			HistoryViewer.FilteredJob filter = new HistoryViewer.FilteredJob(job, TaskStatus.State
				.Failed.ToString());
			PrintFailedAttempts(filter);
			filter = new HistoryViewer.FilteredJob(job, TaskStatus.State.Killed.ToString());
			PrintFailedAttempts(filter);
		}

		private void PrintJobDetails()
		{
			StringBuilder jobDetails = new StringBuilder();
			jobDetails.Append("\nHadoop job: ").Append(job.GetJobId());
			jobDetails.Append("\n=====================================");
			jobDetails.Append("\nUser: ").Append(job.GetUsername());
			jobDetails.Append("\nJobName: ").Append(job.GetJobname());
			jobDetails.Append("\nJobConf: ").Append(job.GetJobConfPath());
			jobDetails.Append("\nSubmitted At: ").Append(StringUtils.GetFormattedTimeWithDiff
				(dateFormat, job.GetSubmitTime(), 0));
			jobDetails.Append("\nLaunched At: ").Append(StringUtils.GetFormattedTimeWithDiff(
				dateFormat, job.GetLaunchTime(), job.GetSubmitTime()));
			jobDetails.Append("\nFinished At: ").Append(StringUtils.GetFormattedTimeWithDiff(
				dateFormat, job.GetFinishTime(), job.GetLaunchTime()));
			jobDetails.Append("\nStatus: ").Append(((job.GetJobStatus() == null) ? "Incomplete"
				 : job.GetJobStatus()));
			PrintCounters(jobDetails, job.GetTotalCounters(), job.GetMapCounters(), job.GetReduceCounters
				());
			jobDetails.Append("\n");
			jobDetails.Append("\n=====================================");
			System.Console.Out.WriteLine(jobDetails.ToString());
		}

		private void PrintCounters(StringBuilder buff, Counters totalCounters, Counters mapCounters
			, Counters reduceCounters)
		{
			// Killed jobs might not have counters
			if (totalCounters == null)
			{
				return;
			}
			buff.Append("\nCounters: \n\n");
			buff.Append(string.Format("|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s|", "Group Name"
				, "Counter name", "Map Value", "Reduce Value", "Total Value"));
			buff.Append("\n------------------------------------------" + "---------------------------------------------"
				);
			foreach (string groupName in totalCounters.GetGroupNames())
			{
				CounterGroup totalGroup = totalCounters.GetGroup(groupName);
				CounterGroup mapGroup = mapCounters.GetGroup(groupName);
				CounterGroup reduceGroup = reduceCounters.GetGroup(groupName);
				Format @decimal = new DecimalFormat();
				IEnumerator<Counter> ctrItr = totalGroup.GetEnumerator();
				while (ctrItr.HasNext())
				{
					Counter counter = ctrItr.Next();
					string name = counter.GetName();
					string mapValue = @decimal.Format(mapGroup.FindCounter(name).GetValue());
					string reduceValue = @decimal.Format(reduceGroup.FindCounter(name).GetValue());
					string totalValue = @decimal.Format(counter.GetValue());
					buff.Append(string.Format("%n|%1$-30s|%2$-30s|%3$-10s|%4$-10s|%5$-10s", totalGroup
						.GetDisplayName(), counter.GetDisplayName(), mapValue, reduceValue, totalValue));
				}
			}
		}

		private void PrintAllTaskAttempts(TaskType taskType)
		{
			IDictionary<TaskID, JobHistoryParser.TaskInfo> tasks = job.GetAllTasks();
			StringBuilder taskList = new StringBuilder();
			taskList.Append("\n").Append(taskType);
			taskList.Append(" task list for ").Append(job.GetJobId());
			taskList.Append("\nTaskId\t\tStartTime");
			if (TaskType.Reduce.Equals(taskType))
			{
				taskList.Append("\tShuffleFinished\tSortFinished");
			}
			taskList.Append("\tFinishTime\tHostName\tError\tTaskLogs");
			taskList.Append("\n====================================================");
			System.Console.Out.WriteLine(taskList.ToString());
			foreach (JobHistoryParser.TaskInfo task in tasks.Values)
			{
				foreach (JobHistoryParser.TaskAttemptInfo attempt in task.GetAllTaskAttempts().Values)
				{
					if (taskType.Equals(task.GetTaskType()))
					{
						taskList.Length = 0;
						taskList.Append(attempt.GetAttemptId()).Append("\t");
						taskList.Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, attempt.GetStartTime
							(), 0)).Append("\t");
						if (TaskType.Reduce.Equals(taskType))
						{
							taskList.Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, attempt.GetShuffleFinishTime
								(), attempt.GetStartTime()));
							taskList.Append("\t");
							taskList.Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, attempt.GetSortFinishTime
								(), attempt.GetShuffleFinishTime()));
						}
						taskList.Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, attempt.GetFinishTime
							(), attempt.GetStartTime()));
						taskList.Append("\t");
						taskList.Append(attempt.GetHostname()).Append("\t");
						taskList.Append(attempt.GetError());
						string taskLogsUrl = GetTaskLogsUrl(WebAppUtils.GetHttpSchemePrefix(fs.GetConf())
							, attempt);
						taskList.Append(taskLogsUrl != null ? taskLogsUrl : "n/a");
						System.Console.Out.WriteLine(taskList.ToString());
					}
				}
			}
		}

		private void PrintTaskSummary()
		{
			HistoryViewer.SummarizedJob ts = new HistoryViewer.SummarizedJob(job);
			StringBuilder taskSummary = new StringBuilder();
			taskSummary.Append("\nTask Summary");
			taskSummary.Append("\n============================");
			taskSummary.Append("\nKind\tTotal\t");
			taskSummary.Append("Successful\tFailed\tKilled\tStartTime\tFinishTime");
			taskSummary.Append("\n");
			taskSummary.Append("\nSetup\t").Append(ts.totalSetups);
			taskSummary.Append("\t").Append(ts.numFinishedSetups);
			taskSummary.Append("\t\t").Append(ts.numFailedSetups);
			taskSummary.Append("\t").Append(ts.numKilledSetups);
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.setupStarted, 0));
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.setupFinished, ts.setupStarted));
			taskSummary.Append("\nMap\t").Append(ts.totalMaps);
			taskSummary.Append("\t").Append(job.GetFinishedMaps());
			taskSummary.Append("\t\t").Append(ts.numFailedMaps);
			taskSummary.Append("\t").Append(ts.numKilledMaps);
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.mapStarted, 0));
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.mapFinished, ts.mapStarted));
			taskSummary.Append("\nReduce\t").Append(ts.totalReduces);
			taskSummary.Append("\t").Append(job.GetFinishedReduces());
			taskSummary.Append("\t\t").Append(ts.numFailedReduces);
			taskSummary.Append("\t").Append(ts.numKilledReduces);
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.reduceStarted, 0));
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.reduceFinished, ts.reduceStarted));
			taskSummary.Append("\nCleanup\t").Append(ts.totalCleanups);
			taskSummary.Append("\t").Append(ts.numFinishedCleanups);
			taskSummary.Append("\t\t").Append(ts.numFailedCleanups);
			taskSummary.Append("\t").Append(ts.numKilledCleanups);
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.cleanupStarted, 0));
			taskSummary.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, 
				ts.cleanupFinished, ts.cleanupStarted));
			taskSummary.Append("\n============================\n");
			System.Console.Out.WriteLine(taskSummary.ToString());
		}

		private void PrintJobAnalysis()
		{
			if (!job.GetJobStatus().Equals(JobStatus.GetJobRunState(JobStatus.Succeeded)))
			{
				System.Console.Out.WriteLine("No Analysis available as job did not finish");
				return;
			}
			HistoryViewer.AnalyzedJob avg = new HistoryViewer.AnalyzedJob(job);
			System.Console.Out.WriteLine("\nAnalysis");
			System.Console.Out.WriteLine("=========");
			PrintAnalysis(avg.GetMapTasks(), cMap, "map", avg.GetAvgMapTime(), 10);
			PrintLast(avg.GetMapTasks(), "map", cFinishMapRed);
			if (avg.GetReduceTasks().Length > 0)
			{
				PrintAnalysis(avg.GetReduceTasks(), cShuffle, "shuffle", avg.GetAvgShuffleTime(), 
					10);
				PrintLast(avg.GetReduceTasks(), "shuffle", cFinishShuffle);
				PrintAnalysis(avg.GetReduceTasks(), cReduce, "reduce", avg.GetAvgReduceTime(), 10
					);
				PrintLast(avg.GetReduceTasks(), "reduce", cFinishMapRed);
			}
			System.Console.Out.WriteLine("=========");
		}

		private void PrintAnalysis(JobHistoryParser.TaskAttemptInfo[] tasks, IComparer<JobHistoryParser.TaskAttemptInfo
			> cmp, string taskType, long avg, int showTasks)
		{
			Arrays.Sort(tasks, cmp);
			JobHistoryParser.TaskAttemptInfo min = tasks[tasks.Length - 1];
			StringBuilder details = new StringBuilder();
			details.Append("\nTime taken by best performing ");
			details.Append(taskType).Append(" task ");
			details.Append(min.GetAttemptId().GetTaskID().ToString()).Append(": ");
			if ("map".Equals(taskType))
			{
				details.Append(StringUtils.FormatTimeDiff(min.GetFinishTime(), min.GetStartTime()
					));
			}
			else
			{
				if ("shuffle".Equals(taskType))
				{
					details.Append(StringUtils.FormatTimeDiff(min.GetShuffleFinishTime(), min.GetStartTime
						()));
				}
				else
				{
					details.Append(StringUtils.FormatTimeDiff(min.GetFinishTime(), min.GetShuffleFinishTime
						()));
				}
			}
			details.Append("\nAverage time taken by ");
			details.Append(taskType).Append(" tasks: ");
			details.Append(StringUtils.FormatTimeDiff(avg, 0));
			details.Append("\nWorse performing ");
			details.Append(taskType).Append(" tasks: ");
			details.Append("\nTaskId\t\tTimetaken");
			System.Console.Out.WriteLine(details.ToString());
			for (int i = 0; i < showTasks && i < tasks.Length; i++)
			{
				details.Length = 0;
				details.Append(tasks[i].GetAttemptId().GetTaskID()).Append(" ");
				if ("map".Equals(taskType))
				{
					details.Append(StringUtils.FormatTimeDiff(tasks[i].GetFinishTime(), tasks[i].GetStartTime
						()));
				}
				else
				{
					if ("shuffle".Equals(taskType))
					{
						details.Append(StringUtils.FormatTimeDiff(tasks[i].GetShuffleFinishTime(), tasks[
							i].GetStartTime()));
					}
					else
					{
						details.Append(StringUtils.FormatTimeDiff(tasks[i].GetFinishTime(), tasks[i].GetShuffleFinishTime
							()));
					}
				}
				System.Console.Out.WriteLine(details.ToString());
			}
		}

		private void PrintLast(JobHistoryParser.TaskAttemptInfo[] tasks, string taskType, 
			IComparer<JobHistoryParser.TaskAttemptInfo> cmp)
		{
			Arrays.Sort(tasks, cFinishMapRed);
			JobHistoryParser.TaskAttemptInfo last = tasks[0];
			StringBuilder lastBuf = new StringBuilder();
			lastBuf.Append("The last ").Append(taskType);
			lastBuf.Append(" task ").Append(last.GetAttemptId().GetTaskID());
			long finishTime;
			if ("shuffle".Equals(taskType))
			{
				finishTime = last.GetShuffleFinishTime();
			}
			else
			{
				finishTime = last.GetFinishTime();
			}
			lastBuf.Append(" finished at (relative to the Job launch time): ");
			lastBuf.Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, finishTime, job.GetLaunchTime
				()));
			System.Console.Out.WriteLine(lastBuf.ToString());
		}

		private void PrintTasks(TaskType taskType, string status)
		{
			IDictionary<TaskID, JobHistoryParser.TaskInfo> tasks = job.GetAllTasks();
			StringBuilder header = new StringBuilder();
			header.Append("\n").Append(status).Append(" ");
			header.Append(taskType).Append(" task list for ").Append(jobId);
			header.Append("\nTaskId\t\tStartTime\tFinishTime\tError");
			if (TaskType.Map.Equals(taskType))
			{
				header.Append("\tInputSplits");
			}
			header.Append("\n====================================================");
			StringBuilder taskList = new StringBuilder();
			foreach (JobHistoryParser.TaskInfo task in tasks.Values)
			{
				if (taskType.Equals(task.GetTaskType()) && (status.Equals(task.GetTaskStatus()) ||
					 Sharpen.Runtime.EqualsIgnoreCase(status, "ALL")))
				{
					taskList.Length = 0;
					taskList.Append(task.GetTaskId());
					taskList.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, task
						.GetStartTime(), 0));
					taskList.Append("\t").Append(StringUtils.GetFormattedTimeWithDiff(dateFormat, task
						.GetFinishTime(), task.GetStartTime()));
					taskList.Append("\t").Append(task.GetError());
					if (TaskType.Map.Equals(taskType))
					{
						taskList.Append("\t").Append(task.GetSplitLocations());
					}
					if (taskList != null)
					{
						System.Console.Out.WriteLine(header.ToString());
						System.Console.Out.WriteLine(taskList.ToString());
					}
				}
			}
		}

		private void PrintFailedAttempts(HistoryViewer.FilteredJob filteredJob)
		{
			IDictionary<string, ICollection<TaskID>> badNodes = filteredJob.GetFilteredMap();
			StringBuilder attempts = new StringBuilder();
			if (badNodes.Count > 0)
			{
				attempts.Append("\n").Append(filteredJob.GetFilter());
				attempts.Append(" task attempts by nodes");
				attempts.Append("\nHostname\tFailedTasks");
				attempts.Append("\n===============================");
				System.Console.Out.WriteLine(attempts.ToString());
				foreach (KeyValuePair<string, ICollection<TaskID>> entry in badNodes)
				{
					string node = entry.Key;
					ICollection<TaskID> failedTasks = entry.Value;
					attempts.Length = 0;
					attempts.Append(node).Append("\t");
					foreach (TaskID t in failedTasks)
					{
						attempts.Append(t).Append(", ");
					}
					System.Console.Out.WriteLine(attempts.ToString());
				}
			}
		}

		/// <summary>Return the TaskLogsUrl of a particular TaskAttempt</summary>
		/// <param name="attempt"/>
		/// <returns>
		/// the taskLogsUrl. null if http-port or tracker-name or
		/// task-attempt-id are unavailable.
		/// </returns>
		public static string GetTaskLogsUrl(string scheme, JobHistoryParser.TaskAttemptInfo
			 attempt)
		{
			if (attempt.GetHttpPort() == -1 || attempt.GetTrackerName().Equals(string.Empty) 
				|| attempt.GetAttemptId() == null)
			{
				return null;
			}
			string taskTrackerName = HostUtil.ConvertTrackerNameToHostName(attempt.GetTrackerName
				());
			return HostUtil.GetTaskLogUrl(scheme, taskTrackerName, Sharpen.Extensions.ToString
				(attempt.GetHttpPort()), attempt.GetAttemptId().ToString());
		}

		private sealed class _IComparer_468 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_468()
			{
			}

			public int Compare(JobHistoryParser.TaskAttemptInfo t1, JobHistoryParser.TaskAttemptInfo
				 t2)
			{
				long l1 = t1.GetFinishTime() - t1.GetStartTime();
				long l2 = t2.GetFinishTime() - t2.GetStartTime();
				return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
			}
		}

		private IComparer<JobHistoryParser.TaskAttemptInfo> cMap = new _IComparer_468();

		private sealed class _IComparer_478 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_478()
			{
			}

			public int Compare(JobHistoryParser.TaskAttemptInfo t1, JobHistoryParser.TaskAttemptInfo
				 t2)
			{
				long l1 = t1.GetShuffleFinishTime() - t1.GetStartTime();
				long l2 = t2.GetShuffleFinishTime() - t2.GetStartTime();
				return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
			}
		}

		private IComparer<JobHistoryParser.TaskAttemptInfo> cShuffle = new _IComparer_478
			();

		private sealed class _IComparer_488 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_488()
			{
			}

			public int Compare(JobHistoryParser.TaskAttemptInfo t1, JobHistoryParser.TaskAttemptInfo
				 t2)
			{
				long l1 = t1.GetShuffleFinishTime();
				long l2 = t2.GetShuffleFinishTime();
				return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
			}
		}

		private IComparer<JobHistoryParser.TaskAttemptInfo> cFinishShuffle = new _IComparer_488
			();

		private sealed class _IComparer_498 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_498()
			{
			}

			public int Compare(JobHistoryParser.TaskAttemptInfo t1, JobHistoryParser.TaskAttemptInfo
				 t2)
			{
				long l1 = t1.GetFinishTime();
				long l2 = t2.GetFinishTime();
				return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
			}
		}

		private IComparer<JobHistoryParser.TaskAttemptInfo> cFinishMapRed = new _IComparer_498
			();

		private sealed class _IComparer_508 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_508()
			{
			}

			public int Compare(JobHistoryParser.TaskAttemptInfo t1, JobHistoryParser.TaskAttemptInfo
				 t2)
			{
				long l1 = t1.GetFinishTime() - t1.GetShuffleFinishTime();
				long l2 = t2.GetFinishTime() - t2.GetShuffleFinishTime();
				return (l2 < l1 ? -1 : (l2 == l1 ? 0 : 1));
			}
		}

		private IComparer<JobHistoryParser.TaskAttemptInfo> cReduce = new _IComparer_508(
			);

		/// <summary>Utility class used the summarize the job.</summary>
		/// <remarks>
		/// Utility class used the summarize the job.
		/// Used by HistoryViewer and the JobHistory UI.
		/// </remarks>
		public class SummarizedJob
		{
			internal IDictionary<TaskID, JobHistoryParser.TaskInfo> tasks;

			internal int totalMaps = 0;

			internal int totalReduces = 0;

			internal int totalCleanups = 0;

			internal int totalSetups = 0;

			internal int numFailedMaps = 0;

			internal int numKilledMaps = 0;

			internal int numFailedReduces = 0;

			internal int numKilledReduces = 0;

			internal int numFinishedCleanups = 0;

			internal int numFailedCleanups = 0;

			internal int numKilledCleanups = 0;

			internal int numFinishedSetups = 0;

			internal int numFailedSetups = 0;

			internal int numKilledSetups = 0;

			internal long mapStarted = 0;

			internal long mapFinished = 0;

			internal long reduceStarted = 0;

			internal long reduceFinished = 0;

			internal long cleanupStarted = 0;

			internal long cleanupFinished = 0;

			internal long setupStarted = 0;

			internal long setupFinished = 0;

			/// <summary>Get total maps</summary>
			public virtual int GetTotalMaps()
			{
				return totalMaps;
			}

			/// <summary>Get total reduces</summary>
			public virtual int GetTotalReduces()
			{
				return totalReduces;
			}

			/// <summary>Get number of clean up tasks</summary>
			public virtual int GetTotalCleanups()
			{
				return totalCleanups;
			}

			/// <summary>Get number of set up tasks</summary>
			public virtual int GetTotalSetups()
			{
				return totalSetups;
			}

			/// <summary>Get number of failed maps</summary>
			public virtual int GetNumFailedMaps()
			{
				return numFailedMaps;
			}

			/// <summary>Get number of killed maps</summary>
			public virtual int GetNumKilledMaps()
			{
				return numKilledMaps;
			}

			/// <summary>Get number of failed reduces</summary>
			public virtual int GetNumFailedReduces()
			{
				return numFailedReduces;
			}

			/// <summary>Get number of killed reduces</summary>
			public virtual int GetNumKilledReduces()
			{
				return numKilledReduces;
			}

			/// <summary>Get number of cleanup tasks that finished</summary>
			public virtual int GetNumFinishedCleanups()
			{
				return numFinishedCleanups;
			}

			/// <summary>Get number of failed cleanup tasks</summary>
			public virtual int GetNumFailedCleanups()
			{
				return numFailedCleanups;
			}

			/// <summary>Get number of killed cleanup tasks</summary>
			public virtual int GetNumKilledCleanups()
			{
				return numKilledCleanups;
			}

			/// <summary>Get number of finished set up tasks</summary>
			public virtual int GetNumFinishedSetups()
			{
				return numFinishedSetups;
			}

			/// <summary>Get number of failed set up tasks</summary>
			public virtual int GetNumFailedSetups()
			{
				return numFailedSetups;
			}

			/// <summary>Get number of killed set up tasks</summary>
			public virtual int GetNumKilledSetups()
			{
				return numKilledSetups;
			}

			/// <summary>Get number of maps that were started</summary>
			public virtual long GetMapStarted()
			{
				return mapStarted;
			}

			/// <summary>Get number of maps that finished</summary>
			public virtual long GetMapFinished()
			{
				return mapFinished;
			}

			/// <summary>Get number of Reducers that were started</summary>
			public virtual long GetReduceStarted()
			{
				return reduceStarted;
			}

			/// <summary>Get number of reducers that finished</summary>
			public virtual long GetReduceFinished()
			{
				return reduceFinished;
			}

			/// <summary>Get number of cleanup tasks started</summary>
			public virtual long GetCleanupStarted()
			{
				return cleanupStarted;
			}

			/// <summary>Get number of cleanup tasks that finished</summary>
			public virtual long GetCleanupFinished()
			{
				return cleanupFinished;
			}

			/// <summary>Get number of setup tasks that started</summary>
			public virtual long GetSetupStarted()
			{
				return setupStarted;
			}

			/// <summary>Get number of setup tasks that finished</summary>
			public virtual long GetSetupFinished()
			{
				return setupFinished;
			}

			/// <summary>Create summary information for the parsed job</summary>
			public SummarizedJob(JobHistoryParser.JobInfo job)
			{
				tasks = job.GetAllTasks();
				foreach (JobHistoryParser.TaskInfo task in tasks.Values)
				{
					IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts = task.GetAllTaskAttempts
						();
					//allHosts.put(task.getHo(Keys.HOSTNAME), "");
					foreach (JobHistoryParser.TaskAttemptInfo attempt in attempts.Values)
					{
						long startTime = attempt.GetStartTime();
						long finishTime = attempt.GetFinishTime();
						if (attempt.GetTaskType().Equals(TaskType.Map))
						{
							if (mapStarted == 0 || mapStarted > startTime)
							{
								mapStarted = startTime;
							}
							if (mapFinished < finishTime)
							{
								mapFinished = finishTime;
							}
							totalMaps++;
							if (attempt.GetTaskStatus().Equals(TaskStatus.State.Failed.ToString()))
							{
								numFailedMaps++;
							}
							else
							{
								if (attempt.GetTaskStatus().Equals(TaskStatus.State.Killed.ToString()))
								{
									numKilledMaps++;
								}
							}
						}
						else
						{
							if (attempt.GetTaskType().Equals(TaskType.Reduce))
							{
								if (reduceStarted == 0 || reduceStarted > startTime)
								{
									reduceStarted = startTime;
								}
								if (reduceFinished < finishTime)
								{
									reduceFinished = finishTime;
								}
								totalReduces++;
								if (attempt.GetTaskStatus().Equals(TaskStatus.State.Failed.ToString()))
								{
									numFailedReduces++;
								}
								else
								{
									if (attempt.GetTaskStatus().Equals(TaskStatus.State.Killed.ToString()))
									{
										numKilledReduces++;
									}
								}
							}
							else
							{
								if (attempt.GetTaskType().Equals(TaskType.JobCleanup))
								{
									if (cleanupStarted == 0 || cleanupStarted > startTime)
									{
										cleanupStarted = startTime;
									}
									if (cleanupFinished < finishTime)
									{
										cleanupFinished = finishTime;
									}
									totalCleanups++;
									if (attempt.GetTaskStatus().Equals(TaskStatus.State.Succeeded.ToString()))
									{
										numFinishedCleanups++;
									}
									else
									{
										if (attempt.GetTaskStatus().Equals(TaskStatus.State.Failed.ToString()))
										{
											numFailedCleanups++;
										}
										else
										{
											if (attempt.GetTaskStatus().Equals(TaskStatus.State.Killed.ToString()))
											{
												numKilledCleanups++;
											}
										}
									}
								}
								else
								{
									if (attempt.GetTaskType().Equals(TaskType.JobSetup))
									{
										if (setupStarted == 0 || setupStarted > startTime)
										{
											setupStarted = startTime;
										}
										if (setupFinished < finishTime)
										{
											setupFinished = finishTime;
										}
										totalSetups++;
										if (attempt.GetTaskStatus().Equals(TaskStatus.State.Succeeded.ToString()))
										{
											numFinishedSetups++;
										}
										else
										{
											if (attempt.GetTaskStatus().Equals(TaskStatus.State.Failed.ToString()))
											{
												numFailedSetups++;
											}
											else
											{
												if (attempt.GetTaskStatus().Equals(TaskStatus.State.Killed.ToString()))
												{
													numKilledSetups++;
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <summary>Utility class used while analyzing the job.</summary>
		/// <remarks>
		/// Utility class used while analyzing the job.
		/// Used by HistoryViewer and the JobHistory UI.
		/// </remarks>
		public class AnalyzedJob
		{
			private long avgMapTime;

			private long avgReduceTime;

			private long avgShuffleTime;

			private JobHistoryParser.TaskAttemptInfo[] mapTasks;

			private JobHistoryParser.TaskAttemptInfo[] reduceTasks;

			/// <summary>Get the average map time</summary>
			public virtual long GetAvgMapTime()
			{
				return avgMapTime;
			}

			/// <summary>Get the average reduce time</summary>
			public virtual long GetAvgReduceTime()
			{
				return avgReduceTime;
			}

			/// <summary>Get the average shuffle time</summary>
			public virtual long GetAvgShuffleTime()
			{
				return avgShuffleTime;
			}

			/// <summary>Get the map tasks list</summary>
			public virtual JobHistoryParser.TaskAttemptInfo[] GetMapTasks()
			{
				return mapTasks;
			}

			/// <summary>Get the reduce tasks list</summary>
			public virtual JobHistoryParser.TaskAttemptInfo[] GetReduceTasks()
			{
				return reduceTasks;
			}

			/// <summary>Generate analysis information for the parsed job</summary>
			public AnalyzedJob(JobHistoryParser.JobInfo job)
			{
				IDictionary<TaskID, JobHistoryParser.TaskInfo> tasks = job.GetAllTasks();
				int finishedMaps = (int)job.GetFinishedMaps();
				int finishedReduces = (int)job.GetFinishedReduces();
				mapTasks = new JobHistoryParser.TaskAttemptInfo[finishedMaps];
				reduceTasks = new JobHistoryParser.TaskAttemptInfo[finishedReduces];
				int mapIndex = 0;
				int reduceIndex = 0;
				avgMapTime = 0;
				avgReduceTime = 0;
				avgShuffleTime = 0;
				foreach (JobHistoryParser.TaskInfo task in tasks.Values)
				{
					IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts = task.GetAllTaskAttempts
						();
					foreach (JobHistoryParser.TaskAttemptInfo attempt in attempts.Values)
					{
						if (attempt.GetTaskStatus().Equals(TaskStatus.State.Succeeded.ToString()))
						{
							long avgFinishTime = (attempt.GetFinishTime() - attempt.GetStartTime());
							if (attempt.GetTaskType().Equals(TaskType.Map))
							{
								mapTasks[mapIndex++] = attempt;
								avgMapTime += avgFinishTime;
							}
							else
							{
								if (attempt.GetTaskType().Equals(TaskType.Reduce))
								{
									reduceTasks[reduceIndex++] = attempt;
									avgShuffleTime += (attempt.GetShuffleFinishTime() - attempt.GetStartTime());
									avgReduceTime += (attempt.GetFinishTime() - attempt.GetShuffleFinishTime());
								}
							}
							break;
						}
					}
				}
				if (finishedMaps > 0)
				{
					avgMapTime /= finishedMaps;
				}
				if (finishedReduces > 0)
				{
					avgReduceTime /= finishedReduces;
					avgShuffleTime /= finishedReduces;
				}
			}
		}

		/// <summary>Utility to filter out events based on the task status</summary>
		public class FilteredJob
		{
			private IDictionary<string, ICollection<TaskID>> badNodesToFilteredTasks = new Dictionary
				<string, ICollection<TaskID>>();

			private string filter;

			/// <summary>Get the map of the filtered tasks</summary>
			public virtual IDictionary<string, ICollection<TaskID>> GetFilteredMap()
			{
				return badNodesToFilteredTasks;
			}

			/// <summary>Get the current filter</summary>
			public virtual string GetFilter()
			{
				return filter;
			}

			/// <summary>Apply the filter (status) on the parsed job and generate summary</summary>
			public FilteredJob(JobHistoryParser.JobInfo job, string status)
			{
				filter = status;
				IDictionary<TaskID, JobHistoryParser.TaskInfo> tasks = job.GetAllTasks();
				foreach (JobHistoryParser.TaskInfo task in tasks.Values)
				{
					IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts = task.GetAllTaskAttempts
						();
					foreach (JobHistoryParser.TaskAttemptInfo attempt in attempts.Values)
					{
						if (attempt.GetTaskStatus().Equals(status))
						{
							string hostname = attempt.GetHostname();
							TaskID id = attempt.GetAttemptId().GetTaskID();
							ICollection<TaskID> set = badNodesToFilteredTasks[hostname];
							if (set == null)
							{
								set = new TreeSet<TaskID>();
								set.AddItem(id);
								badNodesToFilteredTasks[hostname] = set;
							}
							else
							{
								set.AddItem(id);
							}
						}
					}
				}
			}
		}
	}
}
