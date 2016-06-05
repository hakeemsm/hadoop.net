using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class CompletedTaskAttempt : TaskAttempt
	{
		private readonly JobHistoryParser.TaskAttemptInfo attemptInfo;

		private readonly TaskAttemptId attemptId;

		private readonly TaskAttemptState state;

		private readonly IList<string> diagnostics = new AList<string>();

		private TaskAttemptReport report;

		private string localDiagMessage;

		internal CompletedTaskAttempt(TaskId taskId, JobHistoryParser.TaskAttemptInfo attemptInfo
			)
		{
			this.attemptInfo = attemptInfo;
			this.attemptId = TypeConverter.ToYarn(attemptInfo.GetAttemptId());
			if (attemptInfo.GetTaskStatus() != null)
			{
				this.state = TaskAttemptState.ValueOf(attemptInfo.GetTaskStatus());
			}
			else
			{
				this.state = TaskAttemptState.Killed;
				localDiagMessage = "Attmpt state missing from History : marked as KILLED";
				diagnostics.AddItem(localDiagMessage);
			}
			if (attemptInfo.GetError() != null)
			{
				diagnostics.AddItem(attemptInfo.GetError());
			}
		}

		/// <exception cref="System.NotSupportedException"/>
		public virtual NodeId GetNodeId()
		{
			throw new NotSupportedException();
		}

		public virtual ContainerId GetAssignedContainerID()
		{
			return attemptInfo.GetContainerId();
		}

		public virtual string GetAssignedContainerMgrAddress()
		{
			return attemptInfo.GetHostname() + ":" + attemptInfo.GetPort();
		}

		public virtual string GetNodeHttpAddress()
		{
			return attemptInfo.GetTrackerName() + ":" + attemptInfo.GetHttpPort();
		}

		public virtual string GetNodeRackName()
		{
			return attemptInfo.GetRackname();
		}

		public virtual Counters GetCounters()
		{
			return attemptInfo.GetCounters();
		}

		public virtual TaskAttemptId GetID()
		{
			return attemptId;
		}

		public virtual float GetProgress()
		{
			return 1.0f;
		}

		public virtual TaskAttemptReport GetReport()
		{
			lock (this)
			{
				if (report == null)
				{
					ConstructTaskAttemptReport();
				}
				return report;
			}
		}

		public virtual Phase GetPhase()
		{
			return Phase.Cleanup;
		}

		public virtual TaskAttemptState GetState()
		{
			return state;
		}

		public virtual bool IsFinished()
		{
			return true;
		}

		public virtual IList<string> GetDiagnostics()
		{
			return diagnostics;
		}

		public virtual long GetLaunchTime()
		{
			return attemptInfo.GetStartTime();
		}

		public virtual long GetFinishTime()
		{
			return attemptInfo.GetFinishTime();
		}

		public virtual long GetShuffleFinishTime()
		{
			return attemptInfo.GetShuffleFinishTime();
		}

		public virtual long GetSortFinishTime()
		{
			return attemptInfo.GetSortFinishTime();
		}

		public virtual int GetShufflePort()
		{
			return attemptInfo.GetShufflePort();
		}

		private void ConstructTaskAttemptReport()
		{
			report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskAttemptReport>();
			report.SetTaskAttemptId(attemptId);
			report.SetTaskAttemptState(state);
			report.SetProgress(GetProgress());
			report.SetStartTime(attemptInfo.GetStartTime());
			report.SetFinishTime(attemptInfo.GetFinishTime());
			report.SetShuffleFinishTime(attemptInfo.GetShuffleFinishTime());
			report.SetSortFinishTime(attemptInfo.GetSortFinishTime());
			if (localDiagMessage != null)
			{
				report.SetDiagnosticInfo(attemptInfo.GetError() + ", " + localDiagMessage);
			}
			else
			{
				report.SetDiagnosticInfo(attemptInfo.GetError());
			}
			// report.setPhase(attemptInfo.get); //TODO
			report.SetStateString(attemptInfo.GetState());
			report.SetCounters(TypeConverter.ToYarn(GetCounters()));
			report.SetContainerId(attemptInfo.GetContainerId());
			if (attemptInfo.GetHostname() == null)
			{
				report.SetNodeManagerHost("UNKNOWN");
			}
			else
			{
				report.SetNodeManagerHost(attemptInfo.GetHostname());
				report.SetNodeManagerPort(attemptInfo.GetPort());
			}
			report.SetNodeManagerHttpPort(attemptInfo.GetHttpPort());
		}
	}
}
