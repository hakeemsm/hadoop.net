using System;
using System.Collections.Generic;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class ReduceTaskStatus : TaskStatus
	{
		private long shuffleFinishTime;

		private long sortFinishTime;

		private IList<TaskAttemptID> failedFetchTasks = new AList<TaskAttemptID>(1);

		public ReduceTaskStatus()
		{
		}

		public ReduceTaskStatus(TaskAttemptID taskid, float progress, int numSlots, TaskStatus.State
			 runState, string diagnosticInfo, string stateString, string taskTracker, TaskStatus.Phase
			 phase, Counters counters)
			: base(taskid, progress, numSlots, runState, diagnosticInfo, stateString, taskTracker
				, phase, counters)
		{
		}

		public override object Clone()
		{
			Org.Apache.Hadoop.Mapred.ReduceTaskStatus myClone = (Org.Apache.Hadoop.Mapred.ReduceTaskStatus
				)base.Clone();
			myClone.failedFetchTasks = new AList<TaskAttemptID>(failedFetchTasks);
			return myClone;
		}

		public override bool GetIsMap()
		{
			return false;
		}

		internal override void SetFinishTime(long finishTime)
		{
			if (shuffleFinishTime == 0)
			{
				this.shuffleFinishTime = finishTime;
			}
			if (sortFinishTime == 0)
			{
				this.sortFinishTime = finishTime;
			}
			base.SetFinishTime(finishTime);
		}

		public override long GetShuffleFinishTime()
		{
			return shuffleFinishTime;
		}

		internal override void SetShuffleFinishTime(long shuffleFinishTime)
		{
			this.shuffleFinishTime = shuffleFinishTime;
		}

		public override long GetSortFinishTime()
		{
			return sortFinishTime;
		}

		internal override void SetSortFinishTime(long sortFinishTime)
		{
			this.sortFinishTime = sortFinishTime;
			if (0 == this.shuffleFinishTime)
			{
				this.shuffleFinishTime = sortFinishTime;
			}
		}

		public override long GetMapFinishTime()
		{
			throw new NotSupportedException("getMapFinishTime() not supported for ReduceTask"
				);
		}

		internal override void SetMapFinishTime(long shuffleFinishTime)
		{
			throw new NotSupportedException("setMapFinishTime() not supported for ReduceTask"
				);
		}

		public override IList<TaskAttemptID> GetFetchFailedMaps()
		{
			return failedFetchTasks;
		}

		public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
		{
			failedFetchTasks.AddItem(mapTaskId);
		}

		internal override void StatusUpdate(TaskStatus status)
		{
			lock (this)
			{
				base.StatusUpdate(status);
				if (status.GetShuffleFinishTime() != 0)
				{
					this.shuffleFinishTime = status.GetShuffleFinishTime();
				}
				if (status.GetSortFinishTime() != 0)
				{
					sortFinishTime = status.GetSortFinishTime();
				}
				IList<TaskAttemptID> newFetchFailedMaps = status.GetFetchFailedMaps();
				if (failedFetchTasks == null)
				{
					failedFetchTasks = newFetchFailedMaps;
				}
				else
				{
					if (newFetchFailedMaps != null)
					{
						Sharpen.Collections.AddAll(failedFetchTasks, newFetchFailedMaps);
					}
				}
			}
		}

		internal override void ClearStatus()
		{
			lock (this)
			{
				base.ClearStatus();
				failedFetchTasks.Clear();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			shuffleFinishTime = @in.ReadLong();
			sortFinishTime = @in.ReadLong();
			int noFailedFetchTasks = @in.ReadInt();
			failedFetchTasks = new AList<TaskAttemptID>(noFailedFetchTasks);
			for (int i = 0; i < noFailedFetchTasks; ++i)
			{
				TaskAttemptID id = new TaskAttemptID();
				id.ReadFields(@in);
				failedFetchTasks.AddItem(id);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			@out.WriteLong(shuffleFinishTime);
			@out.WriteLong(sortFinishTime);
			@out.WriteInt(failedFetchTasks.Count);
			foreach (TaskAttemptID taskId in failedFetchTasks)
			{
				taskId.Write(@out);
			}
		}
	}
}
