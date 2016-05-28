using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class MapTaskStatus : TaskStatus
	{
		private long mapFinishTime = 0;

		public MapTaskStatus()
		{
		}

		public MapTaskStatus(TaskAttemptID taskid, float progress, int numSlots, TaskStatus.State
			 runState, string diagnosticInfo, string stateString, string taskTracker, TaskStatus.Phase
			 phase, Counters counters)
			: base(taskid, progress, numSlots, runState, diagnosticInfo, stateString, taskTracker
				, phase, counters)
		{
		}

		public override bool GetIsMap()
		{
			return true;
		}

		/// <summary>Sets finishTime.</summary>
		/// <param name="finishTime">finish time of task.</param>
		internal override void SetFinishTime(long finishTime)
		{
			base.SetFinishTime(finishTime);
			// set mapFinishTime if it hasn't been set before
			if (GetMapFinishTime() == 0)
			{
				SetMapFinishTime(finishTime);
			}
		}

		public override long GetShuffleFinishTime()
		{
			throw new NotSupportedException("getShuffleFinishTime() not supported for MapTask"
				);
		}

		internal override void SetShuffleFinishTime(long shuffleFinishTime)
		{
			throw new NotSupportedException("setShuffleFinishTime() not supported for MapTask"
				);
		}

		public override long GetMapFinishTime()
		{
			return mapFinishTime;
		}

		internal override void SetMapFinishTime(long mapFinishTime)
		{
			this.mapFinishTime = mapFinishTime;
		}

		internal override void StatusUpdate(TaskStatus status)
		{
			lock (this)
			{
				base.StatusUpdate(status);
				if (status.GetMapFinishTime() != 0)
				{
					this.mapFinishTime = status.GetMapFinishTime();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			mapFinishTime = @in.ReadLong();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			@out.WriteLong(mapFinishTime);
		}

		public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
		{
			throw new NotSupportedException("addFetchFailedMap() not supported for MapTask");
		}
	}
}
