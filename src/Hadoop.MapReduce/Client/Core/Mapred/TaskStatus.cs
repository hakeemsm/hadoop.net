using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Describes the current status of a task.</summary>
	/// <remarks>
	/// Describes the current status of a task.  This is
	/// not intended to be a comprehensive piece of data.
	/// </remarks>
	public abstract class TaskStatus : Writable, ICloneable
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TaskStatus
			).FullName);

		public enum Phase
		{
			Starting,
			Map,
			Shuffle,
			Sort,
			Reduce,
			Cleanup
		}

		public enum State
		{
			Running,
			Succeeded,
			Failed,
			Unassigned,
			Killed,
			CommitPending,
			FailedUnclean,
			KilledUnclean
		}

		private readonly TaskAttemptID taskid;

		private float progress;

		private volatile TaskStatus.State runState;

		private string diagnosticInfo;

		private string stateString;

		private string taskTracker;

		private int numSlots;

		private long startTime;

		private long finishTime;

		private long outputSize = -1L;

		private volatile TaskStatus.Phase phase = TaskStatus.Phase.Starting;

		private Counters counters;

		private bool includeAllCounters;

		private SortedRanges.Range nextRecordRange = new SortedRanges.Range();

		internal const int MaxStringSize = 1024;

		//enumeration for reporting current phase of a task.
		// what state is the task in?
		//in ms
		// max task-status string size
		/// <summary>
		/// Testcases can override
		/// <see cref="GetMaxStringSize()"/>
		/// to control the max-size
		/// of strings in
		/// <see cref="TaskStatus"/>
		/// . Note that the
		/// <see cref="TaskStatus"/>
		/// is never
		/// exposed to clients or users (i.e Map or Reduce) and hence users cannot
		/// override this api to pass large strings in
		/// <see cref="TaskStatus"/>
		/// .
		/// </summary>
		protected internal virtual int GetMaxStringSize()
		{
			return MaxStringSize;
		}

		public TaskStatus()
		{
			taskid = new TaskAttemptID();
			numSlots = 0;
		}

		public TaskStatus(TaskAttemptID taskid, float progress, int numSlots, TaskStatus.State
			 runState, string diagnosticInfo, string stateString, string taskTracker, TaskStatus.Phase
			 phase, Counters counters)
		{
			this.taskid = taskid;
			this.progress = progress;
			this.numSlots = numSlots;
			this.runState = runState;
			SetDiagnosticInfo(diagnosticInfo);
			SetStateString(stateString);
			this.taskTracker = taskTracker;
			this.phase = phase;
			this.counters = counters;
			this.includeAllCounters = true;
		}

		public virtual TaskAttemptID GetTaskID()
		{
			return taskid;
		}

		public abstract bool GetIsMap();

		public virtual int GetNumSlots()
		{
			return numSlots;
		}

		public virtual float GetProgress()
		{
			return progress;
		}

		public virtual void SetProgress(float progress)
		{
			this.progress = progress;
		}

		public virtual TaskStatus.State GetRunState()
		{
			return runState;
		}

		public virtual string GetTaskTracker()
		{
			return taskTracker;
		}

		public virtual void SetTaskTracker(string tracker)
		{
			this.taskTracker = tracker;
		}

		public virtual void SetRunState(TaskStatus.State runState)
		{
			this.runState = runState;
		}

		public virtual string GetDiagnosticInfo()
		{
			return diagnosticInfo;
		}

		public virtual void SetDiagnosticInfo(string info)
		{
			// if the diag-info has already reached its max then log and return
			if (diagnosticInfo != null && diagnosticInfo.Length == GetMaxStringSize())
			{
				Log.Info("task-diagnostic-info for task " + taskid + " : " + info);
				return;
			}
			diagnosticInfo = ((diagnosticInfo == null) ? info : diagnosticInfo.Concat(info));
			// trim the string to MAX_STRING_SIZE if needed
			if (diagnosticInfo != null && diagnosticInfo.Length > GetMaxStringSize())
			{
				Log.Info("task-diagnostic-info for task " + taskid + " : " + diagnosticInfo);
				diagnosticInfo = Sharpen.Runtime.Substring(diagnosticInfo, 0, GetMaxStringSize());
			}
		}

		public virtual string GetStateString()
		{
			return stateString;
		}

		/// <summary>
		/// Set the state of the
		/// <see cref="TaskStatus"/>
		/// .
		/// </summary>
		public virtual void SetStateString(string stateString)
		{
			if (stateString != null)
			{
				if (stateString.Length <= GetMaxStringSize())
				{
					this.stateString = stateString;
				}
				else
				{
					// log it
					Log.Info("state-string for task " + taskid + " : " + stateString);
					// trim the state string
					this.stateString = Sharpen.Runtime.Substring(stateString, 0, GetMaxStringSize());
				}
			}
		}

		/// <summary>Get the next record range which is going to be processed by Task.</summary>
		/// <returns>nextRecordRange</returns>
		public virtual SortedRanges.Range GetNextRecordRange()
		{
			return nextRecordRange;
		}

		/// <summary>Set the next record range which is going to be processed by Task.</summary>
		/// <param name="nextRecordRange"/>
		public virtual void SetNextRecordRange(SortedRanges.Range nextRecordRange)
		{
			this.nextRecordRange = nextRecordRange;
		}

		/// <summary>Get task finish time.</summary>
		/// <remarks>
		/// Get task finish time. if shuffleFinishTime and sortFinishTime
		/// are not set before, these are set to finishTime. It takes care of
		/// the case when shuffle, sort and finish are completed with in the
		/// heartbeat interval and are not reported separately. if task state is
		/// TaskStatus.FAILED then finish time represents when the task failed.
		/// </remarks>
		/// <returns>finish time of the task.</returns>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>
		/// Sets finishTime for the task status if and only if the
		/// start time is set and passed finish time is greater than
		/// zero.
		/// </summary>
		/// <param name="finishTime">finish time of task.</param>
		internal virtual void SetFinishTime(long finishTime)
		{
			if (this.GetStartTime() > 0 && finishTime > 0)
			{
				this.finishTime = finishTime;
			}
			else
			{
				//Using String utils to get the stack trace.
				Log.Error("Trying to set finish time for task " + taskid + " when no start time is set, stackTrace is : "
					 + StringUtils.StringifyException(new Exception()));
			}
		}

		/// <summary>Get shuffle finish time for the task.</summary>
		/// <remarks>
		/// Get shuffle finish time for the task. If shuffle finish time was
		/// not set due to shuffle/sort/finish phases ending within same
		/// heartbeat interval, it is set to finish time of next phase i.e. sort
		/// or task finish when these are set.
		/// </remarks>
		/// <returns>
		/// 0 if shuffleFinishTime, sortFinishTime and finish time are not set. else
		/// it returns approximate shuffle finish time.
		/// </returns>
		public virtual long GetShuffleFinishTime()
		{
			return 0;
		}

		/// <summary>Set shuffle finish time.</summary>
		/// <param name="shuffleFinishTime"></param>
		internal virtual void SetShuffleFinishTime(long shuffleFinishTime)
		{
		}

		/// <summary>Get map phase finish time for the task.</summary>
		/// <remarks>
		/// Get map phase finish time for the task. If map finsh time was
		/// not set due to sort phase ending within same heartbeat interval,
		/// it is set to finish time of next phase i.e. sort phase
		/// when it is set.
		/// </remarks>
		/// <returns>
		/// 0 if mapFinishTime, sortFinishTime are not set. else
		/// it returns approximate map finish time.
		/// </returns>
		public virtual long GetMapFinishTime()
		{
			return 0;
		}

		/// <summary>Set map phase finish time.</summary>
		/// <param name="mapFinishTime"></param>
		internal virtual void SetMapFinishTime(long mapFinishTime)
		{
		}

		/// <summary>Get sort finish time for the task,.</summary>
		/// <remarks>
		/// Get sort finish time for the task,. If sort finish time was not set
		/// due to sort and reduce phase finishing in same heartebat interval, it is
		/// set to finish time, when finish time is set.
		/// </remarks>
		/// <returns>
		/// 0 if sort finish time and finish time are not set, else returns sort
		/// finish time if that is set, else it returns finish time.
		/// </returns>
		public virtual long GetSortFinishTime()
		{
			return 0;
		}

		/// <summary>
		/// Sets sortFinishTime, if shuffleFinishTime is not set before
		/// then its set to sortFinishTime.
		/// </summary>
		/// <param name="sortFinishTime"/>
		internal virtual void SetSortFinishTime(long sortFinishTime)
		{
		}

		/// <summary>Get start time of the task.</summary>
		/// <returns>0 is start time is not set, else returns start time.</returns>
		public virtual long GetStartTime()
		{
			return startTime;
		}

		/// <summary>Set startTime of the task if start time is greater than zero.</summary>
		/// <param name="startTime">start time</param>
		internal virtual void SetStartTime(long startTime)
		{
			//Making the assumption of passed startTime to be a positive
			//long value explicit.
			if (startTime > 0)
			{
				this.startTime = startTime;
			}
			else
			{
				//Using String utils to get the stack trace.
				Log.Error("Trying to set illegal startTime for task : " + taskid + ".Stack trace is : "
					 + StringUtils.StringifyException(new Exception()));
			}
		}

		/// <summary>Get current phase of this task.</summary>
		/// <remarks>
		/// Get current phase of this task. Phase.Map in case of map tasks,
		/// for reduce one of Phase.SHUFFLE, Phase.SORT or Phase.REDUCE.
		/// </remarks>
		/// <returns>.</returns>
		public virtual TaskStatus.Phase GetPhase()
		{
			return this.phase;
		}

		/// <summary>Set current phase of this task.</summary>
		/// <param name="phase">phase of this task</param>
		public virtual void SetPhase(TaskStatus.Phase phase)
		{
			TaskStatus.Phase oldPhase = GetPhase();
			if (oldPhase != phase)
			{
				// sort phase started
				if (phase == TaskStatus.Phase.Sort)
				{
					if (oldPhase == TaskStatus.Phase.Map)
					{
						SetMapFinishTime(Runtime.CurrentTimeMillis());
					}
					else
					{
						SetShuffleFinishTime(Runtime.CurrentTimeMillis());
					}
				}
				else
				{
					if (phase == TaskStatus.Phase.Reduce)
					{
						SetSortFinishTime(Runtime.CurrentTimeMillis());
					}
				}
				this.phase = phase;
			}
		}

		internal virtual bool InTaskCleanupPhase()
		{
			return (this.phase == TaskStatus.Phase.Cleanup && (this.runState == TaskStatus.State
				.FailedUnclean || this.runState == TaskStatus.State.KilledUnclean));
		}

		public virtual bool GetIncludeAllCounters()
		{
			return includeAllCounters;
		}

		public virtual void SetIncludeAllCounters(bool send)
		{
			includeAllCounters = send;
			counters.SetWriteAllCounters(send);
		}

		/// <summary>Get task's counters.</summary>
		public virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Set the task's counters.</summary>
		/// <param name="counters"/>
		public virtual void SetCounters(Counters counters)
		{
			this.counters = counters;
		}

		/// <summary>Returns the number of bytes of output from this map.</summary>
		public virtual long GetOutputSize()
		{
			return outputSize;
		}

		/// <summary>Set the size on disk of this task's output.</summary>
		/// <param name="l">the number of map output bytes</param>
		internal virtual void SetOutputSize(long l)
		{
			outputSize = l;
		}

		/// <summary>Get the list of maps from which output-fetches failed.</summary>
		/// <returns>the list of maps from which output-fetches failed.</returns>
		public virtual IList<TaskAttemptID> GetFetchFailedMaps()
		{
			return null;
		}

		/// <summary>Add to the list of maps from which output-fetches failed.</summary>
		/// <param name="mapTaskId">map from which fetch failed</param>
		public abstract void AddFetchFailedMap(TaskAttemptID mapTaskId);

		/// <summary>Update the status of the task.</summary>
		/// <remarks>
		/// Update the status of the task.
		/// This update is done by ping thread before sending the status.
		/// </remarks>
		/// <param name="progress"/>
		/// <param name="state"/>
		/// <param name="counters"/>
		internal virtual void StatusUpdate(float progress, string state, Counters counters
			)
		{
			lock (this)
			{
				SetProgress(progress);
				SetStateString(state);
				SetCounters(counters);
			}
		}

		/// <summary>Update the status of the task.</summary>
		/// <param name="status">updated status</param>
		internal virtual void StatusUpdate(Org.Apache.Hadoop.Mapred.TaskStatus status)
		{
			lock (this)
			{
				SetProgress(status.GetProgress());
				this.runState = status.GetRunState();
				SetStateString(status.GetStateString());
				this.nextRecordRange = status.GetNextRecordRange();
				SetDiagnosticInfo(status.GetDiagnosticInfo());
				if (status.GetStartTime() > 0)
				{
					this.SetStartTime(status.GetStartTime());
				}
				if (status.GetFinishTime() > 0)
				{
					this.SetFinishTime(status.GetFinishTime());
				}
				this.phase = status.GetPhase();
				this.counters = status.GetCounters();
				this.outputSize = status.outputSize;
			}
		}

		/// <summary>
		/// Update specific fields of task status
		/// This update is done in JobTracker when a cleanup attempt of task
		/// reports its status.
		/// </summary>
		/// <remarks>
		/// Update specific fields of task status
		/// This update is done in JobTracker when a cleanup attempt of task
		/// reports its status. Then update only specific fields, not all.
		/// </remarks>
		/// <param name="runState"/>
		/// <param name="progress"/>
		/// <param name="state"/>
		/// <param name="phase"/>
		/// <param name="finishTime"/>
		internal virtual void StatusUpdate(TaskStatus.State runState, float progress, string
			 state, TaskStatus.Phase phase, long finishTime)
		{
			lock (this)
			{
				SetRunState(runState);
				SetProgress(progress);
				SetStateString(state);
				SetPhase(phase);
				if (finishTime > 0)
				{
					SetFinishTime(finishTime);
				}
			}
		}

		/// <summary>
		/// Clear out transient information after sending out a status-update
		/// from either the
		/// <see cref="Task"/>
		/// to the
		/// <see cref="TaskTracker"/>
		/// or from the
		/// <see cref="TaskTracker"/>
		/// to the
		/// <see cref="JobTracker"/>
		/// .
		/// </summary>
		internal virtual void ClearStatus()
		{
			lock (this)
			{
				// Clear diagnosticInfo
				diagnosticInfo = string.Empty;
			}
		}

		public virtual object Clone()
		{
			return base.MemberwiseClone();
		}

		// Shouldn't happen since we do implement Clonable
		//////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			taskid.Write(@out);
			@out.WriteFloat(progress);
			@out.WriteInt(numSlots);
			WritableUtils.WriteEnum(@out, runState);
			Text.WriteString(@out, diagnosticInfo);
			Text.WriteString(@out, stateString);
			WritableUtils.WriteEnum(@out, phase);
			@out.WriteLong(startTime);
			@out.WriteLong(finishTime);
			@out.WriteBoolean(includeAllCounters);
			@out.WriteLong(outputSize);
			counters.Write(@out);
			nextRecordRange.Write(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			this.taskid.ReadFields(@in);
			SetProgress(@in.ReadFloat());
			this.numSlots = @in.ReadInt();
			this.runState = WritableUtils.ReadEnum<TaskStatus.State>(@in);
			SetDiagnosticInfo(StringInterner.WeakIntern(Text.ReadString(@in)));
			SetStateString(StringInterner.WeakIntern(Text.ReadString(@in)));
			this.phase = WritableUtils.ReadEnum<TaskStatus.Phase>(@in);
			this.startTime = @in.ReadLong();
			this.finishTime = @in.ReadLong();
			counters = new Counters();
			this.includeAllCounters = @in.ReadBoolean();
			this.outputSize = @in.ReadLong();
			counters.ReadFields(@in);
			nextRecordRange.ReadFields(@in);
		}

		//////////////////////////////////////////////////////////////////////////////
		// Factory-like methods to create/read/write appropriate TaskStatus objects
		//////////////////////////////////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Mapred.TaskStatus CreateTaskStatus(DataInput @in
			, TaskAttemptID taskId, float progress, int numSlots, TaskStatus.State runState, 
			string diagnosticInfo, string stateString, string taskTracker, TaskStatus.Phase 
			phase, Counters counters)
		{
			bool isMap = @in.ReadBoolean();
			return CreateTaskStatus(isMap, taskId, progress, numSlots, runState, diagnosticInfo
				, stateString, taskTracker, phase, counters);
		}

		internal static Org.Apache.Hadoop.Mapred.TaskStatus CreateTaskStatus(bool isMap, 
			TaskAttemptID taskId, float progress, int numSlots, TaskStatus.State runState, string
			 diagnosticInfo, string stateString, string taskTracker, TaskStatus.Phase phase, 
			Counters counters)
		{
			return (isMap) ? new MapTaskStatus(taskId, progress, numSlots, runState, diagnosticInfo
				, stateString, taskTracker, phase, counters) : new ReduceTaskStatus(taskId, progress
				, numSlots, runState, diagnosticInfo, stateString, taskTracker, phase, counters);
		}

		internal static Org.Apache.Hadoop.Mapred.TaskStatus CreateTaskStatus(bool isMap)
		{
			return (isMap) ? new MapTaskStatus() : new ReduceTaskStatus();
		}
	}
}
