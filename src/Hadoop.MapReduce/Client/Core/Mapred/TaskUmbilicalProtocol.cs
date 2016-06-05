using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Protocol that task child process uses to contact its parent process.</summary>
	/// <remarks>
	/// Protocol that task child process uses to contact its parent process.  The
	/// parent is a daemon which which polls the central master for a new map or
	/// reduce task and runs it as a child process.  All communication between child
	/// and parent is via this protocol.
	/// </remarks>
	public abstract class TaskUmbilicalProtocol : VersionedProtocol
	{
		/// <summary>
		/// Changed the version to 2, since we have a new method getMapOutputs
		/// Changed version to 3 to have progress() return a boolean
		/// Changed the version to 4, since we have replaced
		/// TaskUmbilicalProtocol.progress(String, float, String,
		/// org.apache.hadoop.mapred.TaskStatus.Phase, Counters)
		/// with statusUpdate(String, TaskStatus)
		/// Version 5 changed counters representation for HADOOP-2248
		/// Version 6 changes the TaskStatus representation for HADOOP-2208
		/// Version 7 changes the done api (via HADOOP-3140).
		/// </summary>
		/// <remarks>
		/// Changed the version to 2, since we have a new method getMapOutputs
		/// Changed version to 3 to have progress() return a boolean
		/// Changed the version to 4, since we have replaced
		/// TaskUmbilicalProtocol.progress(String, float, String,
		/// org.apache.hadoop.mapred.TaskStatus.Phase, Counters)
		/// with statusUpdate(String, TaskStatus)
		/// Version 5 changed counters representation for HADOOP-2248
		/// Version 6 changes the TaskStatus representation for HADOOP-2208
		/// Version 7 changes the done api (via HADOOP-3140). It now expects whether
		/// or not the task's output needs to be promoted.
		/// Version 8 changes {job|tip|task}id's to use their corresponding
		/// objects rather than strings.
		/// Version 9 changes the counter representation for HADOOP-1915
		/// Version 10 changed the TaskStatus format and added reportNextRecordRange
		/// for HADOOP-153
		/// Version 11 Adds RPCs for task commit as part of HADOOP-3150
		/// Version 12 getMapCompletionEvents() now also indicates if the events are
		/// stale or not. Hence the return type is a class that
		/// encapsulates the events and whether to reset events index.
		/// Version 13 changed the getTask method signature for HADOOP-249
		/// Version 14 changed the getTask method signature for HADOOP-4232
		/// Version 15 Adds FAILED_UNCLEAN and KILLED_UNCLEAN states for HADOOP-4759
		/// Version 16 Change in signature of getTask() for HADOOP-5488
		/// Version 17 Modified TaskID to be aware of the new TaskTypes
		/// Version 18 Added numRequiredSlots to TaskStatus for MAPREDUCE-516
		/// Version 19 Added fatalError for child to communicate fatal errors to TT
		/// </remarks>
		public const long versionID = 19L;

		/// <summary>Called when a child task process starts, to get its task.</summary>
		/// <param name="context">
		/// the JvmContext of the JVM w.r.t the TaskTracker that
		/// launched it
		/// </param>
		/// <returns>Task object</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public abstract JvmTask GetTask(JvmContext context);

		/// <summary>Report child's progress to parent.</summary>
		/// <param name="taskId">task-id of the child</param>
		/// <param name="taskStatus">status of the child</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <returns>True if the task is known</returns>
		public abstract bool StatusUpdate(TaskAttemptID taskId, TaskStatus taskStatus);

		/// <summary>Report error messages back to parent.</summary>
		/// <remarks>
		/// Report error messages back to parent.  Calls should be sparing, since all
		/// such messages are held in the job tracker.
		/// </remarks>
		/// <param name="taskid">the id of the task involved</param>
		/// <param name="trace">the text to report</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ReportDiagnosticInfo(TaskAttemptID taskid, string trace);

		/// <summary>Report the record range which is going to process next by the Task.</summary>
		/// <param name="taskid">the id of the task involved</param>
		/// <param name="range">the range of record sequence nos</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ReportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range
			 range);

		/// <summary>Periodically called by child to check if parent is still alive.</summary>
		/// <returns>True if the task is known</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool Ping(TaskAttemptID taskid);

		/// <summary>Report that the task is successfully completed.</summary>
		/// <remarks>
		/// Report that the task is successfully completed.  Failure is assumed if
		/// the task process exits without calling this.
		/// </remarks>
		/// <param name="taskid">task's id</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Done(TaskAttemptID taskid);

		/// <summary>Report that the task is complete, but its commit is pending.</summary>
		/// <param name="taskId">task's id</param>
		/// <param name="taskStatus">status of the child</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void CommitPending(TaskAttemptID taskId, TaskStatus taskStatus);

		/// <summary>Polling to know whether the task can go-ahead with commit</summary>
		/// <param name="taskid"/>
		/// <returns>true/false</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool CanCommit(TaskAttemptID taskid);

		/// <summary>Report that a reduce-task couldn't shuffle map-outputs.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ShuffleError(TaskAttemptID taskId, string message);

		/// <summary>Report that the task encounted a local filesystem error.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void FsError(TaskAttemptID taskId, string message);

		/// <summary>Report that the task encounted a fatal error.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void FatalError(TaskAttemptID taskId, string message);

		/// <summary>Called by a reduce task to get the map output locations for finished maps.
		/// 	</summary>
		/// <remarks>
		/// Called by a reduce task to get the map output locations for finished maps.
		/// Returns an update centered around the map-task-completion-events.
		/// The update also piggybacks the information whether the events copy at the
		/// task-tracker has changed or not. This will trigger some action at the
		/// child-process.
		/// </remarks>
		/// <param name="fromIndex">
		/// the index starting from which the locations should be
		/// fetched
		/// </param>
		/// <param name="maxLocs">the max number of locations to fetch</param>
		/// <param name="id">The attempt id of the task that is trying to communicate</param>
		/// <returns>
		/// A
		/// <see cref="MapTaskCompletionEventsUpdate"/>
		/// 
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract MapTaskCompletionEventsUpdate GetMapCompletionEvents(JobID jobId, 
			int fromIndex, int maxLocs, TaskAttemptID id);
	}

	public static class TaskUmbilicalProtocolConstants
	{
	}
}
