using System;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>RunningJob</code> is the user-interface to query for details on a
	/// running Map-Reduce job.
	/// </summary>
	/// <remarks>
	/// <code>RunningJob</code> is the user-interface to query for details on a
	/// running Map-Reduce job.
	/// <p>Clients can get hold of <code>RunningJob</code> via the
	/// <see cref="JobClient"/>
	/// and then query the running-job for details such as name, configuration,
	/// progress etc.</p>
	/// </remarks>
	/// <seealso cref="JobClient"/>
	public interface RunningJob
	{
		/// <summary>Get the underlying job configuration</summary>
		/// <returns>the configuration of the job.</returns>
		Configuration GetConfiguration();

		/// <summary>Get the job identifier.</summary>
		/// <returns>the job identifier.</returns>
		JobID GetID();

		[System.ObsoleteAttribute(@"This method is deprecated and will be removed. Applications should rather use GetID() ."
			)]
		string GetJobID();

		/// <summary>Get the name of the job.</summary>
		/// <returns>the name of the job.</returns>
		string GetJobName();

		/// <summary>Get the path of the submitted job configuration.</summary>
		/// <returns>the path of the submitted job configuration.</returns>
		string GetJobFile();

		/// <summary>Get the URL where some job progress information will be displayed.</summary>
		/// <returns>the URL where some job progress information will be displayed.</returns>
		string GetTrackingURL();

		/// <summary>
		/// Get the <i>progress</i> of the job's map-tasks, as a float between 0.0
		/// and 1.0.
		/// </summary>
		/// <remarks>
		/// Get the <i>progress</i> of the job's map-tasks, as a float between 0.0
		/// and 1.0.  When all map tasks have completed, the function returns 1.0.
		/// </remarks>
		/// <returns>the progress of the job's map-tasks.</returns>
		/// <exception cref="System.IO.IOException"/>
		float MapProgress();

		/// <summary>
		/// Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0
		/// and 1.0.
		/// </summary>
		/// <remarks>
		/// Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0
		/// and 1.0.  When all reduce tasks have completed, the function returns 1.0.
		/// </remarks>
		/// <returns>the progress of the job's reduce-tasks.</returns>
		/// <exception cref="System.IO.IOException"/>
		float ReduceProgress();

		/// <summary>
		/// Get the <i>progress</i> of the job's cleanup-tasks, as a float between 0.0
		/// and 1.0.
		/// </summary>
		/// <remarks>
		/// Get the <i>progress</i> of the job's cleanup-tasks, as a float between 0.0
		/// and 1.0.  When all cleanup tasks have completed, the function returns 1.0.
		/// </remarks>
		/// <returns>the progress of the job's cleanup-tasks.</returns>
		/// <exception cref="System.IO.IOException"/>
		float CleanupProgress();

		/// <summary>
		/// Get the <i>progress</i> of the job's setup-tasks, as a float between 0.0
		/// and 1.0.
		/// </summary>
		/// <remarks>
		/// Get the <i>progress</i> of the job's setup-tasks, as a float between 0.0
		/// and 1.0.  When all setup tasks have completed, the function returns 1.0.
		/// </remarks>
		/// <returns>the progress of the job's setup-tasks.</returns>
		/// <exception cref="System.IO.IOException"/>
		float SetupProgress();

		/// <summary>Check if the job is finished or not.</summary>
		/// <remarks>
		/// Check if the job is finished or not.
		/// This is a non-blocking call.
		/// </remarks>
		/// <returns><code>true</code> if the job is complete, else <code>false</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool IsComplete();

		/// <summary>Check if the job completed successfully.</summary>
		/// <returns><code>true</code> if the job succeeded, else <code>false</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool IsSuccessful();

		/// <summary>Blocks until the job is complete.</summary>
		/// <exception cref="System.IO.IOException"/>
		void WaitForCompletion();

		/// <summary>Returns the current state of the Job.</summary>
		/// <remarks>
		/// Returns the current state of the Job.
		/// <see cref="JobStatus"/>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		int GetJobState();

		/// <summary>
		/// Returns a snapshot of the current status,
		/// <see cref="JobStatus"/>
		/// , of the Job.
		/// Need to call again for latest information.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		JobStatus GetJobStatus();

		/// <summary>Kill the running job.</summary>
		/// <remarks>
		/// Kill the running job. Blocks until all job tasks have been killed as well.
		/// If the job is no longer running, it simply returns.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void KillJob();

		/// <summary>Set the priority of a running job.</summary>
		/// <param name="priority">the new priority for the job.</param>
		/// <exception cref="System.IO.IOException"/>
		void SetJobPriority(string priority);

		/// <summary>Get events indicating completion (success/failure) of component tasks.</summary>
		/// <param name="startFrom">index to start fetching events from</param>
		/// <returns>
		/// an array of
		/// <see cref="TaskCompletionEvent"/>
		/// s
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		TaskCompletionEvent[] GetTaskCompletionEvents(int startFrom);

		/// <summary>Kill indicated task attempt.</summary>
		/// <param name="taskId">the id of the task to be terminated.</param>
		/// <param name="shouldFail">
		/// if true the task is failed and added to failed tasks
		/// list, otherwise it is just killed, w/o affecting
		/// job failure status.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void KillTask(TaskAttemptID taskId, bool shouldFail);

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Applications should rather use KillTask(TaskAttemptID, bool)"
			)]
		void KillTask(string taskId, bool shouldFail);

		/// <summary>Gets the counters for this job.</summary>
		/// <returns>the counters for this job or null if the job has been retired.</returns>
		/// <exception cref="System.IO.IOException"/>
		Counters GetCounters();

		/// <summary>Gets the diagnostic messages for a given task attempt.</summary>
		/// <param name="taskid"/>
		/// <returns>the list of diagnostic messages for the task</returns>
		/// <exception cref="System.IO.IOException"/>
		string[] GetTaskDiagnostics(TaskAttemptID taskid);

		/// <summary>Get the url where history file is archived.</summary>
		/// <remarks>
		/// Get the url where history file is archived. Returns empty string if
		/// history file is not available yet.
		/// </remarks>
		/// <returns>the url where history file is archived</returns>
		/// <exception cref="System.IO.IOException"/>
		string GetHistoryUrl();

		/// <summary>Check whether the job has been removed from JobTracker memory and retired.
		/// 	</summary>
		/// <remarks>
		/// Check whether the job has been removed from JobTracker memory and retired.
		/// On retire, the job history file is copied to a location known by
		/// <see cref="GetHistoryUrl()"/>
		/// </remarks>
		/// <returns><code>true</code> if the job retired, else <code>false</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool IsRetired();

		/// <summary>Get failure info for the job.</summary>
		/// <returns>the failure info for the job.</returns>
		/// <exception cref="System.IO.IOException"/>
		string GetFailureInfo();
	}
}
