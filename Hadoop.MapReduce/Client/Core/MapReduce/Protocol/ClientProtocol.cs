using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Protocol
{
	/// <summary>Protocol that a JobClient and the central JobTracker use to communicate.
	/// 	</summary>
	/// <remarks>
	/// Protocol that a JobClient and the central JobTracker use to communicate.  The
	/// JobClient can use these methods to submit a Job for execution, and learn about
	/// the current system status.
	/// </remarks>
	public abstract class ClientProtocol : VersionedProtocol
	{
		public const long versionID = 37L;

		/*
		*Changing the versionID to 2L since the getTaskCompletionEvents method has
		*changed.
		*Changed to 4 since killTask(String,boolean) is added
		*Version 4: added jobtracker state to ClusterStatus
		*Version 5: max_tasks in ClusterStatus is replaced by
		* max_map_tasks and max_reduce_tasks for HADOOP-1274
		* Version 6: change the counters representation for HADOOP-2248
		* Version 7: added getAllJobs for HADOOP-2487
		* Version 8: change {job|task}id's to use corresponding objects rather that strings.
		* Version 9: change the counter representation for HADOOP-1915
		* Version 10: added getSystemDir for HADOOP-3135
		* Version 11: changed JobProfile to include the queue name for HADOOP-3698
		* Version 12: Added getCleanupTaskReports and
		*             cleanupProgress to JobStatus as part of HADOOP-3150
		* Version 13: Added getJobQueueInfos and getJobQueueInfo(queue name)
		*             and getAllJobs(queue) as a part of HADOOP-3930
		* Version 14: Added setPriority for HADOOP-4124
		* Version 15: Added KILLED status to JobStatus as part of HADOOP-3924
		* Version 16: Added getSetupTaskReports and
		*             setupProgress to JobStatus as part of HADOOP-4261
		* Version 17: getClusterStatus returns the amount of memory used by
		*             the server. HADOOP-4435
		* Version 18: Added blacklisted trackers to the ClusterStatus
		*             for HADOOP-4305
		* Version 19: Modified TaskReport to have TIP status and modified the
		*             method getClusterStatus() to take a boolean argument
		*             for HADOOP-4807
		* Version 20: Modified ClusterStatus to have the tasktracker expiry
		*             interval for HADOOP-4939
		* Version 21: Modified TaskID to be aware of the new TaskTypes
		* Version 22: Added method getQueueAclsForCurrentUser to get queue acls info
		*             for a user
		* Version 23: Modified the JobQueueInfo class to inlucde queue state.
		*             Part of HADOOP-5913.
		* Version 24: Modified ClusterStatus to include BlackListInfo class which
		*             encapsulates reasons and report for blacklisted node.
		* Version 25: Added fields to JobStatus for HADOOP-817.
		* Version 26: Added properties to JobQueueInfo as part of MAPREDUCE-861.
		*              added new api's getRootQueues and
		*              getChildQueues(String queueName)
		* Version 27: Changed protocol to use new api objects. And the protocol is
		*             renamed from JobSubmissionProtocol to ClientProtocol.
		* Version 28: Added getJobHistoryDir() as part of MAPREDUCE-975.
		* Version 29: Added reservedSlots, runningTasks and totalJobSubmissions
		*             to ClusterMetrics as part of MAPREDUCE-1048.
		* Version 30: Job submission files are uploaded to a staging area under
		*             user home dir. JobTracker reads the required files from the
		*             staging area using user credentials passed via the rpc.
		* Version 31: Added TokenStorage to submitJob
		* Version 32: Added delegation tokens (add, renew, cancel)
		* Version 33: Added JobACLs to JobStatus as part of MAPREDUCE-1307
		* Version 34: Modified submitJob to use Credentials instead of TokenStorage.
		* Version 35: Added the method getQueueAdmins(queueName) as part of
		*             MAPREDUCE-1664.
		* Version 36: Added the method getJobTrackerStatus() as part of
		*             MAPREDUCE-2337.
		* Version 37: More efficient serialization format for framework counters
		*             (MAPREDUCE-901)
		* Version 38: Added getLogFilePath(JobID, TaskAttemptID) as part of
		*             MAPREDUCE-3146
		*/
		/// <summary>Allocate a name for the job.</summary>
		/// <returns>a unique job name for submitting jobs.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract JobID GetNewJobID();

		/// <summary>Submit a Job for execution.</summary>
		/// <remarks>
		/// Submit a Job for execution.  Returns the latest profile for
		/// that job.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract JobStatus SubmitJob(JobID jobId, string jobSubmitDir, Credentials
			 ts);

		/// <summary>Get the current status of the cluster</summary>
		/// <returns>summary of the state of the cluster</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract ClusterMetrics GetClusterMetrics();

		/// <summary>Get the JobTracker's status.</summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the JobTracker
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract Cluster.JobTrackerStatus GetJobTrackerStatus();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract long GetTaskTrackerExpiryInterval();

		/// <summary>Get the administrators of the given job-queue.</summary>
		/// <remarks>
		/// Get the administrators of the given job-queue.
		/// This method is for hadoop internal use only.
		/// </remarks>
		/// <param name="queueName"/>
		/// <returns>
		/// Queue administrators ACL for the queue to which job is
		/// submitted to
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract AccessControlList GetQueueAdmins(string queueName);

		/// <summary>Kill the indicated job</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void KillJob(JobID jobid);

		/// <summary>Set the priority of the specified job</summary>
		/// <param name="jobid">ID of the job</param>
		/// <param name="priority">Priority to be set for the job</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void SetJobPriority(JobID jobid, string priority);

		/// <summary>Kill indicated task attempt.</summary>
		/// <param name="taskId">the id of the task to kill.</param>
		/// <param name="shouldFail">
		/// if true the task is failed and added to failed tasks list, otherwise
		/// it is just killed, w/o affecting job failure status.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract bool KillTask(TaskAttemptID taskId, bool shouldFail);

		/// <summary>Grab a handle to a job that is already known to the JobTracker.</summary>
		/// <returns>Status of the job, or null if not found.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract JobStatus GetJobStatus(JobID jobid);

		/// <summary>Grab the current job counters</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract Counters GetJobCounters(JobID jobid);

		/// <summary>Grab a bunch of info on the tasks that make up the job</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract TaskReport[] GetTaskReports(JobID jobid, TaskType type);

		/// <summary>A MapReduce system always operates on a single filesystem.</summary>
		/// <remarks>
		/// A MapReduce system always operates on a single filesystem.  This
		/// function returns the fs name.  ('local' if the localfs; 'addr:port'
		/// if dfs).  The client can then copy files into the right locations
		/// prior to submitting the job.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string GetFilesystemName();

		/// <summary>Get all the jobs submitted.</summary>
		/// <returns>array of JobStatus for the submitted jobs</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract JobStatus[] GetAllJobs();

		/// <summary>Get task completion events for the jobid, starting from fromEventId.</summary>
		/// <remarks>
		/// Get task completion events for the jobid, starting from fromEventId.
		/// Returns empty array if no events are available.
		/// </remarks>
		/// <param name="jobid">job id</param>
		/// <param name="fromEventId">event id to start from.</param>
		/// <param name="maxEvents">the max number of events we want to look at</param>
		/// <returns>array of task completion events.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract TaskCompletionEvent[] GetTaskCompletionEvents(JobID jobid, int fromEventId
			, int maxEvents);

		/// <summary>Get the diagnostics for a given task in a given job</summary>
		/// <param name="taskId">the id of the task</param>
		/// <returns>an array of the diagnostic messages</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string[] GetTaskDiagnostics(TaskAttemptID taskId);

		/// <summary>Get all active trackers in cluster.</summary>
		/// <returns>array of TaskTrackerInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract TaskTrackerInfo[] GetActiveTrackers();

		/// <summary>Get all blacklisted trackers in cluster.</summary>
		/// <returns>array of TaskTrackerInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract TaskTrackerInfo[] GetBlacklistedTrackers();

		/// <summary>
		/// Grab the jobtracker system directory path
		/// where job-specific files are to be placed.
		/// </summary>
		/// <returns>the system directory where job-specific files are to be placed.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string GetSystemDir();

		/// <summary>
		/// Get a hint from the JobTracker
		/// where job-specific files are to be placed.
		/// </summary>
		/// <returns>the directory where job-specific files are to be placed.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string GetStagingAreaDir();

		/// <summary>Gets the directory location of the completed job history files.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string GetJobHistoryDir();

		/// <summary>Gets set of Queues associated with the Job Tracker</summary>
		/// <returns>Array of the Queue Information Object</returns>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public abstract QueueInfo[] GetQueues();

		/// <summary>Gets scheduling information associated with the particular Job queue</summary>
		/// <param name="queueName">Queue Name</param>
		/// <returns>Scheduling Information of the Queue</returns>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="System.Exception"/>
		public abstract QueueInfo GetQueue(string queueName);

		/// <summary>Gets the Queue ACLs for current user</summary>
		/// <returns>array of QueueAclsInfo object for current user.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract QueueAclsInfo[] GetQueueAclsForCurrentUser();

		/// <summary>Gets the root level queues.</summary>
		/// <returns>array of JobQueueInfo object.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract QueueInfo[] GetRootQueues();

		/// <summary>Returns immediate children of queueName.</summary>
		/// <param name="queueName"/>
		/// <returns>array of JobQueueInfo which are children of queueName</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract QueueInfo[] GetChildQueues(string queueName);

		/// <summary>Get a new delegation token.</summary>
		/// <param name="renewer">
		/// the user other than the creator (if any) that can renew the
		/// token
		/// </param>
		/// <returns>the new delegation token</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>
			 GetDelegationToken(Text renewer);

		/// <summary>Renew an existing delegation token</summary>
		/// <param name="token">the token to renew</param>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token);

		/// <summary>Cancel a delegation token.</summary>
		/// <param name="token">the token to cancel</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token);

		/// <summary>
		/// Gets the location of the log file for a job if no taskAttemptId is
		/// specified, otherwise gets the log location for the taskAttemptId.
		/// </summary>
		/// <param name="jobID">the jobId.</param>
		/// <param name="taskAttemptID">the taskAttemptId.</param>
		/// <returns>log params.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract LogParams GetLogFileParams(JobID jobID, TaskAttemptID taskAttemptID
			);
	}

	public static class ClientProtocolConstants
	{
	}
}
