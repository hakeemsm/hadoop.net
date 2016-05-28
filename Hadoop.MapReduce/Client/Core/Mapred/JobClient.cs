using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Mapreduce.Tools;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>JobClient</code> is the primary interface for the user-job to interact
	/// with the cluster.
	/// </summary>
	/// <remarks>
	/// <code>JobClient</code> is the primary interface for the user-job to interact
	/// with the cluster.
	/// <code>JobClient</code> provides facilities to submit jobs, track their
	/// progress, access component-tasks' reports/logs, get the Map-Reduce cluster
	/// status information etc.
	/// <p>The job submission process involves:
	/// <ol>
	/// <li>
	/// Checking the input and output specifications of the job.
	/// </li>
	/// <li>
	/// Computing the
	/// <see cref="InputSplit"/>
	/// s for the job.
	/// </li>
	/// <li>
	/// Setup the requisite accounting information for the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
	/// 
	/// of the job, if necessary.
	/// </li>
	/// <li>
	/// Copying the job's jar and configuration to the map-reduce system directory
	/// on the distributed file-system.
	/// </li>
	/// <li>
	/// Submitting the job to the cluster and optionally monitoring
	/// it's status.
	/// </li>
	/// </ol>
	/// Normally the user creates the application, describes various facets of the
	/// job via
	/// <see cref="JobConf"/>
	/// and then uses the <code>JobClient</code> to submit
	/// the job and monitor its progress.
	/// <p>Here is an example on how to use <code>JobClient</code>:</p>
	/// <p><blockquote><pre>
	/// // Create a new JobConf
	/// JobConf job = new JobConf(new Configuration(), MyJob.class);
	/// // Specify various job-specific parameters
	/// job.setJobName("myjob");
	/// job.setInputPath(new Path("in"));
	/// job.setOutputPath(new Path("out"));
	/// job.setMapperClass(MyJob.MyMapper.class);
	/// job.setReducerClass(MyJob.MyReducer.class);
	/// // Submit the job, then poll for progress until the job is complete
	/// JobClient.runJob(job);
	/// </pre></blockquote>
	/// <b id="JobControl">Job Control</b>
	/// <p>At times clients would chain map-reduce jobs to accomplish complex tasks
	/// which cannot be done via a single map-reduce job. This is fairly easy since
	/// the output of the job, typically, goes to distributed file-system and that
	/// can be used as the input for the next job.</p>
	/// <p>However, this also means that the onus on ensuring jobs are complete
	/// (success/failure) lies squarely on the clients. In such situations the
	/// various job-control options are:
	/// <ol>
	/// <li>
	/// <see cref="RunJob(JobConf)"/>
	/// : submits the job and returns only after
	/// the job has completed.
	/// </li>
	/// <li>
	/// <see cref="SubmitJob(JobConf)"/>
	/// : only submits the job, then poll the
	/// returned handle to the
	/// <see cref="RunningJob"/>
	/// to query status and make
	/// scheduling decisions.
	/// </li>
	/// <li>
	/// <see cref="JobConf.SetJobEndNotificationURI(string)"/>
	/// : setup a notification
	/// on job-completion, thus avoiding polling.
	/// </li>
	/// </ol>
	/// </remarks>
	/// <seealso cref="JobConf"/>
	/// <seealso cref="ClusterStatus"/>
	/// <seealso cref="Org.Apache.Hadoop.Util.Tool"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
	public class JobClient : CLI
	{
		[InterfaceAudience.Private]
		public const string MapreduceClientRetryPolicyEnabledKey = "mapreduce.jobclient.retry.policy.enabled";

		[InterfaceAudience.Private]
		public const bool MapreduceClientRetryPolicyEnabledDefault = false;

		[InterfaceAudience.Private]
		public const string MapreduceClientRetryPolicySpecKey = "mapreduce.jobclient.retry.policy.spec";

		[InterfaceAudience.Private]
		public const string MapreduceClientRetryPolicySpecDefault = "10000,6,60000,10";

		public enum TaskStatusFilter
		{
			None,
			Killed,
			Failed,
			Succeeded,
			All
		}

		private JobClient.TaskStatusFilter taskOutputFilter = JobClient.TaskStatusFilter.
			Failed;

		private int maxRetry = MRJobConfig.DefaultMrClientJobMaxRetries;

		private long retryInterval = MRJobConfig.DefaultMrClientJobRetryInterval;

		static JobClient()
		{
			// t1,n1,t2,n2,...
			ConfigUtil.LoadResources();
		}

		/// <summary>A NetworkedJob is an implementation of RunningJob.</summary>
		/// <remarks>
		/// A NetworkedJob is an implementation of RunningJob.  It holds
		/// a JobProfile object to provide some info, and interacts with the
		/// remote service to provide certain functionality.
		/// </remarks>
		internal class NetworkedJob : RunningJob
		{
			internal Job job;

			/// <summary>
			/// We store a JobProfile and a timestamp for when we last
			/// acquired the job profile.
			/// </summary>
			/// <remarks>
			/// We store a JobProfile and a timestamp for when we last
			/// acquired the job profile.  If the job is null, then we cannot
			/// perform any of the tasks.  The job might be null if the cluster
			/// has completely forgotten about the job.  (eg, 24 hours after the
			/// job completes.)
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public NetworkedJob(JobStatus status, Cluster cluster)
				: this(status, cluster, new JobConf(status.GetJobFile()))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			private NetworkedJob(JobStatus status, Cluster cluster, JobConf conf)
				: this(Job.GetInstance(cluster, status, conf))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public NetworkedJob(Job job)
			{
				this.job = job;
			}

			public virtual Configuration GetConfiguration()
			{
				return job.GetConfiguration();
			}

			/// <summary>An identifier for the job</summary>
			public virtual JobID GetID()
			{
				return JobID.Downgrade(job.GetJobID());
			}

			[System.ObsoleteAttribute(@"This method is deprecated and will be removed. Applications should rather use GetID() ."
				)]
			public virtual string GetJobID()
			{
				return GetID().ToString();
			}

			/// <summary>The user-specified job name</summary>
			public virtual string GetJobName()
			{
				return job.GetJobName();
			}

			/// <summary>The name of the job file</summary>
			public virtual string GetJobFile()
			{
				return job.GetJobFile();
			}

			/// <summary>A URL where the job's status can be seen</summary>
			public virtual string GetTrackingURL()
			{
				return job.GetTrackingURL();
			}

			/// <summary>
			/// A float between 0.0 and 1.0, indicating the % of map work
			/// completed.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual float MapProgress()
			{
				return job.MapProgress();
			}

			/// <summary>
			/// A float between 0.0 and 1.0, indicating the % of reduce work
			/// completed.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual float ReduceProgress()
			{
				return job.ReduceProgress();
			}

			/// <summary>
			/// A float between 0.0 and 1.0, indicating the % of cleanup work
			/// completed.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual float CleanupProgress()
			{
				try
				{
					return job.CleanupProgress();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			/// <summary>
			/// A float between 0.0 and 1.0, indicating the % of setup work
			/// completed.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual float SetupProgress()
			{
				return job.SetupProgress();
			}

			/// <summary>Returns immediately whether the whole job is done yet or not.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool IsComplete()
			{
				lock (this)
				{
					return job.IsComplete();
				}
			}

			/// <summary>True iff job completed successfully.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool IsSuccessful()
			{
				lock (this)
				{
					return job.IsSuccessful();
				}
			}

			/// <summary>Blocks until the job is finished</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WaitForCompletion()
			{
				try
				{
					job.WaitForCompletion(false);
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
				catch (TypeLoadException ce)
				{
					throw new IOException(ce);
				}
			}

			/// <summary>Tells the service to get the state of the current job.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual int GetJobState()
			{
				lock (this)
				{
					try
					{
						return job.GetJobState().GetValue();
					}
					catch (Exception ie)
					{
						throw new IOException(ie);
					}
				}
			}

			/// <summary>Tells the service to terminate the current job.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void KillJob()
			{
				lock (this)
				{
					job.KillJob();
				}
			}

			/// <summary>Set the priority of the job.</summary>
			/// <param name="priority">new priority of the job.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void SetJobPriority(string priority)
			{
				lock (this)
				{
					try
					{
						job.SetPriority(JobPriority.ValueOf(priority));
					}
					catch (Exception ie)
					{
						throw new IOException(ie);
					}
				}
			}

			/// <summary>Kill indicated task attempt.</summary>
			/// <param name="taskId">the id of the task to kill.</param>
			/// <param name="shouldFail">
			/// if true the task is failed and added to failed tasks list, otherwise
			/// it is just killed, w/o affecting job failure status.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void KillTask(TaskAttemptID taskId, bool shouldFail)
			{
				lock (this)
				{
					if (shouldFail)
					{
						job.FailTask(taskId);
					}
					else
					{
						job.KillTask(taskId);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Applications should rather use KillTask(TaskAttemptID, bool)"
				)]
			public virtual void KillTask(string taskId, bool shouldFail)
			{
				lock (this)
				{
					KillTask(((TaskAttemptID)TaskAttemptID.ForName(taskId)), shouldFail);
				}
			}

			/// <summary>Fetch task completion events from cluster for this job.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual TaskCompletionEvent[] GetTaskCompletionEvents(int startFrom)
			{
				lock (this)
				{
					try
					{
						TaskCompletionEvent[] acls = job.GetTaskCompletionEvents(startFrom, 10);
						TaskCompletionEvent[] ret = new TaskCompletionEvent[acls.Length];
						for (int i = 0; i < acls.Length; i++)
						{
							ret[i] = TaskCompletionEvent.Downgrade(acls[i]);
						}
						return ret;
					}
					catch (Exception ie)
					{
						throw new IOException(ie);
					}
				}
			}

			/// <summary>Dump stats to screen</summary>
			public override string ToString()
			{
				return job.ToString();
			}

			/// <summary>Returns the counters for this job</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual Counters GetCounters()
			{
				Counters result = null;
				Counters temp = job.GetCounters();
				if (temp != null)
				{
					result = Counters.Downgrade(temp);
				}
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string[] GetTaskDiagnostics(TaskAttemptID id)
			{
				try
				{
					return job.GetTaskDiagnostics(id);
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetHistoryUrl()
			{
				try
				{
					return job.GetHistoryUrl();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool IsRetired()
			{
				try
				{
					return job.IsRetired();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			internal virtual bool MonitorAndPrintJob()
			{
				return job.MonitorAndPrintJob();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetFailureInfo()
			{
				try
				{
					return job.GetStatus().GetFailureInfo();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual JobStatus GetJobStatus()
			{
				try
				{
					return JobStatus.Downgrade(job.GetStatus());
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}
		}

		/// <summary>Ugi of the client.</summary>
		/// <remarks>
		/// Ugi of the client. We store this ugi when the client is created and
		/// then make sure that the same ugi is used to run the various protocols.
		/// </remarks>
		internal UserGroupInformation clientUgi;

		/// <summary>Create a job client.</summary>
		public JobClient()
		{
		}

		/// <summary>
		/// Build a job client with the given
		/// <see cref="JobConf"/>
		/// , and connect to the
		/// default cluster
		/// </summary>
		/// <param name="conf">the job configuration.</param>
		/// <exception cref="System.IO.IOException"/>
		public JobClient(JobConf conf)
		{
			Init(conf);
		}

		/// <summary>
		/// Build a job client with the given
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// ,
		/// and connect to the default cluster
		/// </summary>
		/// <param name="conf">the configuration.</param>
		/// <exception cref="System.IO.IOException"/>
		public JobClient(Configuration conf)
		{
			Init(new JobConf(conf));
		}

		/// <summary>Connect to the default cluster</summary>
		/// <param name="conf">the job configuration.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Init(JobConf conf)
		{
			SetConf(conf);
			cluster = new Cluster(conf);
			clientUgi = UserGroupInformation.GetCurrentUser();
			maxRetry = conf.GetInt(MRJobConfig.MrClientJobMaxRetries, MRJobConfig.DefaultMrClientJobMaxRetries
				);
			retryInterval = conf.GetLong(MRJobConfig.MrClientJobRetryInterval, MRJobConfig.DefaultMrClientJobRetryInterval
				);
		}

		/// <summary>Build a job client, connect to the indicated job tracker.</summary>
		/// <param name="jobTrackAddr">the job tracker to connect to.</param>
		/// <param name="conf">configuration.</param>
		/// <exception cref="System.IO.IOException"/>
		public JobClient(IPEndPoint jobTrackAddr, Configuration conf)
		{
			cluster = new Cluster(jobTrackAddr, conf);
			clientUgi = UserGroupInformation.GetCurrentUser();
		}

		/// <summary>Close the <code>JobClient</code>.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				cluster.Close();
			}
		}

		/// <summary>Get a filesystem handle.</summary>
		/// <remarks>
		/// Get a filesystem handle.  We need this to prepare jobs
		/// for submission to the MapReduce system.
		/// </remarks>
		/// <returns>the filesystem handle.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileSystem GetFs()
		{
			lock (this)
			{
				try
				{
					return cluster.GetFileSystem();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
			}
		}

		/// <summary>Get a handle to the Cluster</summary>
		public virtual Cluster GetClusterHandle()
		{
			return cluster;
		}

		/// <summary>Submit a job to the MR system.</summary>
		/// <remarks>
		/// Submit a job to the MR system.
		/// This returns a handle to the
		/// <see cref="RunningJob"/>
		/// which can be used to track
		/// the running-job.
		/// </remarks>
		/// <param name="jobFile">the job configuration.</param>
		/// <returns>
		/// a handle to the
		/// <see cref="RunningJob"/>
		/// which can be used to track the
		/// running-job.
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="InvalidJobConfException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Mapred.InvalidJobConfException"/>
		public virtual RunningJob SubmitJob(string jobFile)
		{
			// Load in the submitted job details
			JobConf job = new JobConf(jobFile);
			return SubmitJob(job);
		}

		/// <summary>Submit a job to the MR system.</summary>
		/// <remarks>
		/// Submit a job to the MR system.
		/// This returns a handle to the
		/// <see cref="RunningJob"/>
		/// which can be used to track
		/// the running-job.
		/// </remarks>
		/// <param name="conf">the job configuration.</param>
		/// <returns>
		/// a handle to the
		/// <see cref="RunningJob"/>
		/// which can be used to track the
		/// running-job.
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RunningJob SubmitJob(JobConf conf)
		{
			return SubmitJobInternal(conf);
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual RunningJob SubmitJobInternal(JobConf conf)
		{
			try
			{
				conf.SetBooleanIfUnset("mapred.mapper.new-api", false);
				conf.SetBooleanIfUnset("mapred.reducer.new-api", false);
				Job job = clientUgi.DoAs(new _PrivilegedExceptionAction_570(conf));
				// update our Cluster instance with the one created by Job for submission
				// (we can't pass our Cluster instance to Job, since Job wraps the config
				// instance, and the two configs would then diverge)
				cluster = job.GetCluster();
				return new JobClient.NetworkedJob(job);
			}
			catch (Exception ie)
			{
				throw new IOException("interrupted", ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_570 : PrivilegedExceptionAction<Job
			>
		{
			public _PrivilegedExceptionAction_570(JobConf conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			/// <exception cref="System.Exception"/>
			public Job Run()
			{
				Job job = Job.GetInstance(conf);
				job.Submit();
				return job;
			}

			private readonly JobConf conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Job GetJobUsingCluster(JobID jobid)
		{
			return clientUgi.DoAs(new _PrivilegedExceptionAction_591(this, jobid));
		}

		private sealed class _PrivilegedExceptionAction_591 : PrivilegedExceptionAction<Job
			>
		{
			public _PrivilegedExceptionAction_591(JobClient _enclosing, JobID jobid)
			{
				this._enclosing = _enclosing;
				this.jobid = jobid;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Job Run()
			{
				return this._enclosing.cluster.GetJob(jobid);
			}

			private readonly JobClient _enclosing;

			private readonly JobID jobid;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RunningJob GetJobInner(JobID jobid)
		{
			try
			{
				Job job = GetJobUsingCluster(jobid);
				if (job != null)
				{
					JobStatus status = JobStatus.Downgrade(job.GetStatus());
					if (status != null)
					{
						return new JobClient.NetworkedJob(status, cluster, new JobConf(job.GetConfiguration
							()));
					}
				}
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
			return null;
		}

		/// <summary>
		/// Get an
		/// <see cref="RunningJob"/>
		/// object to track an ongoing job.  Returns
		/// null if the id does not correspond to any known job.
		/// </summary>
		/// <param name="jobid">the jobid of the job.</param>
		/// <returns>
		/// the
		/// <see cref="RunningJob"/>
		/// handle to track the job, null if the
		/// <code>jobid</code> doesn't correspond to any known job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual RunningJob GetJob(JobID jobid)
		{
			for (int i = 0; i <= maxRetry; i++)
			{
				if (i > 0)
				{
					try
					{
						Sharpen.Thread.Sleep(retryInterval);
					}
					catch (Exception)
					{
					}
				}
				RunningJob job = GetJobInner(jobid);
				if (job != null)
				{
					return job;
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Applications should rather use GetJob(JobID) .")]
		public virtual RunningJob GetJob(string jobid)
		{
			return GetJob(((JobID)JobID.ForName(jobid)));
		}

		private static readonly TaskReport[] EmptyTaskReports = new TaskReport[0];

		/// <summary>Get the information of the current state of the map tasks of a job.</summary>
		/// <param name="jobId">the job to query.</param>
		/// <returns>the list of all of the map tips.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TaskReport[] GetMapTaskReports(JobID jobId)
		{
			return GetTaskReports(jobId, TaskType.Map);
		}

		/// <exception cref="System.IO.IOException"/>
		private TaskReport[] GetTaskReports(JobID jobId, TaskType type)
		{
			try
			{
				Job j = GetJobUsingCluster(jobId);
				if (j == null)
				{
					return EmptyTaskReports;
				}
				return TaskReport.DowngradeArray(j.GetTaskReports(type));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Applications should rather use GetMapTaskReports(JobID)"
			)]
		public virtual TaskReport[] GetMapTaskReports(string jobId)
		{
			return GetMapTaskReports(((JobID)JobID.ForName(jobId)));
		}

		/// <summary>Get the information of the current state of the reduce tasks of a job.</summary>
		/// <param name="jobId">the job to query.</param>
		/// <returns>the list of all of the reduce tips.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TaskReport[] GetReduceTaskReports(JobID jobId)
		{
			return GetTaskReports(jobId, TaskType.Reduce);
		}

		/// <summary>Get the information of the current state of the cleanup tasks of a job.</summary>
		/// <param name="jobId">the job to query.</param>
		/// <returns>the list of all of the cleanup tips.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TaskReport[] GetCleanupTaskReports(JobID jobId)
		{
			return GetTaskReports(jobId, TaskType.JobCleanup);
		}

		/// <summary>Get the information of the current state of the setup tasks of a job.</summary>
		/// <param name="jobId">the job to query.</param>
		/// <returns>the list of all of the setup tips.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TaskReport[] GetSetupTaskReports(JobID jobId)
		{
			return GetTaskReports(jobId, TaskType.JobSetup);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Applications should rather use GetReduceTaskReports(JobID)"
			)]
		public virtual TaskReport[] GetReduceTaskReports(string jobId)
		{
			return GetReduceTaskReports(((JobID)JobID.ForName(jobId)));
		}

		/// <summary>
		/// Display the information about a job's tasks, of a particular type and
		/// in a particular state
		/// </summary>
		/// <param name="jobId">the ID of the job</param>
		/// <param name="type">the type of the task (map/reduce/setup/cleanup)</param>
		/// <param name="state">
		/// the state of the task
		/// (pending/running/completed/failed/killed)
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DisplayTasks(JobID jobId, string type, string state)
		{
			try
			{
				Job job = GetJobUsingCluster(jobId);
				base.DisplayTasks(job, type, state);
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		/// <summary>Get status information about the Map-Reduce cluster.</summary>
		/// <returns>
		/// the status information about the Map-Reduce cluster as an object
		/// of
		/// <see cref="ClusterStatus"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual ClusterStatus GetClusterStatus()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_746(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_746 : PrivilegedExceptionAction<ClusterStatus
			>
		{
			public _PrivilegedExceptionAction_746(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public ClusterStatus Run()
			{
				ClusterMetrics metrics = this._enclosing.cluster.GetClusterStatus();
				return new ClusterStatus(metrics.GetTaskTrackerCount(), metrics.GetBlackListedTaskTrackerCount
					(), this._enclosing.cluster.GetTaskTrackerExpiryInterval(), metrics.GetOccupiedMapSlots
					(), metrics.GetOccupiedReduceSlots(), metrics.GetMapSlotCapacity(), metrics.GetReduceSlotCapacity
					(), this._enclosing.cluster.GetJobTrackerStatus(), metrics.GetDecommissionedTaskTrackerCount
					(), metrics.GetGrayListedTaskTrackerCount());
			}

			private readonly JobClient _enclosing;
		}

		private ICollection<string> ArrayToStringList(TaskTrackerInfo[] objs)
		{
			ICollection<string> list = new AList<string>();
			foreach (TaskTrackerInfo info in objs)
			{
				list.AddItem(info.GetTaskTrackerName());
			}
			return list;
		}

		private ICollection<ClusterStatus.BlackListInfo> ArrayToBlackListInfo(TaskTrackerInfo
			[] objs)
		{
			ICollection<ClusterStatus.BlackListInfo> list = new AList<ClusterStatus.BlackListInfo
				>();
			foreach (TaskTrackerInfo info in objs)
			{
				ClusterStatus.BlackListInfo binfo = new ClusterStatus.BlackListInfo();
				binfo.SetTrackerName(info.GetTaskTrackerName());
				binfo.SetReasonForBlackListing(info.GetReasonForBlacklist());
				binfo.SetBlackListReport(info.GetBlacklistReport());
				list.AddItem(binfo);
			}
			return list;
		}

		/// <summary>Get status information about the Map-Reduce cluster.</summary>
		/// <param name="detailed">
		/// if true then get a detailed status including the
		/// tracker names
		/// </param>
		/// <returns>
		/// the status information about the Map-Reduce cluster as an object
		/// of
		/// <see cref="ClusterStatus"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual ClusterStatus GetClusterStatus(bool detailed)
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_794(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_794 : PrivilegedExceptionAction<ClusterStatus
			>
		{
			public _PrivilegedExceptionAction_794(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public ClusterStatus Run()
			{
				ClusterMetrics metrics = this._enclosing.cluster.GetClusterStatus();
				return new ClusterStatus(this._enclosing.ArrayToStringList(this._enclosing.cluster
					.GetActiveTaskTrackers()), this._enclosing.ArrayToBlackListInfo(this._enclosing.
					cluster.GetBlackListedTaskTrackers()), this._enclosing.cluster.GetTaskTrackerExpiryInterval
					(), metrics.GetOccupiedMapSlots(), metrics.GetOccupiedReduceSlots(), metrics.GetMapSlotCapacity
					(), metrics.GetReduceSlotCapacity(), this._enclosing.cluster.GetJobTrackerStatus
					());
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>Get the jobs that are not completed and not failed.</summary>
		/// <returns>
		/// array of
		/// <see cref="JobStatus"/>
		/// for the running/to-be-run jobs.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobStatus[] JobsToComplete()
		{
			IList<JobStatus> stats = new AList<JobStatus>();
			foreach (JobStatus stat in GetAllJobs())
			{
				if (!stat.IsJobComplete())
				{
					stats.AddItem(stat);
				}
			}
			return Sharpen.Collections.ToArray(stats, new JobStatus[0]);
		}

		/// <summary>Get the jobs that are submitted.</summary>
		/// <returns>
		/// array of
		/// <see cref="JobStatus"/>
		/// for the submitted jobs.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobStatus[] GetAllJobs()
		{
			try
			{
				JobStatus[] jobs = clientUgi.DoAs(new _PrivilegedExceptionAction_837(this));
				JobStatus[] stats = new JobStatus[jobs.Length];
				for (int i = 0; i < jobs.Length; i++)
				{
					stats[i] = JobStatus.Downgrade(jobs[i]);
				}
				return stats;
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_837 : PrivilegedExceptionAction<JobStatus
			[]>
		{
			public _PrivilegedExceptionAction_837(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public JobStatus[] Run()
			{
				return this._enclosing.cluster.GetAllJobStatuses();
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>
		/// Utility that submits a job, then polls for progress until the job is
		/// complete.
		/// </summary>
		/// <param name="job">the job configuration.</param>
		/// <exception cref="System.IO.IOException">if the job fails</exception>
		public static RunningJob RunJob(JobConf job)
		{
			JobClient jc = new JobClient(job);
			RunningJob rj = jc.SubmitJob(job);
			try
			{
				if (!jc.MonitorAndPrintJob(job, rj))
				{
					throw new IOException("Job failed!");
				}
			}
			catch (Exception)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
			}
			return rj;
		}

		/// <summary>
		/// Monitor a job and print status in real-time as progress is made and tasks
		/// fail.
		/// </summary>
		/// <param name="conf">the job's configuration</param>
		/// <param name="job">the job to track</param>
		/// <returns>true if the job succeeded</returns>
		/// <exception cref="System.IO.IOException">if communication to the JobTracker fails</exception>
		/// <exception cref="System.Exception"/>
		public virtual bool MonitorAndPrintJob(JobConf conf, RunningJob job)
		{
			return ((JobClient.NetworkedJob)job).MonitorAndPrintJob();
		}

		internal static string GetTaskLogURL(TaskAttemptID taskId, string baseUrl)
		{
			return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId);
		}

		internal static Configuration GetConfiguration(string jobTrackerSpec)
		{
			Configuration conf = new Configuration();
			if (jobTrackerSpec != null)
			{
				if (jobTrackerSpec.IndexOf(":") >= 0)
				{
					conf.Set("mapred.job.tracker", jobTrackerSpec);
				}
				else
				{
					string classpathFile = "hadoop-" + jobTrackerSpec + ".xml";
					Uri validate = conf.GetResource(classpathFile);
					if (validate == null)
					{
						throw new RuntimeException(classpathFile + " not found on CLASSPATH");
					}
					conf.AddResource(classpathFile);
				}
			}
			return conf;
		}

		/// <summary>Sets the output filter for tasks.</summary>
		/// <remarks>
		/// Sets the output filter for tasks. only those tasks are printed whose
		/// output matches the filter.
		/// </remarks>
		/// <param name="newValue">task filter.</param>
		[Obsolete]
		public virtual void SetTaskOutputFilter(JobClient.TaskStatusFilter newValue)
		{
			this.taskOutputFilter = newValue;
		}

		/// <summary>Get the task output filter out of the JobConf.</summary>
		/// <param name="job">the JobConf to examine.</param>
		/// <returns>the filter level.</returns>
		public static JobClient.TaskStatusFilter GetTaskOutputFilter(JobConf job)
		{
			return JobClient.TaskStatusFilter.ValueOf(job.Get("jobclient.output.filter", "FAILED"
				));
		}

		/// <summary>Modify the JobConf to set the task output filter.</summary>
		/// <param name="job">the JobConf to modify.</param>
		/// <param name="newValue">the value to set.</param>
		public static void SetTaskOutputFilter(JobConf job, JobClient.TaskStatusFilter newValue
			)
		{
			job.Set("jobclient.output.filter", newValue.ToString());
		}

		/// <summary>Returns task output filter.</summary>
		/// <returns>task filter.</returns>
		[Obsolete]
		public virtual JobClient.TaskStatusFilter GetTaskOutputFilter()
		{
			return this.taskOutputFilter;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override long GetCounter(Counters cntrs, string counterGroupName
			, string counterName)
		{
			Counters counters = Counters.Downgrade(cntrs);
			return counters.FindCounter(counterGroupName, counterName).GetValue();
		}

		/// <summary>Get status information about the max available Maps in the cluster.</summary>
		/// <returns>the max available Maps in the cluster</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int GetDefaultMaps()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_964(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_964 : PrivilegedExceptionAction<int
			>
		{
			public _PrivilegedExceptionAction_964(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public int Run()
			{
				return this._enclosing.cluster.GetClusterStatus().GetMapSlotCapacity();
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>Get status information about the max available Reduces in the cluster.</summary>
		/// <returns>the max available Reduces in the cluster</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int GetDefaultReduces()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_983(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_983 : PrivilegedExceptionAction<int
			>
		{
			public _PrivilegedExceptionAction_983(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public int Run()
			{
				return this._enclosing.cluster.GetClusterStatus().GetReduceSlotCapacity();
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>Grab the jobtracker system directory path where job-specific files are to be placed.
		/// 	</summary>
		/// <returns>the system directory where job-specific files are to be placed.</returns>
		public virtual Path GetSystemDir()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_1001(this));
			}
			catch (IOException)
			{
				return null;
			}
			catch (Exception)
			{
				return null;
			}
		}

		private sealed class _PrivilegedExceptionAction_1001 : PrivilegedExceptionAction<
			Path>
		{
			public _PrivilegedExceptionAction_1001(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Path Run()
			{
				return this._enclosing.cluster.GetSystemDir();
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>
		/// Checks if the job directory is clean and has all the required components
		/// for (re) starting the job
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool IsJobDirValid(Path jobDirPath, FileSystem fs)
		{
			FileStatus[] contents = fs.ListStatus(jobDirPath);
			int matchCount = 0;
			if (contents != null && contents.Length >= 2)
			{
				foreach (FileStatus status in contents)
				{
					if ("job.xml".Equals(status.GetPath().GetName()))
					{
						++matchCount;
					}
					if ("job.split".Equals(status.GetPath().GetName()))
					{
						++matchCount;
					}
				}
				if (matchCount == 2)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Fetch the staging area directory for the application</summary>
		/// <returns>path to staging area directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetStagingAreaDir()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_1046(this));
			}
			catch (Exception ie)
			{
				// throw RuntimeException instead for compatibility reasons
				throw new RuntimeException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1046 : PrivilegedExceptionAction<
			Path>
		{
			public _PrivilegedExceptionAction_1046(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Path Run()
			{
				return this._enclosing.cluster.GetStagingAreaDir();
			}

			private readonly JobClient _enclosing;
		}

		private JobQueueInfo GetJobQueueInfo(QueueInfo queue)
		{
			JobQueueInfo ret = new JobQueueInfo(queue);
			// make sure to convert any children
			if (queue.GetQueueChildren().Count > 0)
			{
				IList<JobQueueInfo> childQueues = new AList<JobQueueInfo>(queue.GetQueueChildren(
					).Count);
				foreach (QueueInfo child in queue.GetQueueChildren())
				{
					childQueues.AddItem(GetJobQueueInfo(child));
				}
				ret.SetChildren(childQueues);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private JobQueueInfo[] GetJobQueueInfoArray(QueueInfo[] queues)
		{
			JobQueueInfo[] ret = new JobQueueInfo[queues.Length];
			for (int i = 0; i < queues.Length; i++)
			{
				ret[i] = GetJobQueueInfo(queues[i]);
			}
			return ret;
		}

		/// <summary>
		/// Returns an array of queue information objects about root level queues
		/// configured
		/// </summary>
		/// <returns>the array of root level JobQueueInfo objects</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobQueueInfo[] GetRootQueues()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_1090(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1090 : PrivilegedExceptionAction<
			JobQueueInfo[]>
		{
			public _PrivilegedExceptionAction_1090(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public JobQueueInfo[] Run()
			{
				return this._enclosing.GetJobQueueInfoArray(this._enclosing.cluster.GetRootQueues
					());
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>
		/// Returns an array of queue information objects about immediate children
		/// of queue queueName.
		/// </summary>
		/// <param name="queueName"/>
		/// <returns>the array of immediate children JobQueueInfo objects</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobQueueInfo[] GetChildQueues(string queueName)
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_1110(this, queueName));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1110 : PrivilegedExceptionAction<
			JobQueueInfo[]>
		{
			public _PrivilegedExceptionAction_1110(JobClient _enclosing, string queueName)
			{
				this._enclosing = _enclosing;
				this.queueName = queueName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public JobQueueInfo[] Run()
			{
				return this._enclosing.GetJobQueueInfoArray(this._enclosing.cluster.GetChildQueues
					(queueName));
			}

			private readonly JobClient _enclosing;

			private readonly string queueName;
		}

		/// <summary>
		/// Return an array of queue information objects about all the Job Queues
		/// configured.
		/// </summary>
		/// <returns>Array of JobQueueInfo objects</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobQueueInfo[] GetQueues()
		{
			try
			{
				return clientUgi.DoAs(new _PrivilegedExceptionAction_1129(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1129 : PrivilegedExceptionAction<
			JobQueueInfo[]>
		{
			public _PrivilegedExceptionAction_1129(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public JobQueueInfo[] Run()
			{
				return this._enclosing.GetJobQueueInfoArray(this._enclosing.cluster.GetQueues());
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>Gets all the jobs which were added to particular Job Queue</summary>
		/// <param name="queueName">name of the Job Queue</param>
		/// <returns>Array of jobs present in the job queue</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobStatus[] GetJobsFromQueue(string queueName)
		{
			try
			{
				QueueInfo queue = clientUgi.DoAs(new _PrivilegedExceptionAction_1149(this, queueName
					));
				if (queue == null)
				{
					return null;
				}
				JobStatus[] stats = queue.GetJobStatuses();
				JobStatus[] ret = new JobStatus[stats.Length];
				for (int i = 0; i < stats.Length; i++)
				{
					ret[i] = JobStatus.Downgrade(stats[i]);
				}
				return ret;
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1149 : PrivilegedExceptionAction<
			QueueInfo>
		{
			public _PrivilegedExceptionAction_1149(JobClient _enclosing, string queueName)
			{
				this._enclosing = _enclosing;
				this.queueName = queueName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public QueueInfo Run()
			{
				return this._enclosing.cluster.GetQueue(queueName);
			}

			private readonly JobClient _enclosing;

			private readonly string queueName;
		}

		/// <summary>Gets the queue information associated to a particular Job Queue</summary>
		/// <param name="queueName">name of the job queue.</param>
		/// <returns>Queue information associated to particular queue.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual JobQueueInfo GetQueueInfo(string queueName)
		{
			try
			{
				QueueInfo queueInfo = clientUgi.DoAs(new _PrivilegedExceptionAction_1180(this, queueName
					));
				if (queueInfo != null)
				{
					return new JobQueueInfo(queueInfo);
				}
				return null;
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1180 : PrivilegedExceptionAction<
			QueueInfo>
		{
			public _PrivilegedExceptionAction_1180(JobClient _enclosing, string queueName)
			{
				this._enclosing = _enclosing;
				this.queueName = queueName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public QueueInfo Run()
			{
				return this._enclosing.cluster.GetQueue(queueName);
			}

			private readonly JobClient _enclosing;

			private readonly string queueName;
		}

		/// <summary>Gets the Queue ACLs for current user</summary>
		/// <returns>array of QueueAclsInfo object for current user.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual QueueAclsInfo[] GetQueueAclsForCurrentUser()
		{
			try
			{
				QueueAclsInfo[] acls = clientUgi.DoAs(new _PrivilegedExceptionAction_1204(this));
				QueueAclsInfo[] ret = new QueueAclsInfo[acls.Length];
				for (int i = 0; i < acls.Length; i++)
				{
					ret[i] = QueueAclsInfo.Downgrade(acls[i]);
				}
				return ret;
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_1204 : PrivilegedExceptionAction<
			QueueAclsInfo[]>
		{
			public _PrivilegedExceptionAction_1204(JobClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public QueueAclsInfo[] Run()
			{
				return this._enclosing.cluster.GetQueueAclsForCurrentUser();
			}

			private readonly JobClient _enclosing;
		}

		/// <summary>Get a delegation token for the user from the JobTracker.</summary>
		/// <param name="renewer">the user who can renew the token</param>
		/// <returns>the new token</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			GetDelegationToken(Text renewer)
		{
			return clientUgi.DoAs(new _PrivilegedExceptionAction_1229(this, renewer));
		}

		private sealed class _PrivilegedExceptionAction_1229 : PrivilegedExceptionAction<
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>>
		{
			public _PrivilegedExceptionAction_1229(JobClient _enclosing, Text renewer)
			{
				this._enclosing = _enclosing;
				this.renewer = renewer;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> Run()
			{
				return this._enclosing.cluster.GetDelegationToken(renewer);
			}

			private readonly JobClient _enclosing;

			private readonly Text renewer;
		}

		/// <summary>Renew a delegation token</summary>
		/// <param name="token">the token to renew</param>
		/// <returns>true if the renewal went well</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Token.Token{T}.Renew(Org.Apache.Hadoop.Conf.Configuration) instead"
			)]
		public virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			return token.Renew(GetConf());
		}

		/// <summary>Cancel a delegation token from the JobTracker</summary>
		/// <param name="token">the token to cancel</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Token.Token{T}.Cancel(Org.Apache.Hadoop.Conf.Configuration) instead"
			)]
		public virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			token.Cancel(GetConf());
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new JobClient(), argv);
			System.Environment.Exit(res);
		}
	}
}
