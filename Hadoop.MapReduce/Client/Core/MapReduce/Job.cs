using System;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>The job submitter's view of the Job.</summary>
	/// <remarks>
	/// The job submitter's view of the Job.
	/// <p>It allows the user to configure the
	/// job, submit it, control its execution, and query the state. The set methods
	/// only work until the job is submitted, afterwards they will throw an
	/// IllegalStateException. </p>
	/// <p>
	/// Normally the user creates the application, describes various facets of the
	/// job via
	/// <see cref="Job"/>
	/// and then submits the job and monitor its progress.</p>
	/// <p>Here is an example on how to submit a job:</p>
	/// <p><blockquote><pre>
	/// // Create a new Job
	/// Job job = Job.getInstance();
	/// job.setJarByClass(MyJob.class);
	/// // Specify various job-specific parameters
	/// job.setJobName("myjob");
	/// job.setInputPath(new Path("in"));
	/// job.setOutputPath(new Path("out"));
	/// job.setMapperClass(MyJob.MyMapper.class);
	/// job.setReducerClass(MyJob.MyReducer.class);
	/// // Submit the job, then poll for progress until the job is complete
	/// job.waitForCompletion(true);
	/// </pre></blockquote>
	/// </remarks>
	public class Job : JobContextImpl, JobContext
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Job
			));

		public enum JobState
		{
			Define,
			Running
		}

		private const long MaxJobstatusAge = 1000 * 2;

		public const string OutputFilter = "mapreduce.client.output.filter";

		/// <summary>Key in mapred-*.xml that sets completionPollInvervalMillis</summary>
		public const string CompletionPollIntervalKey = "mapreduce.client.completion.pollinterval";

		/// <summary>Default completionPollIntervalMillis is 5000 ms.</summary>
		internal const int DefaultCompletionPollInterval = 5000;

		/// <summary>Key in mapred-*.xml that sets progMonitorPollIntervalMillis</summary>
		public const string ProgressMonitorPollIntervalKey = "mapreduce.client.progressmonitor.pollinterval";

		/// <summary>Default progMonitorPollIntervalMillis is 1000 ms.</summary>
		internal const int DefaultMonitorPollInterval = 1000;

		public const string UsedGenericParser = "mapreduce.client.genericoptionsparser.used";

		public const string SubmitReplication = "mapreduce.client.submit.file.replication";

		public const int DefaultSubmitReplication = 10;

		public enum TaskStatusFilter
		{
			None,
			Killed,
			Failed,
			Succeeded,
			All
		}

		static Job()
		{
			ConfigUtil.LoadResources();
		}

		private Job.JobState state = Job.JobState.Define;

		private JobStatus status;

		private long statustime;

		private Cluster cluster;

		private ReservationId reservationId;

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use GetInstance()")]
		public Job()
			: this(new JobConf(new Configuration()))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use GetInstance(Org.Apache.Hadoop.Conf.Configuration)"
			)]
		public Job(Configuration conf)
			: this(new JobConf(conf))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use GetInstance(Org.Apache.Hadoop.Conf.Configuration, string)"
			)]
		public Job(Configuration conf, string jobName)
			: this(new JobConf(conf))
		{
			SetJobName(jobName);
		}

		/// <exception cref="System.IO.IOException"/>
		internal Job(JobConf conf)
			: base(conf, null)
		{
			// propagate existing user credentials to job
			this.credentials.MergeAll(this.ugi.GetCredentials());
			this.cluster = null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal Job(JobStatus status, JobConf conf)
			: this(conf)
		{
			SetJobID(status.GetJobID());
			this.status = status;
			state = Job.JobState.Running;
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// .
		/// A Cluster will be created with a generic
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// .
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance()
		{
			// create with a null Cluster
			return GetInstance(new Configuration());
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// and a
		/// given
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// .
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(Configuration conf)
		{
			// create with a null Cluster
			JobConf jobConf = new JobConf(conf);
			return new Org.Apache.Hadoop.Mapreduce.Job(jobConf);
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// and a given jobName.
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(Configuration conf, string
			 jobName)
		{
			// create with a null Cluster
			Org.Apache.Hadoop.Mapreduce.Job result = GetInstance(conf);
			result.SetJobName(jobName);
			return result;
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// and given
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// and
		/// <see cref="JobStatus"/>
		/// .
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// </summary>
		/// <param name="status">job status</param>
		/// <param name="conf">job configuration</param>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(JobStatus status, Configuration
			 conf)
		{
			return new Org.Apache.Hadoop.Mapreduce.Job(status, new JobConf(conf));
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// .
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// </summary>
		/// <param name="ignored"/>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use GetInstance()")]
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(Cluster ignored)
		{
			return GetInstance();
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// and given
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// .
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// </summary>
		/// <param name="ignored"/>
		/// <param name="conf">job configuration</param>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use GetInstance(Org.Apache.Hadoop.Conf.Configuration)"
			)]
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(Cluster ignored, Configuration
			 conf)
		{
			return GetInstance(conf);
		}

		/// <summary>
		/// Creates a new
		/// <see cref="Job"/>
		/// with no particular
		/// <see cref="Cluster"/>
		/// and given
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// and
		/// <see cref="JobStatus"/>
		/// .
		/// A Cluster will be created from the conf parameter only when it's needed.
		/// The <code>Job</code> makes a copy of the <code>Configuration</code> so
		/// that any necessary internal modifications do not reflect on the incoming
		/// parameter.
		/// </summary>
		/// <param name="cluster">cluster</param>
		/// <param name="status">job status</param>
		/// <param name="conf">job configuration</param>
		/// <returns>
		/// the
		/// <see cref="Job"/>
		/// , with no connection to a cluster yet.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public static Org.Apache.Hadoop.Mapreduce.Job GetInstance(Cluster cluster, JobStatus
			 status, Configuration conf)
		{
			Org.Apache.Hadoop.Mapreduce.Job job = GetInstance(status, conf);
			job.SetCluster(cluster);
			return job;
		}

		/// <exception cref="System.InvalidOperationException"/>
		private void EnsureState(Job.JobState state)
		{
			if (state != this.state)
			{
				throw new InvalidOperationException("Job in state " + this.state + " instead of "
					 + state);
			}
			if (state == Job.JobState.Running && cluster == null)
			{
				throw new InvalidOperationException("Job in state " + this.state + ", but it isn't attached to any job tracker!"
					);
			}
		}

		/// <summary>Some methods rely on having a recent job status object.</summary>
		/// <remarks>
		/// Some methods rely on having a recent job status object.  Refresh
		/// it, if necessary
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void EnsureFreshStatus()
		{
			lock (this)
			{
				if (Runtime.CurrentTimeMillis() - statustime > MaxJobstatusAge)
				{
					UpdateStatus();
				}
			}
		}

		/// <summary>Some methods need to update status immediately.</summary>
		/// <remarks>
		/// Some methods need to update status immediately. So, refresh
		/// immediately
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void UpdateStatus()
		{
			lock (this)
			{
				try
				{
					this.status = ugi.DoAs(new _PrivilegedExceptionAction_320(this));
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
				if (this.status == null)
				{
					throw new IOException("Job status not available ");
				}
				this.statustime = Runtime.CurrentTimeMillis();
			}
		}

		private sealed class _PrivilegedExceptionAction_320 : PrivilegedExceptionAction<JobStatus
			>
		{
			public _PrivilegedExceptionAction_320(Job _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public JobStatus Run()
			{
				return this._enclosing.cluster.GetClient().GetJobStatus(this._enclosing.status.GetJobID
					());
			}

			private readonly Job _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobStatus GetStatus()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status;
		}

		/// <summary>Returns the current state of the Job.</summary>
		/// <returns>JobStatus#State</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobStatus.State GetJobState()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.GetState();
		}

		/// <summary>Get the URL where some job progress information will be displayed.</summary>
		/// <returns>the URL where some job progress information will be displayed.</returns>
		public virtual string GetTrackingURL()
		{
			EnsureState(Job.JobState.Running);
			return status.GetTrackingUrl().ToString();
		}

		/// <summary>Get the path of the submitted job configuration.</summary>
		/// <returns>the path of the submitted job configuration.</returns>
		public virtual string GetJobFile()
		{
			EnsureState(Job.JobState.Running);
			return status.GetJobFile();
		}

		/// <summary>Get start time of the job.</summary>
		/// <returns>the start time of the job</returns>
		public virtual long GetStartTime()
		{
			EnsureState(Job.JobState.Running);
			return status.GetStartTime();
		}

		/// <summary>Get finish time of the job.</summary>
		/// <returns>the finish time of the job</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual long GetFinishTime()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.GetFinishTime();
		}

		/// <summary>Get scheduling info of the job.</summary>
		/// <returns>the scheduling info of the job</returns>
		public virtual string GetSchedulingInfo()
		{
			EnsureState(Job.JobState.Running);
			return status.GetSchedulingInfo();
		}

		/// <summary>Get scheduling info of the job.</summary>
		/// <returns>the scheduling info of the job</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobPriority GetPriority()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.GetPriority();
		}

		/// <summary>The user-specified job name.</summary>
		public override string GetJobName()
		{
			if (state == Job.JobState.Define)
			{
				return base.GetJobName();
			}
			EnsureState(Job.JobState.Running);
			return status.GetJobName();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetHistoryUrl()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.GetHistoryFile();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool IsRetired()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.IsRetired();
		}

		[InterfaceAudience.Private]
		public virtual Cluster GetCluster()
		{
			return cluster;
		}

		/// <summary>Only for mocks in unit tests.</summary>
		[InterfaceAudience.Private]
		private void SetCluster(Cluster cluster)
		{
			this.cluster = cluster;
		}

		/// <summary>Dump stats to screen.</summary>
		public override string ToString()
		{
			EnsureState(Job.JobState.Running);
			string reasonforFailure = " ";
			int numMaps = 0;
			int numReduces = 0;
			try
			{
				UpdateStatus();
				if (status.GetState().Equals(JobStatus.State.Failed))
				{
					reasonforFailure = GetTaskFailureEventString();
				}
				numMaps = GetTaskReports(TaskType.Map).Length;
				numReduces = GetTaskReports(TaskType.Reduce).Length;
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
			}
			StringBuilder sb = new StringBuilder();
			sb.Append("Job: ").Append(status.GetJobID()).Append("\n");
			sb.Append("Job File: ").Append(status.GetJobFile()).Append("\n");
			sb.Append("Job Tracking URL : ").Append(status.GetTrackingUrl());
			sb.Append("\n");
			sb.Append("Uber job : ").Append(status.IsUber()).Append("\n");
			sb.Append("Number of maps: ").Append(numMaps).Append("\n");
			sb.Append("Number of reduces: ").Append(numReduces).Append("\n");
			sb.Append("map() completion: ");
			sb.Append(status.GetMapProgress()).Append("\n");
			sb.Append("reduce() completion: ");
			sb.Append(status.GetReduceProgress()).Append("\n");
			sb.Append("Job state: ");
			sb.Append(status.GetState()).Append("\n");
			sb.Append("retired: ").Append(status.IsRetired()).Append("\n");
			sb.Append("reason for failure: ").Append(reasonforFailure);
			return sb.ToString();
		}

		/// <returns>taskid which caused job failure</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual string GetTaskFailureEventString()
		{
			int failCount = 1;
			TaskCompletionEvent lastEvent = null;
			TaskCompletionEvent[] events = ugi.DoAs(new _PrivilegedExceptionAction_499(this));
			foreach (TaskCompletionEvent @event in events)
			{
				if (@event.GetStatus().Equals(TaskCompletionEvent.Status.Failed))
				{
					failCount++;
					lastEvent = @event;
				}
			}
			if (lastEvent == null)
			{
				return "There are no failed tasks for the job. " + "Job is failed due to some other reason and reason "
					 + "can be found in the logs.";
			}
			string[] taskAttemptID = lastEvent.GetTaskAttemptId().ToString().Split("_", 2);
			string taskID = Sharpen.Runtime.Substring(taskAttemptID[1], 0, taskAttemptID[1].Length
				 - 2);
			return (" task " + taskID + " failed " + failCount + " times " + "For details check tasktracker at: "
				 + lastEvent.GetTaskTrackerHttp());
		}

		private sealed class _PrivilegedExceptionAction_499 : PrivilegedExceptionAction<TaskCompletionEvent
			[]>
		{
			public _PrivilegedExceptionAction_499(Job _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public TaskCompletionEvent[] Run()
			{
				return this._enclosing.cluster.GetClient().GetTaskCompletionEvents(this._enclosing
					.status.GetJobID(), 0, 10);
			}

			private readonly Job _enclosing;
		}

		/// <summary>Get the information of the current state of the tasks of a job.</summary>
		/// <param name="type">Type of the task</param>
		/// <returns>the list of all of the map tips.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskReport[] GetTaskReports(TaskType type)
		{
			EnsureState(Job.JobState.Running);
			TaskType tmpType = type;
			return ugi.DoAs(new _PrivilegedExceptionAction_536(this, tmpType));
		}

		private sealed class _PrivilegedExceptionAction_536 : PrivilegedExceptionAction<TaskReport
			[]>
		{
			public _PrivilegedExceptionAction_536(Job _enclosing, TaskType tmpType)
			{
				this._enclosing = _enclosing;
				this.tmpType = tmpType;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public TaskReport[] Run()
			{
				return this._enclosing.cluster.GetClient().GetTaskReports(this._enclosing.GetJobID
					(), tmpType);
			}

			private readonly Job _enclosing;

			private readonly TaskType tmpType;
		}

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
		public virtual float MapProgress()
		{
			EnsureState(Job.JobState.Running);
			EnsureFreshStatus();
			return status.GetMapProgress();
		}

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
		public virtual float ReduceProgress()
		{
			EnsureState(Job.JobState.Running);
			EnsureFreshStatus();
			return status.GetReduceProgress();
		}

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
		/// <exception cref="System.Exception"/>
		public virtual float CleanupProgress()
		{
			EnsureState(Job.JobState.Running);
			EnsureFreshStatus();
			return status.GetCleanupProgress();
		}

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
		public virtual float SetupProgress()
		{
			EnsureState(Job.JobState.Running);
			EnsureFreshStatus();
			return status.GetSetupProgress();
		}

		/// <summary>Check if the job is finished or not.</summary>
		/// <remarks>
		/// Check if the job is finished or not.
		/// This is a non-blocking call.
		/// </remarks>
		/// <returns><code>true</code> if the job is complete, else <code>false</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsComplete()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.IsJobComplete();
		}

		/// <summary>Check if the job completed successfully.</summary>
		/// <returns><code>true</code> if the job succeeded, else <code>false</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsSuccessful()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.GetState() == JobStatus.State.Succeeded;
		}

		/// <summary>Kill the running job.</summary>
		/// <remarks>
		/// Kill the running job.  Blocks until all job tasks have been
		/// killed as well.  If the job is no longer running, it simply returns.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void KillJob()
		{
			EnsureState(Job.JobState.Running);
			try
			{
				cluster.GetClient().KillJob(GetJobID());
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		/// <summary>Set the priority of a running job.</summary>
		/// <param name="priority">the new priority for the job.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void SetPriority(JobPriority priority)
		{
			if (state == Job.JobState.Define)
			{
				conf.SetJobPriority(JobPriority.ValueOf(priority.ToString()));
			}
			else
			{
				EnsureState(Job.JobState.Running);
				JobPriority tmpPriority = priority;
				ugi.DoAs(new _PrivilegedExceptionAction_649(this, tmpPriority));
			}
		}

		private sealed class _PrivilegedExceptionAction_649 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_649(Job _enclosing, JobPriority tmpPriority)
			{
				this._enclosing = _enclosing;
				this.tmpPriority = tmpPriority;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public object Run()
			{
				this._enclosing.cluster.GetClient().SetJobPriority(this._enclosing.GetJobID(), tmpPriority
					.ToString());
				return null;
			}

			private readonly Job _enclosing;

			private readonly JobPriority tmpPriority;
		}

		/// <summary>Get events indicating completion (success/failure) of component tasks.</summary>
		/// <param name="startFrom">index to start fetching events from</param>
		/// <param name="numEvents">number of events to fetch</param>
		/// <returns>
		/// an array of
		/// <see cref="TaskCompletionEvent"/>
		/// s
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskCompletionEvent[] GetTaskCompletionEvents(int startFrom, int numEvents
			)
		{
			EnsureState(Job.JobState.Running);
			return ugi.DoAs(new _PrivilegedExceptionAction_670(this, startFrom, numEvents));
		}

		private sealed class _PrivilegedExceptionAction_670 : PrivilegedExceptionAction<TaskCompletionEvent
			[]>
		{
			public _PrivilegedExceptionAction_670(Job _enclosing, int startFrom, int numEvents
				)
			{
				this._enclosing = _enclosing;
				this.startFrom = startFrom;
				this.numEvents = numEvents;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public TaskCompletionEvent[] Run()
			{
				return this._enclosing.cluster.GetClient().GetTaskCompletionEvents(this._enclosing
					.GetJobID(), startFrom, numEvents);
			}

			private readonly Job _enclosing;

			private readonly int startFrom;

			private readonly int numEvents;
		}

		/// <summary>Get events indicating completion (success/failure) of component tasks.</summary>
		/// <param name="startFrom">index to start fetching events from</param>
		/// <returns>
		/// an array of
		/// <see cref="Org.Apache.Hadoop.Mapred.TaskCompletionEvent"/>
		/// s
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual TaskCompletionEvent[] GetTaskCompletionEvents(int startFrom)
		{
			try
			{
				TaskCompletionEvent[] events = GetTaskCompletionEvents(startFrom, 10);
				TaskCompletionEvent[] retEvents = new TaskCompletionEvent[events.Length];
				for (int i = 0; i < events.Length; i++)
				{
					retEvents[i] = TaskCompletionEvent.Downgrade(events[i]);
				}
				return retEvents;
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		/// <summary>Kill indicated task attempt.</summary>
		/// <param name="taskId">the id of the task to kill.</param>
		/// <param name="shouldFail">
		/// if <code>true</code> the task is failed and added
		/// to failed tasks list, otherwise it is just killed,
		/// w/o affecting job failure status.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual bool KillTask(TaskAttemptID taskId, bool shouldFail)
		{
			EnsureState(Job.JobState.Running);
			try
			{
				return ugi.DoAs(new _PrivilegedExceptionAction_714(this, taskId, shouldFail));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_714 : PrivilegedExceptionAction<bool
			>
		{
			public _PrivilegedExceptionAction_714(Job _enclosing, TaskAttemptID taskId, bool 
				shouldFail)
			{
				this._enclosing = _enclosing;
				this.taskId = taskId;
				this.shouldFail = shouldFail;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public bool Run()
			{
				return this._enclosing.cluster.GetClient().KillTask(taskId, shouldFail);
			}

			private readonly Job _enclosing;

			private readonly TaskAttemptID taskId;

			private readonly bool shouldFail;
		}

		/// <summary>Kill indicated task attempt.</summary>
		/// <param name="taskId">the id of the task to be terminated.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void KillTask(TaskAttemptID taskId)
		{
			KillTask(taskId, false);
		}

		/// <summary>Fail indicated task attempt.</summary>
		/// <param name="taskId">the id of the task to be terminated.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FailTask(TaskAttemptID taskId)
		{
			KillTask(taskId, true);
		}

		/// <summary>Gets the counters for this job.</summary>
		/// <remarks>
		/// Gets the counters for this job. May return null if the job has been
		/// retired and the job is no longer in the completed job store.
		/// </remarks>
		/// <returns>the counters for this job.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Counters GetCounters()
		{
			EnsureState(Job.JobState.Running);
			try
			{
				return ugi.DoAs(new _PrivilegedExceptionAction_758(this));
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
		}

		private sealed class _PrivilegedExceptionAction_758 : PrivilegedExceptionAction<Counters
			>
		{
			public _PrivilegedExceptionAction_758(Job _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Counters Run()
			{
				return this._enclosing.cluster.GetClient().GetJobCounters(this._enclosing.GetJobID
					());
			}

			private readonly Job _enclosing;
		}

		/// <summary>Gets the diagnostic messages for a given task attempt.</summary>
		/// <param name="taskid"/>
		/// <returns>the list of diagnostic messages for the task</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string[] GetTaskDiagnostics(TaskAttemptID taskid)
		{
			EnsureState(Job.JobState.Running);
			return ugi.DoAs(new _PrivilegedExceptionAction_779(this, taskid));
		}

		private sealed class _PrivilegedExceptionAction_779 : PrivilegedExceptionAction<string
			[]>
		{
			public _PrivilegedExceptionAction_779(Job _enclosing, TaskAttemptID taskid)
			{
				this._enclosing = _enclosing;
				this.taskid = taskid;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public string[] Run()
			{
				return this._enclosing.cluster.GetClient().GetTaskDiagnostics(taskid);
			}

			private readonly Job _enclosing;

			private readonly TaskAttemptID taskid;
		}

		/// <summary>Set the number of reduce tasks for the job.</summary>
		/// <param name="tasks">the number of reduce tasks</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetNumReduceTasks(int tasks)
		{
			EnsureState(Job.JobState.Define);
			conf.SetNumReduceTasks(tasks);
		}

		/// <summary>Set the current working directory for the default file system.</summary>
		/// <param name="dir">the new current working directory.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetWorkingDirectory(Path dir)
		{
			EnsureState(Job.JobState.Define);
			conf.SetWorkingDirectory(dir);
		}

		/// <summary>
		/// Set the
		/// <see cref="InputFormat{K, V}"/>
		/// for the job.
		/// </summary>
		/// <param name="cls">the <code>InputFormat</code> to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetInputFormatClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(InputFormatClassAttr, cls, typeof(InputFormat));
		}

		/// <summary>
		/// Set the
		/// <see cref="OutputFormat{K, V}"/>
		/// for the job.
		/// </summary>
		/// <param name="cls">the <code>OutputFormat</code> to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetOutputFormatClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(OutputFormatClassAttr, cls, typeof(OutputFormat));
		}

		/// <summary>
		/// Set the
		/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// for the job.
		/// </summary>
		/// <param name="cls">the <code>Mapper</code> to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetMapperClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(MapClassAttr, cls, typeof(Mapper));
		}

		/// <summary>Set the Jar by finding where a given class came from.</summary>
		/// <param name="cls">the example class</param>
		public virtual void SetJarByClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetJarByClass(cls);
		}

		/// <summary>Set the job jar</summary>
		public virtual void SetJar(string jar)
		{
			EnsureState(Job.JobState.Define);
			conf.SetJar(jar);
		}

		/// <summary>Set the reported username for this job.</summary>
		/// <param name="user">the username for this job.</param>
		public virtual void SetUser(string user)
		{
			EnsureState(Job.JobState.Define);
			conf.SetUser(user);
		}

		/// <summary>Set the combiner class for the job.</summary>
		/// <param name="cls">the combiner to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetCombinerClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(CombineClassAttr, cls, typeof(Reducer));
		}

		/// <summary>
		/// Set the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// for the job.
		/// </summary>
		/// <param name="cls">the <code>Reducer</code> to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetReducerClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(ReduceClassAttr, cls, typeof(Reducer));
		}

		/// <summary>
		/// Set the
		/// <see cref="Partitioner{KEY, VALUE}"/>
		/// for the job.
		/// </summary>
		/// <param name="cls">the <code>Partitioner</code> to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetPartitionerClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetClass(PartitionerClassAttr, cls, typeof(Partitioner));
		}

		/// <summary>Set the key class for the map output data.</summary>
		/// <remarks>
		/// Set the key class for the map output data. This allows the user to
		/// specify the map output key class to be different than the final output
		/// value class.
		/// </remarks>
		/// <param name="theClass">the map output key class.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetMapOutputKeyClass(Type theClass)
		{
			EnsureState(Job.JobState.Define);
			conf.SetMapOutputKeyClass(theClass);
		}

		/// <summary>Set the value class for the map output data.</summary>
		/// <remarks>
		/// Set the value class for the map output data. This allows the user to
		/// specify the map output value class to be different than the final output
		/// value class.
		/// </remarks>
		/// <param name="theClass">the map output value class.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetMapOutputValueClass(Type theClass)
		{
			EnsureState(Job.JobState.Define);
			conf.SetMapOutputValueClass(theClass);
		}

		/// <summary>Set the key class for the job output data.</summary>
		/// <param name="theClass">the key class for the job output data.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetOutputKeyClass(Type theClass)
		{
			EnsureState(Job.JobState.Define);
			conf.SetOutputKeyClass(theClass);
		}

		/// <summary>Set the value class for job outputs.</summary>
		/// <param name="theClass">the value class for job outputs.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetOutputValueClass(Type theClass)
		{
			EnsureState(Job.JobState.Define);
			conf.SetOutputValueClass(theClass);
		}

		/// <summary>
		/// Define the comparator that controls which keys are grouped together
		/// for a single call to combiner,
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}.Reduce(object, System.Collections.IEnumerable{T}, Context)
		/// 	"/>
		/// </summary>
		/// <param name="cls">the raw comparator to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetCombinerKeyGroupingComparatorClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetCombinerKeyGroupingComparator(cls);
		}

		/// <summary>
		/// Define the comparator that controls how the keys are sorted before they
		/// are passed to the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// .
		/// </summary>
		/// <param name="cls">the raw comparator</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		/// <seealso cref="SetCombinerKeyGroupingComparatorClass(System.Type{T})"/>
		public virtual void SetSortComparatorClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetOutputKeyComparatorClass(cls);
		}

		/// <summary>
		/// Define the comparator that controls which keys are grouped together
		/// for a single call to
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}.Reduce(object, System.Collections.IEnumerable{T}, Context)
		/// 	"/>
		/// </summary>
		/// <param name="cls">the raw comparator to use</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		/// <seealso cref="SetCombinerKeyGroupingComparatorClass(System.Type{T})"/>
		public virtual void SetGroupingComparatorClass(Type cls)
		{
			EnsureState(Job.JobState.Define);
			conf.SetOutputValueGroupingComparator(cls);
		}

		/// <summary>Set the user-specified job name.</summary>
		/// <param name="name">the job's new name.</param>
		/// <exception cref="System.InvalidOperationException">if the job is submitted</exception>
		public virtual void SetJobName(string name)
		{
			EnsureState(Job.JobState.Define);
			conf.SetJobName(name);
		}

		/// <summary>Turn speculative execution on or off for this job.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on, else <code>false</code>.
		/// </param>
		public virtual void SetSpeculativeExecution(bool speculativeExecution)
		{
			EnsureState(Job.JobState.Define);
			conf.SetSpeculativeExecution(speculativeExecution);
		}

		/// <summary>Turn speculative execution on or off for this job for map tasks.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on for map tasks,
		/// else <code>false</code>.
		/// </param>
		public virtual void SetMapSpeculativeExecution(bool speculativeExecution)
		{
			EnsureState(Job.JobState.Define);
			conf.SetMapSpeculativeExecution(speculativeExecution);
		}

		/// <summary>Turn speculative execution on or off for this job for reduce tasks.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on for reduce tasks,
		/// else <code>false</code>.
		/// </param>
		public virtual void SetReduceSpeculativeExecution(bool speculativeExecution)
		{
			EnsureState(Job.JobState.Define);
			conf.SetReduceSpeculativeExecution(speculativeExecution);
		}

		/// <summary>Specify whether job-setup and job-cleanup is needed for the job</summary>
		/// <param name="needed">
		/// If <code>true</code>, job-setup and job-cleanup will be
		/// considered from
		/// <see cref="OutputCommitter"/>
		/// 
		/// else ignored.
		/// </param>
		public virtual void SetJobSetupCleanupNeeded(bool needed)
		{
			EnsureState(Job.JobState.Define);
			conf.SetBoolean(SetupCleanupNeeded, needed);
		}

		/// <summary>Set the given set of archives</summary>
		/// <param name="archives">The list of archives that need to be localized</param>
		public virtual void SetCacheArchives(URI[] archives)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.SetCacheArchives(archives, conf);
		}

		/// <summary>Set the given set of files</summary>
		/// <param name="files">The list of files that need to be localized</param>
		public virtual void SetCacheFiles(URI[] files)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.SetCacheFiles(files, conf);
		}

		/// <summary>Add a archives to be localized</summary>
		/// <param name="uri">The uri of the cache to be localized</param>
		public virtual void AddCacheArchive(URI uri)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.AddCacheArchive(uri, conf);
		}

		/// <summary>Add a file to be localized</summary>
		/// <param name="uri">The uri of the cache to be localized</param>
		public virtual void AddCacheFile(URI uri)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.AddCacheFile(uri, conf);
		}

		/// <summary>
		/// Add an file path to the current set of classpath entries It adds the file
		/// to cache as well.
		/// </summary>
		/// <remarks>
		/// Add an file path to the current set of classpath entries It adds the file
		/// to cache as well.
		/// Files added with this method will not be unpacked while being added to the
		/// classpath.
		/// To add archives to classpath, use the
		/// <see cref="AddArchiveToClassPath(Org.Apache.Hadoop.FS.Path)"/>
		/// method instead.
		/// </remarks>
		/// <param name="file">Path of the file to be added</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddFileToClassPath(Path file)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.AddFileToClassPath(file, conf, file.GetFileSystem(conf));
		}

		/// <summary>Add an archive path to the current set of classpath entries.</summary>
		/// <remarks>
		/// Add an archive path to the current set of classpath entries. It adds the
		/// archive to cache as well.
		/// Archive files will be unpacked and added to the classpath
		/// when being distributed.
		/// </remarks>
		/// <param name="archive">Path of the archive to be added</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddArchiveToClassPath(Path archive)
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.AddArchiveToClassPath(archive, conf, archive.GetFileSystem(conf)
				);
		}

		/// <summary>
		/// Originally intended to enable symlinks, but currently symlinks cannot be
		/// disabled.
		/// </summary>
		[Obsolete]
		public virtual void CreateSymlink()
		{
			EnsureState(Job.JobState.Define);
			DistributedCache.CreateSymlink(conf);
		}

		/// <summary>
		/// Expert: Set the number of maximum attempts that will be made to run a
		/// map task.
		/// </summary>
		/// <param name="n">the number of attempts per map task.</param>
		public virtual void SetMaxMapAttempts(int n)
		{
			EnsureState(Job.JobState.Define);
			conf.SetMaxMapAttempts(n);
		}

		/// <summary>
		/// Expert: Set the number of maximum attempts that will be made to run a
		/// reduce task.
		/// </summary>
		/// <param name="n">the number of attempts per reduce task.</param>
		public virtual void SetMaxReduceAttempts(int n)
		{
			EnsureState(Job.JobState.Define);
			conf.SetMaxReduceAttempts(n);
		}

		/// <summary>
		/// Set whether the system should collect profiler information for some of
		/// the tasks in this job? The information is stored in the user log
		/// directory.
		/// </summary>
		/// <param name="newValue">true means it should be gathered</param>
		public virtual void SetProfileEnabled(bool newValue)
		{
			EnsureState(Job.JobState.Define);
			conf.SetProfileEnabled(newValue);
		}

		/// <summary>Set the profiler configuration arguments.</summary>
		/// <remarks>
		/// Set the profiler configuration arguments. If the string contains a '%s' it
		/// will be replaced with the name of the profiling output file when the task
		/// runs.
		/// This value is passed to the task child JVM on the command line.
		/// </remarks>
		/// <param name="value">the configuration string</param>
		public virtual void SetProfileParams(string value)
		{
			EnsureState(Job.JobState.Define);
			conf.SetProfileParams(value);
		}

		/// <summary>Set the ranges of maps or reduces to profile.</summary>
		/// <remarks>
		/// Set the ranges of maps or reduces to profile. setProfileEnabled(true)
		/// must also be called.
		/// </remarks>
		/// <param name="newValue">a set of integer ranges of the map ids</param>
		public virtual void SetProfileTaskRange(bool isMap, string newValue)
		{
			EnsureState(Job.JobState.Define);
			conf.SetProfileTaskRange(isMap, newValue);
		}

		/// <exception cref="System.IO.IOException"/>
		private void EnsureNotSet(string attr, string msg)
		{
			if (conf.Get(attr) != null)
			{
				throw new IOException(attr + " is incompatible with " + msg + " mode.");
			}
		}

		/// <summary>
		/// Sets the flag that will allow the JobTracker to cancel the HDFS delegation
		/// tokens upon job completion.
		/// </summary>
		/// <remarks>
		/// Sets the flag that will allow the JobTracker to cancel the HDFS delegation
		/// tokens upon job completion. Defaults to true.
		/// </remarks>
		public virtual void SetCancelDelegationTokenUponJobCompletion(bool value)
		{
			EnsureState(Job.JobState.Define);
			conf.SetBoolean(JobCancelDelegationToken, value);
		}

		/// <summary>
		/// Default to the new APIs unless they are explicitly set or the old mapper or
		/// reduce attributes are used.
		/// </summary>
		/// <exception cref="System.IO.IOException">if the configuration is inconsistant</exception>
		private void SetUseNewAPI()
		{
			int numReduces = conf.GetNumReduceTasks();
			string oldMapperClass = "mapred.mapper.class";
			string oldReduceClass = "mapred.reducer.class";
			conf.SetBooleanIfUnset("mapred.mapper.new-api", conf.Get(oldMapperClass) == null);
			if (conf.GetUseNewMapper())
			{
				string mode = "new map API";
				EnsureNotSet("mapred.input.format.class", mode);
				EnsureNotSet(oldMapperClass, mode);
				if (numReduces != 0)
				{
					EnsureNotSet("mapred.partitioner.class", mode);
				}
				else
				{
					EnsureNotSet("mapred.output.format.class", mode);
				}
			}
			else
			{
				string mode = "map compatability";
				EnsureNotSet(InputFormatClassAttr, mode);
				EnsureNotSet(MapClassAttr, mode);
				if (numReduces != 0)
				{
					EnsureNotSet(PartitionerClassAttr, mode);
				}
				else
				{
					EnsureNotSet(OutputFormatClassAttr, mode);
				}
			}
			if (numReduces != 0)
			{
				conf.SetBooleanIfUnset("mapred.reducer.new-api", conf.Get(oldReduceClass) == null
					);
				if (conf.GetUseNewReducer())
				{
					string mode = "new reduce API";
					EnsureNotSet("mapred.output.format.class", mode);
					EnsureNotSet(oldReduceClass, mode);
				}
				else
				{
					string mode = "reduce compatability";
					EnsureNotSet(OutputFormatClassAttr, mode);
					EnsureNotSet(ReduceClassAttr, mode);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private void Connect()
		{
			lock (this)
			{
				if (cluster == null)
				{
					cluster = ugi.DoAs(new _PrivilegedExceptionAction_1256(this));
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_1256 : PrivilegedExceptionAction<
			Cluster>
		{
			public _PrivilegedExceptionAction_1256(Job _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public Cluster Run()
			{
				return new Cluster(this._enclosing.GetConfiguration());
			}

			private readonly Job _enclosing;
		}

		internal virtual bool IsConnected()
		{
			return cluster != null;
		}

		/// <summary>Only for mocking via unit tests.</summary>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual JobSubmitter GetJobSubmitter(FileSystem fs, ClientProtocol submitClient
			)
		{
			return new JobSubmitter(fs, submitClient);
		}

		/// <summary>Submit the job to the cluster and return immediately.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual void Submit()
		{
			EnsureState(Job.JobState.Define);
			SetUseNewAPI();
			Connect();
			JobSubmitter submitter = GetJobSubmitter(cluster.GetFileSystem(), cluster.GetClient
				());
			status = ugi.DoAs(new _PrivilegedExceptionAction_1287(this, submitter));
			state = Job.JobState.Running;
			Log.Info("The url to track the job: " + GetTrackingURL());
		}

		private sealed class _PrivilegedExceptionAction_1287 : PrivilegedExceptionAction<
			JobStatus>
		{
			public _PrivilegedExceptionAction_1287(Job _enclosing, JobSubmitter submitter)
			{
				this._enclosing = _enclosing;
				this.submitter = submitter;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public JobStatus Run()
			{
				return submitter.SubmitJobInternal(this._enclosing, this._enclosing.cluster);
			}

			private readonly Job _enclosing;

			private readonly JobSubmitter submitter;
		}

		/// <summary>Submit the job to the cluster and wait for it to finish.</summary>
		/// <param name="verbose">print the progress to the user</param>
		/// <returns>true if the job succeeded</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if the communication with the
		/// <code>JobTracker</code> is lost
		/// </exception>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual bool WaitForCompletion(bool verbose)
		{
			if (state == Job.JobState.Define)
			{
				Submit();
			}
			if (verbose)
			{
				MonitorAndPrintJob();
			}
			else
			{
				// get the completion poll interval from the client.
				int completionPollIntervalMillis = Org.Apache.Hadoop.Mapreduce.Job.GetCompletionPollInterval
					(cluster.GetConf());
				while (!IsComplete())
				{
					try
					{
						Sharpen.Thread.Sleep(completionPollIntervalMillis);
					}
					catch (Exception)
					{
					}
				}
			}
			return IsSuccessful();
		}

		/// <summary>
		/// Monitor a job and print status in real-time as progress is made and tasks
		/// fail.
		/// </summary>
		/// <returns>true if the job succeeded</returns>
		/// <exception cref="System.IO.IOException">if communication to the JobTracker fails</exception>
		/// <exception cref="System.Exception"/>
		public virtual bool MonitorAndPrintJob()
		{
			string lastReport = null;
			Job.TaskStatusFilter filter;
			Configuration clientConf = GetConfiguration();
			filter = Org.Apache.Hadoop.Mapreduce.Job.GetTaskOutputFilter(clientConf);
			JobID jobId = GetJobID();
			Log.Info("Running job: " + jobId);
			int eventCounter = 0;
			bool profiling = GetProfileEnabled();
			Configuration.IntegerRanges mapRanges = GetProfileTaskRange(true);
			Configuration.IntegerRanges reduceRanges = GetProfileTaskRange(false);
			int progMonitorPollIntervalMillis = Org.Apache.Hadoop.Mapreduce.Job.GetProgressPollInterval
				(clientConf);
			/* make sure to report full progress after the job is done */
			bool reportedAfterCompletion = false;
			bool reportedUberMode = false;
			while (!IsComplete() || !reportedAfterCompletion)
			{
				if (IsComplete())
				{
					reportedAfterCompletion = true;
				}
				else
				{
					Sharpen.Thread.Sleep(progMonitorPollIntervalMillis);
				}
				if (status.GetState() == JobStatus.State.Prep)
				{
					continue;
				}
				if (!reportedUberMode)
				{
					reportedUberMode = true;
					Log.Info("Job " + jobId + " running in uber mode : " + IsUber());
				}
				string report = (" map " + StringUtils.FormatPercent(MapProgress(), 0) + " reduce "
					 + StringUtils.FormatPercent(ReduceProgress(), 0));
				if (!report.Equals(lastReport))
				{
					Log.Info(report);
					lastReport = report;
				}
				TaskCompletionEvent[] events = GetTaskCompletionEvents(eventCounter, 10);
				eventCounter += events.Length;
				PrintTaskEvents(events, filter, profiling, mapRanges, reduceRanges);
			}
			bool success = IsSuccessful();
			if (success)
			{
				Log.Info("Job " + jobId + " completed successfully");
			}
			else
			{
				Log.Info("Job " + jobId + " failed with state " + status.GetState() + " due to: "
					 + status.GetFailureInfo());
			}
			Counters counters = GetCounters();
			if (counters != null)
			{
				Log.Info(counters.ToString());
			}
			return success;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void PrintTaskEvents(TaskCompletionEvent[] events, Job.TaskStatusFilter filter
			, bool profiling, Configuration.IntegerRanges mapRanges, Configuration.IntegerRanges
			 reduceRanges)
		{
			foreach (TaskCompletionEvent @event in events)
			{
				switch (filter)
				{
					case Job.TaskStatusFilter.None:
					{
						break;
					}

					case Job.TaskStatusFilter.Succeeded:
					{
						if (@event.GetStatus() == TaskCompletionEvent.Status.Succeeded)
						{
							Log.Info(@event.ToString());
						}
						break;
					}

					case Job.TaskStatusFilter.Failed:
					{
						if (@event.GetStatus() == TaskCompletionEvent.Status.Failed)
						{
							Log.Info(@event.ToString());
							// Displaying the task diagnostic information
							TaskAttemptID taskId = @event.GetTaskAttemptId();
							string[] taskDiagnostics = GetTaskDiagnostics(taskId);
							if (taskDiagnostics != null)
							{
								foreach (string diagnostics in taskDiagnostics)
								{
									System.Console.Error.WriteLine(diagnostics);
								}
							}
						}
						break;
					}

					case Job.TaskStatusFilter.Killed:
					{
						if (@event.GetStatus() == TaskCompletionEvent.Status.Killed)
						{
							Log.Info(@event.ToString());
						}
						break;
					}

					case Job.TaskStatusFilter.All:
					{
						Log.Info(@event.ToString());
						break;
					}
				}
			}
		}

		/// <summary>The interval at which monitorAndPrintJob() prints status</summary>
		public static int GetProgressPollInterval(Configuration conf)
		{
			// Read progress monitor poll interval from config. Default is 1 second.
			int progMonitorPollIntervalMillis = conf.GetInt(ProgressMonitorPollIntervalKey, DefaultMonitorPollInterval
				);
			if (progMonitorPollIntervalMillis < 1)
			{
				Log.Warn(ProgressMonitorPollIntervalKey + " has been set to an invalid value; " +
					 " replacing with " + DefaultMonitorPollInterval);
				progMonitorPollIntervalMillis = DefaultMonitorPollInterval;
			}
			return progMonitorPollIntervalMillis;
		}

		/// <summary>The interval at which waitForCompletion() should check.</summary>
		public static int GetCompletionPollInterval(Configuration conf)
		{
			int completionPollIntervalMillis = conf.GetInt(CompletionPollIntervalKey, DefaultCompletionPollInterval
				);
			if (completionPollIntervalMillis < 1)
			{
				Log.Warn(CompletionPollIntervalKey + " has been set to an invalid value; " + "replacing with "
					 + DefaultCompletionPollInterval);
				completionPollIntervalMillis = DefaultCompletionPollInterval;
			}
			return completionPollIntervalMillis;
		}

		/// <summary>Get the task output filter.</summary>
		/// <param name="conf">the configuration.</param>
		/// <returns>the filter level.</returns>
		public static Job.TaskStatusFilter GetTaskOutputFilter(Configuration conf)
		{
			return Job.TaskStatusFilter.ValueOf(conf.Get(Org.Apache.Hadoop.Mapreduce.Job.OutputFilter
				, "FAILED"));
		}

		/// <summary>Modify the Configuration to set the task output filter.</summary>
		/// <param name="conf">the Configuration to modify.</param>
		/// <param name="newValue">the value to set.</param>
		public static void SetTaskOutputFilter(Configuration conf, Job.TaskStatusFilter newValue
			)
		{
			conf.Set(Org.Apache.Hadoop.Mapreduce.Job.OutputFilter, newValue.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual bool IsUber()
		{
			EnsureState(Job.JobState.Running);
			UpdateStatus();
			return status.IsUber();
		}

		/// <summary>Get the reservation to which the job is submitted to, if any</summary>
		/// <returns>
		/// the reservationId the identifier of the job's reservation, null if
		/// the job does not have any reservation associated with it
		/// </returns>
		public virtual ReservationId GetReservationId()
		{
			return reservationId;
		}

		/// <summary>Set the reservation to which the job is submitted to</summary>
		/// <param name="reservationId">the reservationId to set</param>
		public virtual void SetReservationId(ReservationId reservationId)
		{
			this.reservationId = reservationId;
		}
	}
}
