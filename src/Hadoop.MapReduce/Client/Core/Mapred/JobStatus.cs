using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Describes the current status of a job.</summary>
	/// <remarks>
	/// Describes the current status of a job.  This is
	/// not intended to be a comprehensive piece of data.
	/// For that, look at JobProfile.
	/// </remarks>
	public class JobStatus : Org.Apache.Hadoop.Mapreduce.JobStatus
	{
		public static readonly int Running = JobStatus.State.Running.GetValue();

		public static readonly int Succeeded = JobStatus.State.Succeeded.GetValue();

		public static readonly int Failed = JobStatus.State.Failed.GetValue();

		public static readonly int Prep = JobStatus.State.Prep.GetValue();

		public static readonly int Killed = JobStatus.State.Killed.GetValue();

		private const string Unknown = "UNKNOWN";

		private static readonly string[] runStates = new string[] { Unknown, "RUNNING", "SUCCEEDED"
			, "FAILED", "PREP", "KILLED" };

		/// <summary>Helper method to get human-readable state of the job.</summary>
		/// <param name="state">job state</param>
		/// <returns>human-readable state of the job</returns>
		public static string GetJobRunState(int state)
		{
			if (state < 1 || state >= runStates.Length)
			{
				return Unknown;
			}
			return runStates[state];
		}

		internal static JobStatus.State GetEnum(int state)
		{
			switch (state)
			{
				case 1:
				{
					return JobStatus.State.Running;
				}

				case 2:
				{
					return JobStatus.State.Succeeded;
				}

				case 3:
				{
					return JobStatus.State.Failed;
				}

				case 4:
				{
					return JobStatus.State.Prep;
				}

				case 5:
				{
					return JobStatus.State.Killed;
				}
			}
			return null;
		}

		public JobStatus()
		{
		}

		[Obsolete]
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, float cleanupProgress
			, int runState)
			: this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, null, null, 
				null, null)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="runState">The current state of the job</param>
		[Obsolete]
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, int runState
			)
			: this(jobid, mapProgress, reduceProgress, runState, null, null, null, null)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		[Obsolete]
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, float cleanupProgress
			, int runState, JobPriority jp)
			: this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, jp, null, null
				, null, null)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="setupProgress">The progress made on the setup</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on the cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		[Obsolete]
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, int runState, JobPriority jp)
			: this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, runState
				, jp, null, null, null, null)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, float cleanupProgress
			, int runState, string user, string jobName, string jobFile, string trackingUrl)
			: this(jobid, mapProgress, reduceProgress, cleanupProgress, runState, JobPriority
				.Normal, user, jobName, jobFile, trackingUrl)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, int runState
			, string user, string jobName, string jobFile, string trackingUrl)
			: this(jobid, mapProgress, reduceProgress, 0.0f, runState, user, jobName, jobFile
				, trackingUrl)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float mapProgress, float reduceProgress, float cleanupProgress
			, int runState, JobPriority jp, string user, string jobName, string jobFile, string
			 trackingUrl)
			: this(jobid, 0.0f, mapProgress, reduceProgress, cleanupProgress, runState, jp, user
				, jobName, jobFile, trackingUrl)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="setupProgress">The progress made on the setup</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on the cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, int runState, JobPriority jp, string user, string jobName
			, string jobFile, string trackingUrl)
			: this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, runState
				, jp, user, jobName, "default", jobFile, trackingUrl)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="setupProgress">The progress made on the setup</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on the cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		/// <param name="isUber">Whether job running in uber mode</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, int runState, JobPriority jp, string user, string jobName
			, string jobFile, string trackingUrl, bool isUber)
			: this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, runState
				, jp, user, jobName, "default", jobFile, trackingUrl, isUber)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="setupProgress">The progress made on the setup</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on the cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="queue">job queue name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, int runState, JobPriority jp, string user, string jobName
			, string queue, string jobFile, string trackingUrl)
			: this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, runState
				, jp, user, jobName, queue, jobFile, trackingUrl, false)
		{
		}

		/// <summary>Create a job status object for a given jobid.</summary>
		/// <param name="jobid">The jobid of the job</param>
		/// <param name="setupProgress">The progress made on the setup</param>
		/// <param name="mapProgress">The progress made on the maps</param>
		/// <param name="reduceProgress">The progress made on the reduces</param>
		/// <param name="cleanupProgress">The progress made on the cleanup</param>
		/// <param name="runState">The current state of the job</param>
		/// <param name="jp">Priority of the job.</param>
		/// <param name="user">userid of the person who submitted the job.</param>
		/// <param name="jobName">user-specified job name.</param>
		/// <param name="queue">job queue name.</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		/// <param name="isUber">Whether job running in uber mode</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, int runState, JobPriority jp, string user, string jobName
			, string queue, string jobFile, string trackingUrl, bool isUber)
			: base(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, GetEnum
				(runState), JobPriority.ValueOf(jp.ToString()), user, jobName, queue, jobFile, trackingUrl
				, isUber)
		{
		}

		public static Org.Apache.Hadoop.Mapred.JobStatus Downgrade(Org.Apache.Hadoop.Mapreduce.JobStatus
			 stat)
		{
			Org.Apache.Hadoop.Mapred.JobStatus old = new Org.Apache.Hadoop.Mapred.JobStatus(JobID
				.Downgrade(stat.GetJobID()), stat.GetSetupProgress(), stat.GetMapProgress(), stat
				.GetReduceProgress(), stat.GetCleanupProgress(), stat.GetState().GetValue(), JobPriority
				.ValueOf(stat.GetPriority().ToString()), stat.GetUsername(), stat.GetJobName(), 
				stat.GetQueue(), stat.GetJobFile(), stat.GetTrackingUrl(), stat.IsUber());
			old.SetStartTime(stat.GetStartTime());
			old.SetFinishTime(stat.GetFinishTime());
			old.SetSchedulingInfo(stat.GetSchedulingInfo());
			old.SetHistoryFile(stat.GetHistoryFile());
			return old;
		}

		[System.ObsoleteAttribute(@"use getJobID instead")]
		public virtual string GetJobId()
		{
			return ((JobID)GetJobID()).ToString();
		}

		/// <returns>The jobid of the Job</returns>
		public override JobID GetJobID()
		{
			return JobID.Downgrade(base.GetJobID());
		}

		/// <summary>Return the priority of the job</summary>
		/// <returns>job priority</returns>
		public virtual JobPriority GetJobPriority()
		{
			lock (this)
			{
				return JobPriority.ValueOf(base.GetPriority().ToString());
			}
		}

		/// <summary>Sets the map progress of this job</summary>
		/// <param name="p">The value of map progress to set to</param>
		protected internal override void SetMapProgress(float p)
		{
			lock (this)
			{
				base.SetMapProgress(p);
			}
		}

		/// <summary>Sets the cleanup progress of this job</summary>
		/// <param name="p">The value of cleanup progress to set to</param>
		protected internal override void SetCleanupProgress(float p)
		{
			lock (this)
			{
				base.SetCleanupProgress(p);
			}
		}

		/// <summary>Sets the setup progress of this job</summary>
		/// <param name="p">The value of setup progress to set to</param>
		protected internal override void SetSetupProgress(float p)
		{
			lock (this)
			{
				base.SetSetupProgress(p);
			}
		}

		/// <summary>Sets the reduce progress of this Job</summary>
		/// <param name="p">The value of reduce progress to set to</param>
		protected internal override void SetReduceProgress(float p)
		{
			lock (this)
			{
				base.SetReduceProgress(p);
			}
		}

		/// <summary>Set the finish time of the job</summary>
		/// <param name="finishTime">The finishTime of the job</param>
		protected internal override void SetFinishTime(long finishTime)
		{
			lock (this)
			{
				base.SetFinishTime(finishTime);
			}
		}

		/// <summary>Set the job history file url for a completed job</summary>
		protected internal override void SetHistoryFile(string historyFile)
		{
			lock (this)
			{
				base.SetHistoryFile(historyFile);
			}
		}

		/// <summary>Set the link to the web-ui for details of the job.</summary>
		protected internal override void SetTrackingUrl(string trackingUrl)
		{
			lock (this)
			{
				base.SetTrackingUrl(trackingUrl);
			}
		}

		/// <summary>Set the job retire flag to true.</summary>
		protected internal override void SetRetired()
		{
			lock (this)
			{
				base.SetRetired();
			}
		}

		/// <summary>Change the current run state of the job.</summary>
		/// <remarks>
		/// Change the current run state of the job.
		/// The setter is public to be compatible with M/R 1.x, however, it should be
		/// used internally.
		/// </remarks>
		/// <param name="state">the state of the job</param>
		[InterfaceAudience.Private]
		public virtual void SetRunState(int state)
		{
			lock (this)
			{
				base.SetState(GetEnum(state));
			}
		}

		/// <returns>running state of the job</returns>
		public virtual int GetRunState()
		{
			lock (this)
			{
				return base.GetState().GetValue();
			}
		}

		/// <summary>Set the start time of the job</summary>
		/// <param name="startTime">The startTime of the job</param>
		protected internal override void SetStartTime(long startTime)
		{
			lock (this)
			{
				base.SetStartTime(startTime);
			}
		}

		/// <param name="userName">The username of the job</param>
		protected internal override void SetUsername(string userName)
		{
			lock (this)
			{
				base.SetUsername(userName);
			}
		}

		/// <summary>Used to set the scheduling information associated to a particular Job.</summary>
		/// <remarks>
		/// Used to set the scheduling information associated to a particular Job.
		/// The setter is public to be compatible with M/R 1.x, however, it should be
		/// used internally.
		/// </remarks>
		/// <param name="schedulingInfo">Scheduling information of the job</param>
		[InterfaceAudience.Private]
		protected internal override void SetSchedulingInfo(string schedulingInfo)
		{
			lock (this)
			{
				base.SetSchedulingInfo(schedulingInfo);
			}
		}

		protected internal override void SetJobACLs(IDictionary<JobACL, AccessControlList
			> acls)
		{
			lock (this)
			{
				base.SetJobACLs(acls);
			}
		}

		protected internal override void SetFailureInfo(string failureInfo)
		{
			lock (this)
			{
				base.SetFailureInfo(failureInfo);
			}
		}

		/// <summary>Set the priority of the job, defaulting to NORMAL.</summary>
		/// <param name="jp">new job priority</param>
		public virtual void SetJobPriority(JobPriority jp)
		{
			lock (this)
			{
				base.SetPriority(JobPriority.ValueOf(jp.ToString()));
			}
		}

		/// <returns>Percentage of progress in maps</returns>
		public virtual float MapProgress()
		{
			lock (this)
			{
				return base.GetMapProgress();
			}
		}

		/// <returns>Percentage of progress in cleanup</returns>
		public virtual float CleanupProgress()
		{
			lock (this)
			{
				return base.GetCleanupProgress();
			}
		}

		/// <returns>Percentage of progress in setup</returns>
		public virtual float SetupProgress()
		{
			lock (this)
			{
				return base.GetSetupProgress();
			}
		}

		/// <returns>Percentage of progress in reduce</returns>
		public virtual float ReduceProgress()
		{
			lock (this)
			{
				return base.GetReduceProgress();
			}
		}

		// A utility to convert new job runstates to the old ones.
		internal static int GetOldNewJobRunState(JobStatus.State state)
		{
			return state.GetValue();
		}
	}
}
