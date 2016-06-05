using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Describes the current status of a job.</summary>
	public class JobStatus : Writable, ICloneable
	{
		static JobStatus()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Mapreduce.JobStatus), new _WritableFactory_47
				());
		}

		private sealed class _WritableFactory_47 : WritableFactory
		{
			public _WritableFactory_47()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Mapreduce.JobStatus();
			}
		}

		/// <summary>Current state of the job</summary>
		[System.Serializable]
		public sealed class State
		{
			public static readonly JobStatus.State Running = new JobStatus.State(1);

			public static readonly JobStatus.State Succeeded = new JobStatus.State(2);

			public static readonly JobStatus.State Failed = new JobStatus.State(3);

			public static readonly JobStatus.State Prep = new JobStatus.State(4);

			public static readonly JobStatus.State Killed = new JobStatus.State(5);

			internal int value;

			internal State(int value)
			{
				this.value = value;
			}

			public int GetValue()
			{
				return JobStatus.State.value;
			}
		}

		private JobID jobid;

		private float mapProgress;

		private float reduceProgress;

		private float cleanupProgress;

		private float setupProgress;

		private JobStatus.State runState;

		private long startTime;

		private string user;

		private string queue;

		private JobPriority priority;

		private string schedulingInfo = "NA";

		private string failureInfo = "NA";

		private IDictionary<JobACL, AccessControlList> jobACLs = new Dictionary<JobACL, AccessControlList
			>();

		private string jobName;

		private string jobFile;

		private long finishTime;

		private bool isRetired;

		private string historyFile = string.Empty;

		private string trackingUrl = string.Empty;

		private int numUsedSlots;

		private int numReservedSlots;

		private int usedMem;

		private int reservedMem;

		private int neededMem;

		private bool isUber;

		public JobStatus()
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
			, float cleanupProgress, JobStatus.State runState, JobPriority jp, string user, 
			string jobName, string jobFile, string trackingUrl)
			: this(jobid, setupProgress, mapProgress, reduceProgress, cleanupProgress, runState
				, jp, user, jobName, "default", jobFile, trackingUrl, false)
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
		/// <param name="queue">queue name</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, JobStatus.State runState, JobPriority jp, string user, 
			string jobName, string queue, string jobFile, string trackingUrl)
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
		/// <param name="queue">queue name</param>
		/// <param name="jobFile">job configuration file.</param>
		/// <param name="trackingUrl">link to the web-ui for details of the job.</param>
		/// <param name="isUber">Whether job running in uber mode</param>
		public JobStatus(JobID jobid, float setupProgress, float mapProgress, float reduceProgress
			, float cleanupProgress, JobStatus.State runState, JobPriority jp, string user, 
			string jobName, string queue, string jobFile, string trackingUrl, bool isUber)
		{
			this.jobid = jobid;
			this.setupProgress = setupProgress;
			this.mapProgress = mapProgress;
			this.reduceProgress = reduceProgress;
			this.cleanupProgress = cleanupProgress;
			this.runState = runState;
			this.user = user;
			this.queue = queue;
			if (jp == null)
			{
				throw new ArgumentException("Job Priority cannot be null.");
			}
			priority = jp;
			this.jobName = jobName;
			this.jobFile = jobFile;
			this.trackingUrl = trackingUrl;
			this.isUber = isUber;
		}

		/// <summary>Sets the map progress of this job</summary>
		/// <param name="p">The value of map progress to set to</param>
		protected internal virtual void SetMapProgress(float p)
		{
			lock (this)
			{
				this.mapProgress = (float)Math.Min(1.0, Math.Max(0.0, p));
			}
		}

		/// <summary>Sets the cleanup progress of this job</summary>
		/// <param name="p">The value of cleanup progress to set to</param>
		protected internal virtual void SetCleanupProgress(float p)
		{
			lock (this)
			{
				this.cleanupProgress = (float)Math.Min(1.0, Math.Max(0.0, p));
			}
		}

		/// <summary>Sets the setup progress of this job</summary>
		/// <param name="p">The value of setup progress to set to</param>
		protected internal virtual void SetSetupProgress(float p)
		{
			lock (this)
			{
				this.setupProgress = (float)Math.Min(1.0, Math.Max(0.0, p));
			}
		}

		/// <summary>Sets the reduce progress of this Job</summary>
		/// <param name="p">The value of reduce progress to set to</param>
		protected internal virtual void SetReduceProgress(float p)
		{
			lock (this)
			{
				this.reduceProgress = (float)Math.Min(1.0, Math.Max(0.0, p));
			}
		}

		/// <summary>Set the priority of the job, defaulting to NORMAL.</summary>
		/// <param name="jp">new job priority</param>
		protected internal virtual void SetPriority(JobPriority jp)
		{
			lock (this)
			{
				if (jp == null)
				{
					throw new ArgumentException("Job priority cannot be null.");
				}
				priority = jp;
			}
		}

		/// <summary>Set the finish time of the job</summary>
		/// <param name="finishTime">The finishTime of the job</param>
		protected internal virtual void SetFinishTime(long finishTime)
		{
			lock (this)
			{
				this.finishTime = finishTime;
			}
		}

		/// <summary>Set the job history file url for a completed job</summary>
		protected internal virtual void SetHistoryFile(string historyFile)
		{
			lock (this)
			{
				this.historyFile = historyFile;
			}
		}

		/// <summary>Set the link to the web-ui for details of the job.</summary>
		protected internal virtual void SetTrackingUrl(string trackingUrl)
		{
			lock (this)
			{
				this.trackingUrl = trackingUrl;
			}
		}

		/// <summary>Set the job retire flag to true.</summary>
		protected internal virtual void SetRetired()
		{
			lock (this)
			{
				this.isRetired = true;
			}
		}

		/// <summary>Change the current run state of the job.</summary>
		protected internal virtual void SetState(JobStatus.State state)
		{
			lock (this)
			{
				this.runState = state;
			}
		}

		/// <summary>Set the start time of the job</summary>
		/// <param name="startTime">The startTime of the job</param>
		protected internal virtual void SetStartTime(long startTime)
		{
			lock (this)
			{
				this.startTime = startTime;
			}
		}

		/// <param name="userName">The username of the job</param>
		protected internal virtual void SetUsername(string userName)
		{
			lock (this)
			{
				this.user = userName;
			}
		}

		/// <summary>Used to set the scheduling information associated to a particular Job.</summary>
		/// <param name="schedulingInfo">Scheduling information of the job</param>
		protected internal virtual void SetSchedulingInfo(string schedulingInfo)
		{
			lock (this)
			{
				this.schedulingInfo = schedulingInfo;
			}
		}

		/// <summary>Set the job acls.</summary>
		/// <param name="acls">
		/// 
		/// <see cref="System.Collections.IDictionary{K, V}"/>
		/// from
		/// <see cref="JobACL"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// </param>
		protected internal virtual void SetJobACLs(IDictionary<JobACL, AccessControlList>
			 acls)
		{
			lock (this)
			{
				this.jobACLs = acls;
			}
		}

		/// <summary>Set queue name</summary>
		/// <param name="queue">queue name</param>
		protected internal virtual void SetQueue(string queue)
		{
			lock (this)
			{
				this.queue = queue;
			}
		}

		/// <summary>Set diagnostic information.</summary>
		/// <param name="failureInfo">diagnostic information</param>
		protected internal virtual void SetFailureInfo(string failureInfo)
		{
			lock (this)
			{
				this.failureInfo = failureInfo;
			}
		}

		/// <summary>Get queue name</summary>
		/// <returns>queue name</returns>
		public virtual string GetQueue()
		{
			lock (this)
			{
				return queue;
			}
		}

		/// <returns>Percentage of progress in maps</returns>
		public virtual float GetMapProgress()
		{
			lock (this)
			{
				return mapProgress;
			}
		}

		/// <returns>Percentage of progress in cleanup</returns>
		public virtual float GetCleanupProgress()
		{
			lock (this)
			{
				return cleanupProgress;
			}
		}

		/// <returns>Percentage of progress in setup</returns>
		public virtual float GetSetupProgress()
		{
			lock (this)
			{
				return setupProgress;
			}
		}

		/// <returns>Percentage of progress in reduce</returns>
		public virtual float GetReduceProgress()
		{
			lock (this)
			{
				return reduceProgress;
			}
		}

		/// <returns>running state of the job</returns>
		public virtual JobStatus.State GetState()
		{
			lock (this)
			{
				return runState;
			}
		}

		/// <returns>start time of the job</returns>
		public virtual long GetStartTime()
		{
			lock (this)
			{
				return startTime;
			}
		}

		public virtual object Clone()
		{
			return base.MemberwiseClone();
		}

		// Shouldn't happen since we do implement Clonable
		/// <returns>The jobid of the Job</returns>
		public virtual JobID GetJobID()
		{
			return jobid;
		}

		/// <returns>the username of the job</returns>
		public virtual string GetUsername()
		{
			lock (this)
			{
				return this.user;
			}
		}

		/// <summary>Gets the Scheduling information associated to a particular Job.</summary>
		/// <returns>the scheduling information of the job</returns>
		public virtual string GetSchedulingInfo()
		{
			lock (this)
			{
				return schedulingInfo;
			}
		}

		/// <summary>Get the job acls.</summary>
		/// <returns>
		/// a
		/// <see cref="System.Collections.IDictionary{K, V}"/>
		/// from
		/// <see cref="JobACL"/>
		/// to
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// </returns>
		public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
		{
			lock (this)
			{
				return jobACLs;
			}
		}

		/// <summary>Return the priority of the job</summary>
		/// <returns>job priority</returns>
		public virtual JobPriority GetPriority()
		{
			lock (this)
			{
				return priority;
			}
		}

		/// <summary>Gets any available info on the reason of failure of the job.</summary>
		/// <returns>diagnostic information on why a job might have failed.</returns>
		public virtual string GetFailureInfo()
		{
			lock (this)
			{
				return this.failureInfo;
			}
		}

		/// <summary>Returns true if the status is for a completed job.</summary>
		public virtual bool IsJobComplete()
		{
			lock (this)
			{
				return (runState == JobStatus.State.Succeeded || runState == JobStatus.State.Failed
					 || runState == JobStatus.State.Killed);
			}
		}

		///////////////////////////////////////
		// Writable
		///////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			lock (this)
			{
				jobid.Write(@out);
				@out.WriteFloat(setupProgress);
				@out.WriteFloat(mapProgress);
				@out.WriteFloat(reduceProgress);
				@out.WriteFloat(cleanupProgress);
				WritableUtils.WriteEnum(@out, runState);
				@out.WriteLong(startTime);
				Text.WriteString(@out, user);
				WritableUtils.WriteEnum(@out, priority);
				Text.WriteString(@out, schedulingInfo);
				@out.WriteLong(finishTime);
				@out.WriteBoolean(isRetired);
				Text.WriteString(@out, historyFile);
				Text.WriteString(@out, jobName);
				Text.WriteString(@out, trackingUrl);
				Text.WriteString(@out, jobFile);
				@out.WriteBoolean(isUber);
				// Serialize the job's ACLs
				@out.WriteInt(jobACLs.Count);
				foreach (KeyValuePair<JobACL, AccessControlList> entry in jobACLs)
				{
					WritableUtils.WriteEnum(@out, entry.Key);
					entry.Value.Write(@out);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			lock (this)
			{
				this.jobid = new JobID();
				this.jobid.ReadFields(@in);
				this.setupProgress = @in.ReadFloat();
				this.mapProgress = @in.ReadFloat();
				this.reduceProgress = @in.ReadFloat();
				this.cleanupProgress = @in.ReadFloat();
				this.runState = WritableUtils.ReadEnum<JobStatus.State>(@in);
				this.startTime = @in.ReadLong();
				this.user = StringInterner.WeakIntern(Text.ReadString(@in));
				this.priority = WritableUtils.ReadEnum<JobPriority>(@in);
				this.schedulingInfo = StringInterner.WeakIntern(Text.ReadString(@in));
				this.finishTime = @in.ReadLong();
				this.isRetired = @in.ReadBoolean();
				this.historyFile = StringInterner.WeakIntern(Text.ReadString(@in));
				this.jobName = StringInterner.WeakIntern(Text.ReadString(@in));
				this.trackingUrl = StringInterner.WeakIntern(Text.ReadString(@in));
				this.jobFile = StringInterner.WeakIntern(Text.ReadString(@in));
				this.isUber = @in.ReadBoolean();
				// De-serialize the job's ACLs
				int numACLs = @in.ReadInt();
				for (int i = 0; i < numACLs; i++)
				{
					JobACL aclType = WritableUtils.ReadEnum<JobACL>(@in);
					AccessControlList acl = new AccessControlList(" ");
					acl.ReadFields(@in);
					this.jobACLs[aclType] = acl;
				}
			}
		}

		/// <summary>Get the user-specified job name.</summary>
		public virtual string GetJobName()
		{
			return jobName;
		}

		/// <summary>Get the configuration file for the job.</summary>
		public virtual string GetJobFile()
		{
			return jobFile;
		}

		/// <summary>Get the link to the web-ui for details of the job.</summary>
		public virtual string GetTrackingUrl()
		{
			lock (this)
			{
				return trackingUrl;
			}
		}

		/// <summary>Get the finish time of the job.</summary>
		public virtual long GetFinishTime()
		{
			lock (this)
			{
				return finishTime;
			}
		}

		/// <summary>Check whether the job has retired.</summary>
		public virtual bool IsRetired()
		{
			lock (this)
			{
				return isRetired;
			}
		}

		/// <returns>
		/// the job history file name for a completed job. If job is not
		/// completed or history file not available then return null.
		/// </returns>
		public virtual string GetHistoryFile()
		{
			lock (this)
			{
				return historyFile;
			}
		}

		/// <returns>number of used mapred slots</returns>
		public virtual int GetNumUsedSlots()
		{
			return numUsedSlots;
		}

		/// <param name="n">number of used mapred slots</param>
		public virtual void SetNumUsedSlots(int n)
		{
			numUsedSlots = n;
		}

		/// <returns>the number of reserved slots</returns>
		public virtual int GetNumReservedSlots()
		{
			return numReservedSlots;
		}

		/// <param name="n">the number of reserved slots</param>
		public virtual void SetNumReservedSlots(int n)
		{
			this.numReservedSlots = n;
		}

		/// <returns>the used memory</returns>
		public virtual int GetUsedMem()
		{
			return usedMem;
		}

		/// <param name="m">the used memory</param>
		public virtual void SetUsedMem(int m)
		{
			this.usedMem = m;
		}

		/// <returns>the reserved memory</returns>
		public virtual int GetReservedMem()
		{
			return reservedMem;
		}

		/// <param name="r">the reserved memory</param>
		public virtual void SetReservedMem(int r)
		{
			this.reservedMem = r;
		}

		/// <returns>the needed memory</returns>
		public virtual int GetNeededMem()
		{
			return neededMem;
		}

		/// <param name="n">the needed memory</param>
		public virtual void SetNeededMem(int n)
		{
			this.neededMem = n;
		}

		/// <summary>Whether job running in uber mode</summary>
		/// <returns>job in uber-mode</returns>
		public virtual bool IsUber()
		{
			lock (this)
			{
				return isUber;
			}
		}

		/// <summary>Set uber-mode flag</summary>
		/// <param name="isUber">Whether job running in uber-mode</param>
		public virtual void SetUber(bool isUber)
		{
			lock (this)
			{
				this.isUber = isUber;
			}
		}

		public override string ToString()
		{
			StringBuilder buffer = new StringBuilder();
			buffer.Append("job-id : " + jobid);
			buffer.Append("uber-mode : " + isUber);
			buffer.Append("map-progress : " + mapProgress);
			buffer.Append("reduce-progress : " + reduceProgress);
			buffer.Append("cleanup-progress : " + cleanupProgress);
			buffer.Append("setup-progress : " + setupProgress);
			buffer.Append("runstate : " + runState);
			buffer.Append("start-time : " + startTime);
			buffer.Append("user-name : " + user);
			buffer.Append("priority : " + priority);
			buffer.Append("scheduling-info : " + schedulingInfo);
			buffer.Append("num-used-slots" + numUsedSlots);
			buffer.Append("num-reserved-slots" + numReservedSlots);
			buffer.Append("used-mem" + usedMem);
			buffer.Append("reserved-mem" + reservedMem);
			buffer.Append("needed-mem" + neededMem);
			return buffer.ToString();
		}
	}
}
