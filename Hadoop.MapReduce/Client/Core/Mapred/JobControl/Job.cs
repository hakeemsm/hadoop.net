using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Jobcontrol
{
	public class Job : ControlledJob
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Jobcontrol.Job
			));

		public const int Success = 0;

		public const int Waiting = 1;

		public const int Running = 2;

		public const int Ready = 3;

		public const int Failed = 4;

		public const int DependentFailed = 5;

		/// <summary>Construct a job.</summary>
		/// <param name="jobConf">a mapred job configuration representing a job to be executed.
		/// 	</param>
		/// <param name="dependingJobs">an array of jobs the current job depends on</param>
		/// <exception cref="System.IO.IOException"/>
		public Job(JobConf jobConf, AList<object> dependingJobs)
			: base(Org.Apache.Hadoop.Mapreduce.Job.GetInstance(jobConf), (IList<ControlledJob
				>)dependingJobs)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public Job(JobConf conf)
			: base(conf)
		{
		}

		/// <returns>the mapred ID of this job as assigned by the mapred framework.</returns>
		public virtual JobID GetAssignedJobID()
		{
			JobID temp = base.GetMapredJobId();
			if (temp == null)
			{
				return null;
			}
			return JobID.Downgrade(temp);
		}

		[System.ObsoleteAttribute(@"setAssignedJobID should not be called. JOBID is set by the framework."
			)]
		public virtual void SetAssignedJobID(JobID mapredJobID)
		{
		}

		// do nothing
		/// <returns>the mapred job conf of this job</returns>
		public virtual JobConf GetJobConf()
		{
			lock (this)
			{
				return new JobConf(base.GetJob().GetConfiguration());
			}
		}

		/// <summary>Set the mapred job conf for this job.</summary>
		/// <param name="jobConf">the mapred job conf for this job.</param>
		public virtual void SetJobConf(JobConf jobConf)
		{
			lock (this)
			{
				try
				{
					base.SetJob(Org.Apache.Hadoop.Mapreduce.Job.GetInstance(jobConf));
				}
				catch (IOException ioe)
				{
					Log.Info("Exception" + ioe);
				}
			}
		}

		/// <returns>the state of this job</returns>
		public virtual int GetState()
		{
			lock (this)
			{
				ControlledJob.State state = base.GetJobState();
				if (state == ControlledJob.State.Success)
				{
					return Success;
				}
				if (state == ControlledJob.State.Waiting)
				{
					return Waiting;
				}
				if (state == ControlledJob.State.Running)
				{
					return Running;
				}
				if (state == ControlledJob.State.Ready)
				{
					return Ready;
				}
				if (state == ControlledJob.State.Failed)
				{
					return Failed;
				}
				if (state == ControlledJob.State.DependentFailed)
				{
					return DependentFailed;
				}
				return -1;
			}
		}

		/// <summary>
		/// This is a no-op function, Its a behavior change from 1.x We no more can
		/// change the state from job
		/// </summary>
		/// <param name="state">the new state for this job.</param>
		[Obsolete]
		protected internal virtual void SetState(int state)
		{
			lock (this)
			{
			}
		}

		// No-Op, we dont want to change the sate
		/// <summary>Add a job to this jobs' dependency list.</summary>
		/// <remarks>
		/// Add a job to this jobs' dependency list.
		/// Dependent jobs can only be added while a Job
		/// is waiting to run, not during or afterwards.
		/// </remarks>
		/// <param name="dependingJob">Job that this Job depends on.</param>
		/// <returns><tt>true</tt> if the Job was added.</returns>
		public virtual bool AddDependingJob(Org.Apache.Hadoop.Mapred.Jobcontrol.Job dependingJob
			)
		{
			lock (this)
			{
				return base.AddDependingJob(dependingJob);
			}
		}

		/// <returns>the job client of this job</returns>
		public virtual JobClient GetJobClient()
		{
			try
			{
				return new JobClient(base.GetJob().GetConfiguration());
			}
			catch (IOException)
			{
				return null;
			}
		}

		/// <returns>the depending jobs of this job</returns>
		public virtual AList<Org.Apache.Hadoop.Mapred.Jobcontrol.Job> GetDependingJobs()
		{
			return JobControl.CastToJobList(base.GetDependentJobs());
		}

		/// <returns>the mapred ID of this job as assigned by the mapred framework.</returns>
		public virtual string GetMapredJobID()
		{
			lock (this)
			{
				if (base.GetMapredJobId() != null)
				{
					return base.GetMapredJobId().ToString();
				}
				return null;
			}
		}

		/// <summary>This is no-op method for backward compatibility.</summary>
		/// <remarks>
		/// This is no-op method for backward compatibility. It's a behavior change
		/// from 1.x, we can not change job ids from job.
		/// </remarks>
		/// <param name="mapredJobID">the mapred job ID for this job.</param>
		[Obsolete]
		public virtual void SetMapredJobID(string mapredJobID)
		{
			lock (this)
			{
				SetAssignedJobID(((JobID)JobID.ForName(mapredJobID)));
			}
		}
	}
}
