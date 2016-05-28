using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Jobcontrol
{
	public class JobControl : Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.JobControl
	{
		/// <summary>Construct a job control for a group of jobs.</summary>
		/// <param name="groupName">a name identifying this group</param>
		public JobControl(string groupName)
			: base(groupName)
		{
		}

		internal static AList<Job> CastToJobList(IList<ControlledJob> cjobs)
		{
			AList<Job> ret = new AList<Job>();
			foreach (ControlledJob job in cjobs)
			{
				ret.AddItem((Job)job);
			}
			return ret;
		}

		/// <returns>the jobs in the waiting state</returns>
		public virtual AList<Job> GetWaitingJobs()
		{
			return CastToJobList(base.GetWaitingJobList());
		}

		/// <returns>the jobs in the running state</returns>
		public virtual AList<Job> GetRunningJobs()
		{
			return CastToJobList(base.GetRunningJobList());
		}

		/// <returns>the jobs in the ready state</returns>
		public virtual AList<Job> GetReadyJobs()
		{
			return CastToJobList(base.GetReadyJobsList());
		}

		/// <returns>the jobs in the success state</returns>
		public virtual AList<Job> GetSuccessfulJobs()
		{
			return CastToJobList(base.GetSuccessfulJobList());
		}

		public virtual AList<Job> GetFailedJobs()
		{
			return CastToJobList(base.GetFailedJobList());
		}

		/// <summary>Add a collection of jobs</summary>
		/// <param name="jobs"/>
		public virtual void AddJobs(ICollection<Job> jobs)
		{
			foreach (Job job in jobs)
			{
				AddJob(job);
			}
		}

		/// <returns>the thread state</returns>
		public virtual int GetState()
		{
			JobControl.ThreadState state = base.GetThreadState();
			if (state == JobControl.ThreadState.Running)
			{
				return 0;
			}
			if (state == JobControl.ThreadState.Suspended)
			{
				return 1;
			}
			if (state == JobControl.ThreadState.Stopped)
			{
				return 2;
			}
			if (state == JobControl.ThreadState.Stopping)
			{
				return 3;
			}
			if (state == JobControl.ThreadState.Ready)
			{
				return 4;
			}
			return -1;
		}
	}
}
