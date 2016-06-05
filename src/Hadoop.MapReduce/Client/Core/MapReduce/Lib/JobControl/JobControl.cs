using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapred.Jobcontrol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol
{
	/// <summary>This class encapsulates a set of MapReduce jobs and its dependency.</summary>
	/// <remarks>
	/// This class encapsulates a set of MapReduce jobs and its dependency.
	/// It tracks the states of the jobs by placing them into different tables
	/// according to their states.
	/// This class provides APIs for the client app to add a job to the group
	/// and to get the jobs in the group in different states. When a job is
	/// added, an ID unique to the group is assigned to the job.
	/// This class has a thread that submits jobs when they become ready,
	/// monitors the states of the running jobs, and updates the states of jobs
	/// based on the state changes of their depending jobs states. The class
	/// provides APIs for suspending/resuming the thread, and
	/// for stopping the thread.
	/// </remarks>
	public class JobControl : Runnable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.JobControl
			));

		public enum ThreadState
		{
			Running,
			Suspended,
			Stopped,
			Stopping,
			Ready
		}

		private JobControl.ThreadState runnerState;

		private List<ControlledJob> jobsInProgress = new List<ControlledJob>();

		private List<ControlledJob> successfulJobs = new List<ControlledJob>();

		private List<ControlledJob> failedJobs = new List<ControlledJob>();

		private long nextJobID;

		private string groupName;

		/// <summary>Construct a job control for a group of jobs.</summary>
		/// <param name="groupName">a name identifying this group</param>
		public JobControl(string groupName)
		{
			// The thread can be in one of the following state
			// the thread state
			this.nextJobID = -1;
			this.groupName = groupName;
			this.runnerState = JobControl.ThreadState.Ready;
		}

		private static IList<ControlledJob> ToList(List<ControlledJob> jobs)
		{
			AList<ControlledJob> retv = new AList<ControlledJob>();
			foreach (ControlledJob job in jobs)
			{
				retv.AddItem(job);
			}
			return retv;
		}

		private IList<ControlledJob> GetJobsIn(ControlledJob.State state)
		{
			lock (this)
			{
				List<ControlledJob> l = new List<ControlledJob>();
				foreach (ControlledJob j in jobsInProgress)
				{
					if (j.GetJobState() == state)
					{
						l.AddItem(j);
					}
				}
				return l;
			}
		}

		/// <returns>the jobs in the waiting state</returns>
		public virtual IList<ControlledJob> GetWaitingJobList()
		{
			return GetJobsIn(ControlledJob.State.Waiting);
		}

		/// <returns>the jobs in the running state</returns>
		public virtual IList<ControlledJob> GetRunningJobList()
		{
			return GetJobsIn(ControlledJob.State.Running);
		}

		/// <returns>the jobs in the ready state</returns>
		public virtual IList<ControlledJob> GetReadyJobsList()
		{
			return GetJobsIn(ControlledJob.State.Ready);
		}

		/// <returns>the jobs in the success state</returns>
		public virtual IList<ControlledJob> GetSuccessfulJobList()
		{
			lock (this)
			{
				return ToList(this.successfulJobs);
			}
		}

		public virtual IList<ControlledJob> GetFailedJobList()
		{
			lock (this)
			{
				return ToList(this.failedJobs);
			}
		}

		private string GetNextJobID()
		{
			nextJobID += 1;
			return this.groupName + this.nextJobID;
		}

		/// <summary>Add a new controlled job.</summary>
		/// <param name="aJob">the new controlled job</param>
		public virtual string AddJob(ControlledJob aJob)
		{
			lock (this)
			{
				string id = this.GetNextJobID();
				aJob.SetJobID(id);
				aJob.SetJobState(ControlledJob.State.Waiting);
				jobsInProgress.AddItem(aJob);
				return id;
			}
		}

		/// <summary>Add a new job.</summary>
		/// <param name="aJob">the new job</param>
		public virtual string AddJob(Job aJob)
		{
			lock (this)
			{
				return AddJob((ControlledJob)aJob);
			}
		}

		/// <summary>Add a collection of jobs</summary>
		/// <param name="jobs"/>
		public virtual void AddJobCollection(ICollection<ControlledJob> jobs)
		{
			foreach (ControlledJob job in jobs)
			{
				AddJob(job);
			}
		}

		/// <returns>the thread state</returns>
		public virtual JobControl.ThreadState GetThreadState()
		{
			return this.runnerState;
		}

		/// <summary>
		/// set the thread state to STOPPING so that the
		/// thread will stop when it wakes up.
		/// </summary>
		public virtual void Stop()
		{
			this.runnerState = JobControl.ThreadState.Stopping;
		}

		/// <summary>suspend the running thread</summary>
		public virtual void Suspend()
		{
			if (this.runnerState == JobControl.ThreadState.Running)
			{
				this.runnerState = JobControl.ThreadState.Suspended;
			}
		}

		/// <summary>resume the suspended thread</summary>
		public virtual void Resume()
		{
			if (this.runnerState == JobControl.ThreadState.Suspended)
			{
				this.runnerState = JobControl.ThreadState.Running;
			}
		}

		public virtual bool AllFinished()
		{
			lock (this)
			{
				return jobsInProgress.IsEmpty();
			}
		}

		/// <summary>The main loop for the thread.</summary>
		/// <remarks>
		/// The main loop for the thread.
		/// The loop does the following:
		/// Check the states of the running jobs
		/// Update the states of waiting jobs
		/// Submit the jobs in ready state
		/// </remarks>
		public virtual void Run()
		{
			try
			{
				this.runnerState = JobControl.ThreadState.Running;
				while (true)
				{
					while (this.runnerState == JobControl.ThreadState.Suspended)
					{
						try
						{
							Sharpen.Thread.Sleep(5000);
						}
						catch (Exception)
						{
						}
					}
					//TODO the thread was interrupted, do something!!!
					lock (this)
					{
						IEnumerator<ControlledJob> it = jobsInProgress.GetEnumerator();
						while (it.HasNext())
						{
							ControlledJob j = it.Next();
							Log.Debug("Checking state of job " + j);
							switch (j.CheckState())
							{
								case ControlledJob.State.Success:
								{
									successfulJobs.AddItem(j);
									it.Remove();
									break;
								}

								case ControlledJob.State.Failed:
								case ControlledJob.State.DependentFailed:
								{
									failedJobs.AddItem(j);
									it.Remove();
									break;
								}

								case ControlledJob.State.Ready:
								{
									j.Submit();
									break;
								}

								case ControlledJob.State.Running:
								case ControlledJob.State.Waiting:
								{
									//Do Nothing
									break;
								}
							}
						}
					}
					if (this.runnerState != JobControl.ThreadState.Running && this.runnerState != JobControl.ThreadState
						.Suspended)
					{
						break;
					}
					try
					{
						Sharpen.Thread.Sleep(5000);
					}
					catch (Exception)
					{
					}
					//TODO the thread was interrupted, do something!!!
					if (this.runnerState != JobControl.ThreadState.Running && this.runnerState != JobControl.ThreadState
						.Suspended)
					{
						break;
					}
				}
			}
			catch (Exception t)
			{
				Log.Error("Error while trying to run jobs.", t);
				//Mark all jobs as failed because we got something bad.
				FailAllJobs(t);
			}
			this.runnerState = JobControl.ThreadState.Stopped;
		}

		private void FailAllJobs(Exception t)
		{
			lock (this)
			{
				string message = "Unexpected System Error Occured: " + StringUtils.StringifyException
					(t);
				IEnumerator<ControlledJob> it = jobsInProgress.GetEnumerator();
				while (it.HasNext())
				{
					ControlledJob j = it.Next();
					try
					{
						j.FailJob(message);
					}
					catch (IOException e)
					{
						Log.Error("Error while tyring to clean up " + j.GetJobName(), e);
					}
					catch (Exception e)
					{
						Log.Error("Error while tyring to clean up " + j.GetJobName(), e);
					}
					finally
					{
						failedJobs.AddItem(j);
						it.Remove();
					}
				}
			}
		}
	}
}
