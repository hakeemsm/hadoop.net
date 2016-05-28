using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol
{
	/// <summary>This class encapsulates a MapReduce job and its dependency.</summary>
	/// <remarks>
	/// This class encapsulates a MapReduce job and its dependency. It monitors
	/// the states of the depending jobs and updates the state of this job.
	/// A job starts in the WAITING state. If it does not have any depending jobs,
	/// or all of the depending jobs are in SUCCESS state, then the job state
	/// will become READY. If any depending jobs fail, the job will fail too.
	/// When in READY state, the job can be submitted to Hadoop for execution, with
	/// the state changing into RUNNING state. From RUNNING state, the job
	/// can get into SUCCESS or FAILED state, depending
	/// the status of the job execution.
	/// </remarks>
	public class ControlledJob
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob
			));

		public enum State
		{
			Success,
			Waiting,
			Running,
			Ready,
			Failed,
			DependentFailed
		}

		public const string CreateDir = "mapreduce.jobcontrol.createdir.ifnotexist";

		private ControlledJob.State state;

		private string controlID;

		private Job job;

		private string message;

		private IList<Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob> dependingJobs;

		/// <summary>Construct a job.</summary>
		/// <param name="job">a mapreduce job to be executed.</param>
		/// <param name="dependingJobs">an array of jobs the current job depends on</param>
		/// <exception cref="System.IO.IOException"/>
		public ControlledJob(Job job, IList<Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob
			> dependingJobs)
		{
			// A job will be in one of the following states
			// assigned and used by JobControl class
			// mapreduce job to be executed.
			// some info for human consumption, e.g. the reason why the job failed
			// the jobs the current job depends on
			this.job = job;
			this.dependingJobs = dependingJobs;
			this.state = ControlledJob.State.Waiting;
			this.controlID = "unassigned";
			this.message = "just initialized";
		}

		/// <summary>Construct a job.</summary>
		/// <param name="conf">mapred job configuration representing a job to be executed.</param>
		/// <exception cref="System.IO.IOException"/>
		public ControlledJob(Configuration conf)
			: this(Job.GetInstance(conf), null)
		{
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("job name:\t").Append(this.job.GetJobName()).Append("\n");
			sb.Append("job id:\t").Append(this.controlID).Append("\n");
			sb.Append("job state:\t").Append(this.state).Append("\n");
			sb.Append("job mapred id:\t").Append(this.job.GetJobID()).Append("\n");
			sb.Append("job message:\t").Append(this.message).Append("\n");
			if (this.dependingJobs == null || this.dependingJobs.Count == 0)
			{
				sb.Append("job has no depending job:\t").Append("\n");
			}
			else
			{
				sb.Append("job has ").Append(this.dependingJobs.Count).Append(" dependeng jobs:\n"
					);
				for (int i = 0; i < this.dependingJobs.Count; i++)
				{
					sb.Append("\t depending job ").Append(i).Append(":\t");
					sb.Append((this.dependingJobs[i]).GetJobName()).Append("\n");
				}
			}
			return sb.ToString();
		}

		/// <returns>the job name of this job</returns>
		public virtual string GetJobName()
		{
			return job.GetJobName();
		}

		/// <summary>Set the job name for  this job.</summary>
		/// <param name="jobName">the job name</param>
		public virtual void SetJobName(string jobName)
		{
			job.SetJobName(jobName);
		}

		/// <returns>the job ID of this job assigned by JobControl</returns>
		public virtual string GetJobID()
		{
			return this.controlID;
		}

		/// <summary>Set the job ID for  this job.</summary>
		/// <param name="id">the job ID</param>
		public virtual void SetJobID(string id)
		{
			this.controlID = id;
		}

		/// <returns>the mapred ID of this job as assigned by the mapred framework.</returns>
		public virtual JobID GetMapredJobId()
		{
			lock (this)
			{
				return this.job.GetJobID();
			}
		}

		/// <returns>the mapreduce job</returns>
		public virtual Job GetJob()
		{
			lock (this)
			{
				return this.job;
			}
		}

		/// <summary>Set the mapreduce job</summary>
		/// <param name="job">the mapreduce job for this job.</param>
		public virtual void SetJob(Job job)
		{
			lock (this)
			{
				this.job = job;
			}
		}

		/// <returns>the state of this job</returns>
		public virtual ControlledJob.State GetJobState()
		{
			lock (this)
			{
				return this.state;
			}
		}

		/// <summary>Set the state for this job.</summary>
		/// <param name="state">the new state for this job.</param>
		protected internal virtual void SetJobState(ControlledJob.State state)
		{
			lock (this)
			{
				this.state = state;
			}
		}

		/// <returns>the message of this job</returns>
		public virtual string GetMessage()
		{
			lock (this)
			{
				return this.message;
			}
		}

		/// <summary>Set the message for this job.</summary>
		/// <param name="message">the message for this job.</param>
		public virtual void SetMessage(string message)
		{
			lock (this)
			{
				this.message = message;
			}
		}

		/// <returns>the depending jobs of this job</returns>
		public virtual IList<Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob> GetDependentJobs
			()
		{
			return this.dependingJobs;
		}

		/// <summary>Add a job to this jobs' dependency list.</summary>
		/// <remarks>
		/// Add a job to this jobs' dependency list.
		/// Dependent jobs can only be added while a Job
		/// is waiting to run, not during or afterwards.
		/// </remarks>
		/// <param name="dependingJob">Job that this Job depends on.</param>
		/// <returns><tt>true</tt> if the Job was added.</returns>
		public virtual bool AddDependingJob(Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob
			 dependingJob)
		{
			lock (this)
			{
				if (this.state == ControlledJob.State.Waiting)
				{
					//only allowed to add jobs when waiting
					if (this.dependingJobs == null)
					{
						this.dependingJobs = new AList<Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob
							>();
					}
					return this.dependingJobs.AddItem(dependingJob);
				}
				else
				{
					return false;
				}
			}
		}

		/// <returns>true if this job is in a complete state</returns>
		public virtual bool IsCompleted()
		{
			lock (this)
			{
				return this.state == ControlledJob.State.Failed || this.state == ControlledJob.State
					.DependentFailed || this.state == ControlledJob.State.Success;
			}
		}

		/// <returns>true if this job is in READY state</returns>
		public virtual bool IsReady()
		{
			lock (this)
			{
				return this.state == ControlledJob.State.Ready;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void KillJob()
		{
			job.KillJob();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void FailJob(string message)
		{
			lock (this)
			{
				try
				{
					if (job != null && this.state == ControlledJob.State.Running)
					{
						job.KillJob();
					}
				}
				finally
				{
					this.state = ControlledJob.State.Failed;
					this.message = message;
				}
			}
		}

		/// <summary>Check the state of this running job.</summary>
		/// <remarks>
		/// Check the state of this running job. The state may
		/// remain the same, become SUCCESS or FAILED.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CheckRunningState()
		{
			try
			{
				if (job.IsComplete())
				{
					if (job.IsSuccessful())
					{
						this.state = ControlledJob.State.Success;
					}
					else
					{
						this.state = ControlledJob.State.Failed;
						this.message = "Job failed!";
					}
				}
			}
			catch (IOException ioe)
			{
				this.state = ControlledJob.State.Failed;
				this.message = StringUtils.StringifyException(ioe);
				try
				{
					if (job != null)
					{
						job.KillJob();
					}
				}
				catch (IOException)
				{
				}
			}
		}

		/// <summary>Check and update the state of this job.</summary>
		/// <remarks>
		/// Check and update the state of this job. The state changes
		/// depending on its current state and the states of the depending jobs.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual ControlledJob.State CheckState()
		{
			lock (this)
			{
				if (this.state == ControlledJob.State.Running)
				{
					CheckRunningState();
				}
				if (this.state != ControlledJob.State.Waiting)
				{
					return this.state;
				}
				if (this.dependingJobs == null || this.dependingJobs.Count == 0)
				{
					this.state = ControlledJob.State.Ready;
					return this.state;
				}
				Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol.ControlledJob pred = null;
				int n = this.dependingJobs.Count;
				for (int i = 0; i < n; i++)
				{
					pred = this.dependingJobs[i];
					ControlledJob.State s = pred.CheckState();
					if (s == ControlledJob.State.Waiting || s == ControlledJob.State.Ready || s == ControlledJob.State
						.Running)
					{
						break;
					}
					// a pred is still not completed, continue in WAITING
					// state
					if (s == ControlledJob.State.Failed || s == ControlledJob.State.DependentFailed)
					{
						this.state = ControlledJob.State.DependentFailed;
						this.message = "depending job " + i + " with jobID " + pred.GetJobID() + " failed. "
							 + pred.GetMessage();
						break;
					}
					// pred must be in success state
					if (i == n - 1)
					{
						this.state = ControlledJob.State.Ready;
					}
				}
				return this.state;
			}
		}

		/// <summary>Submit this job to mapred.</summary>
		/// <remarks>
		/// Submit this job to mapred. The state becomes RUNNING if submission
		/// is successful, FAILED otherwise.
		/// </remarks>
		protected internal virtual void Submit()
		{
			lock (this)
			{
				try
				{
					Configuration conf = job.GetConfiguration();
					if (conf.GetBoolean(CreateDir, false))
					{
						FileSystem fs = FileSystem.Get(conf);
						Path[] inputPaths = FileInputFormat.GetInputPaths(job);
						for (int i = 0; i < inputPaths.Length; i++)
						{
							if (!fs.Exists(inputPaths[i]))
							{
								try
								{
									fs.Mkdirs(inputPaths[i]);
								}
								catch (IOException)
								{
								}
							}
						}
					}
					job.Submit();
					this.state = ControlledJob.State.Running;
				}
				catch (Exception ioe)
				{
					Log.Info(GetJobName() + " got an error while submitting ", ioe);
					this.state = ControlledJob.State.Failed;
					this.message = StringUtils.StringifyException(ioe);
				}
			}
		}
	}
}
