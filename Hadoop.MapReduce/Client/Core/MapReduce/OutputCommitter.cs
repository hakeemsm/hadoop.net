using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <code>OutputCommitter</code> describes the commit of task output for a
	/// Map-Reduce job.
	/// </summary>
	/// <remarks>
	/// <code>OutputCommitter</code> describes the commit of task output for a
	/// Map-Reduce job.
	/// <p>The Map-Reduce framework relies on the <code>OutputCommitter</code> of
	/// the job to:<p>
	/// <ol>
	/// <li>
	/// Setup the job during initialization. For example, create the temporary
	/// output directory for the job during the initialization of the job.
	/// </li>
	/// <li>
	/// Cleanup the job after the job completion. For example, remove the
	/// temporary output directory after the job completion.
	/// </li>
	/// <li>
	/// Setup the task temporary output.
	/// </li>
	/// <li>
	/// Check whether a task needs a commit. This is to avoid the commit
	/// procedure if a task does not need commit.
	/// </li>
	/// <li>
	/// Commit of the task output.
	/// </li>
	/// <li>
	/// Discard the task commit.
	/// </li>
	/// </ol>
	/// The methods in this class can be called from several different processes and
	/// from several different contexts.  It is important to know which process and
	/// which context each is called from.  Each method should be marked accordingly
	/// in its documentation.  It is also important to note that not all methods are
	/// guaranteed to be called once and only once.  If a method is not guaranteed to
	/// have this property the output committer needs to handle this appropriately.
	/// Also note it will only be in rare situations where they may be called
	/// multiple times for the same task.
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Output.FileOutputCommitter"></seealso>
	/// <seealso cref="JobContext"/>
	/// <seealso cref="TaskAttemptContext"></seealso>
	public abstract class OutputCommitter
	{
		/// <summary>For the framework to setup the job output during initialization.</summary>
		/// <remarks>
		/// For the framework to setup the job output during initialization.  This is
		/// called from the application master process for the entire job. This will be
		/// called multiple times, once per job attempt.
		/// </remarks>
		/// <param name="jobContext">Context of the job whose output is being written.</param>
		/// <exception cref="System.IO.IOException">if temporary output could not be created</exception>
		public abstract void SetupJob(JobContext jobContext);

		/// <summary>For cleaning up the job's output after job completion.</summary>
		/// <remarks>
		/// For cleaning up the job's output after job completion.  This is called
		/// from the application master process for the entire job. This may be called
		/// multiple times.
		/// </remarks>
		/// <param name="jobContext">Context of the job whose output is being written.</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CommitJob(JobContext) andAbortJob(JobContext, State) instead."
			)]
		public virtual void CleanupJob(JobContext jobContext)
		{
		}

		/// <summary>For committing job's output after successful job completion.</summary>
		/// <remarks>
		/// For committing job's output after successful job completion. Note that this
		/// is invoked for jobs with final runstate as SUCCESSFUL.  This is called
		/// from the application master process for the entire job. This is guaranteed
		/// to only be called once.  If it throws an exception the entire job will
		/// fail.
		/// </remarks>
		/// <param name="jobContext">Context of the job whose output is being written.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CommitJob(JobContext jobContext)
		{
			CleanupJob(jobContext);
		}

		/// <summary>For aborting an unsuccessful job's output.</summary>
		/// <remarks>
		/// For aborting an unsuccessful job's output. Note that this is invoked for
		/// jobs with final runstate as
		/// <see cref="State.Failed"/>
		/// or
		/// <see cref="State.Killed"/>
		/// .  This is called from the application
		/// master process for the entire job. This may be called multiple times.
		/// </remarks>
		/// <param name="jobContext">Context of the job whose output is being written.</param>
		/// <param name="state">final runstate of the job</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AbortJob(JobContext jobContext, JobStatus.State state)
		{
			CleanupJob(jobContext);
		}

		/// <summary>Sets up output for the task.</summary>
		/// <remarks>
		/// Sets up output for the task.  This is called from each individual task's
		/// process that will output to HDFS, and it is called just for that task. This
		/// may be called multiple times for the same task, but for different task
		/// attempts.
		/// </remarks>
		/// <param name="taskContext">Context of the task whose output is being written.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetupTask(TaskAttemptContext taskContext);

		/// <summary>Check whether task needs a commit.</summary>
		/// <remarks>
		/// Check whether task needs a commit.  This is called from each individual
		/// task's process that will output to HDFS, and it is called just for that
		/// task.
		/// </remarks>
		/// <param name="taskContext"/>
		/// <returns>true/false</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool NeedsTaskCommit(TaskAttemptContext taskContext);

		/// <summary>To promote the task's temporary output to final output location.</summary>
		/// <remarks>
		/// To promote the task's temporary output to final output location.
		/// If
		/// <see cref="NeedsTaskCommit(TaskAttemptContext)"/>
		/// returns true and this
		/// task is the task that the AM determines finished first, this method
		/// is called to commit an individual task's output.  This is to mark
		/// that tasks output as complete, as
		/// <see cref="CommitJob(JobContext)"/>
		/// will
		/// also be called later on if the entire job finished successfully. This
		/// is called from a task's process. This may be called multiple times for the
		/// same task, but different task attempts.  It should be very rare for this to
		/// be called multiple times and requires odd networking failures to make this
		/// happen. In the future the Hadoop framework may eliminate this race.
		/// </remarks>
		/// <param name="taskContext">Context of the task whose output is being written.</param>
		/// <exception cref="System.IO.IOException">if commit is not successful.</exception>
		public abstract void CommitTask(TaskAttemptContext taskContext);

		/// <summary>Discard the task output.</summary>
		/// <remarks>
		/// Discard the task output. This is called from a task's process to clean
		/// up a single task's output that can not yet been committed. This may be
		/// called multiple times for the same task, but for different task attempts.
		/// </remarks>
		/// <param name="taskContext"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void AbortTask(TaskAttemptContext taskContext);

		/// <summary>
		/// Is task output recovery supported for restarting jobs?
		/// If task output recovery is supported, job restart can be done more
		/// efficiently.
		/// </summary>
		/// <returns>
		/// <code>true</code> if task output recovery is supported,
		/// <code>false</code> otherwise
		/// </returns>
		/// <seealso cref="RecoverTask(TaskAttemptContext)"/>
		[System.ObsoleteAttribute(@"Use IsRecoverySupported(JobContext) instead.")]
		public virtual bool IsRecoverySupported()
		{
			return false;
		}

		/// <summary>
		/// Is task output recovery supported for restarting jobs?
		/// If task output recovery is supported, job restart can be done more
		/// efficiently.
		/// </summary>
		/// <param name="jobContext">Context of the job whose output is being written.</param>
		/// <returns>
		/// <code>true</code> if task output recovery is supported,
		/// <code>false</code> otherwise
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="RecoverTask(TaskAttemptContext)"/>
		public virtual bool IsRecoverySupported(JobContext jobContext)
		{
			return IsRecoverySupported();
		}

		/// <summary>Recover the task output.</summary>
		/// <remarks>
		/// Recover the task output.
		/// The retry-count for the job will be passed via the
		/// <see cref="MRJobConfig.ApplicationAttemptId"/>
		/// key in
		/// <see cref="JobContext.GetConfiguration()"/>
		/// for the
		/// <code>OutputCommitter</code>.  This is called from the application master
		/// process, but it is called individually for each task.
		/// If an exception is thrown the task will be attempted again.
		/// This may be called multiple times for the same task.  But from different
		/// application attempts.
		/// </remarks>
		/// <param name="taskContext">Context of the task whose output is being recovered</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RecoverTask(TaskAttemptContext taskContext)
		{
		}
	}
}
