using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputCommitter"/>
	/// that commits files specified
	/// in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
	/// </summary>
	public class FileOutputCommitter : OutputCommitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Output.FileOutputCommitter
			));

		/// <summary>Name of directory where pending data is placed.</summary>
		/// <remarks>
		/// Name of directory where pending data is placed.  Data that has not been
		/// committed yet.
		/// </remarks>
		public const string PendingDirName = "_temporary";

		/// <summary>
		/// Temporary directory name
		/// The static variable to be compatible with M/R 1.x
		/// </summary>
		[Obsolete]
		protected internal const string TempDirName = PendingDirName;

		public const string SucceededFileName = "_SUCCESS";

		public const string SuccessfulJobOutputDirMarker = "mapreduce.fileoutputcommitter.marksuccessfuljobs";

		public const string FileoutputcommitterAlgorithmVersion = "mapreduce.fileoutputcommitter.algorithm.version";

		public const int FileoutputcommitterAlgorithmVersionDefault = 1;

		private Path outputPath = null;

		private Path workPath = null;

		private readonly int algorithmVersion;

		/// <summary>Create a file output committer</summary>
		/// <param name="outputPath">
		/// the job's output path, or null if you want the output
		/// committer to act as a noop.
		/// </param>
		/// <param name="context">the task's context</param>
		/// <exception cref="System.IO.IOException"/>
		public FileOutputCommitter(Path outputPath, TaskAttemptContext context)
			: this(outputPath, (JobContext)context)
		{
			if (outputPath != null)
			{
				workPath = GetTaskAttemptPath(context, outputPath);
			}
		}

		/// <summary>Create a file output committer</summary>
		/// <param name="outputPath">
		/// the job's output path, or null if you want the output
		/// committer to act as a noop.
		/// </param>
		/// <param name="context">the task's context</param>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public FileOutputCommitter(Path outputPath, JobContext context)
		{
			Configuration conf = context.GetConfiguration();
			algorithmVersion = conf.GetInt(FileoutputcommitterAlgorithmVersion, FileoutputcommitterAlgorithmVersionDefault
				);
			Log.Info("File Output Committer Algorithm version is " + algorithmVersion);
			if (algorithmVersion != 1 && algorithmVersion != 2)
			{
				throw new IOException("Only 1 or 2 algorithm version is supported");
			}
			if (outputPath != null)
			{
				FileSystem fs = outputPath.GetFileSystem(context.GetConfiguration());
				this.outputPath = fs.MakeQualified(outputPath);
			}
		}

		/// <returns>
		/// the path where final output of the job should be placed.  This
		/// could also be considered the committed application attempt path.
		/// </returns>
		private Path GetOutputPath()
		{
			return this.outputPath;
		}

		/// <returns>true if we have an output path set, else false.</returns>
		private bool HasOutputPath()
		{
			return this.outputPath != null;
		}

		/// <returns>
		/// the path where the output of pending job attempts are
		/// stored.
		/// </returns>
		private Path GetPendingJobAttemptsPath()
		{
			return GetPendingJobAttemptsPath(GetOutputPath());
		}

		/// <summary>Get the location of pending job attempts.</summary>
		/// <param name="out">the base output directory.</param>
		/// <returns>the location of pending job attempts.</returns>
		private static Path GetPendingJobAttemptsPath(Path @out)
		{
			return new Path(@out, PendingDirName);
		}

		/// <summary>Get the Application Attempt Id for this job</summary>
		/// <param name="context">the context to look in</param>
		/// <returns>the Application Attempt Id for a given job.</returns>
		private static int GetAppAttemptId(JobContext context)
		{
			return context.GetConfiguration().GetInt(MRJobConfig.ApplicationAttemptId, 0);
		}

		/// <summary>Compute the path where the output of a given job attempt will be placed.
		/// 	</summary>
		/// <param name="context">
		/// the context of the job.  This is used to get the
		/// application attempt id.
		/// </param>
		/// <returns>the path to store job attempt data.</returns>
		public virtual Path GetJobAttemptPath(JobContext context)
		{
			return GetJobAttemptPath(context, GetOutputPath());
		}

		/// <summary>Compute the path where the output of a given job attempt will be placed.
		/// 	</summary>
		/// <param name="context">
		/// the context of the job.  This is used to get the
		/// application attempt id.
		/// </param>
		/// <param name="out">the output path to place these in.</param>
		/// <returns>the path to store job attempt data.</returns>
		public static Path GetJobAttemptPath(JobContext context, Path @out)
		{
			return GetJobAttemptPath(GetAppAttemptId(context), @out);
		}

		/// <summary>Compute the path where the output of a given job attempt will be placed.
		/// 	</summary>
		/// <param name="appAttemptId">the ID of the application attempt for this job.</param>
		/// <returns>the path to store job attempt data.</returns>
		protected internal virtual Path GetJobAttemptPath(int appAttemptId)
		{
			return GetJobAttemptPath(appAttemptId, GetOutputPath());
		}

		/// <summary>Compute the path where the output of a given job attempt will be placed.
		/// 	</summary>
		/// <param name="appAttemptId">the ID of the application attempt for this job.</param>
		/// <returns>the path to store job attempt data.</returns>
		private static Path GetJobAttemptPath(int appAttemptId, Path @out)
		{
			return new Path(GetPendingJobAttemptsPath(@out), appAttemptId.ToString());
		}

		/// <summary>Compute the path where the output of pending task attempts are stored.</summary>
		/// <param name="context">the context of the job with pending tasks.</param>
		/// <returns>the path where the output of pending task attempts are stored.</returns>
		private Path GetPendingTaskAttemptsPath(JobContext context)
		{
			return GetPendingTaskAttemptsPath(context, GetOutputPath());
		}

		/// <summary>Compute the path where the output of pending task attempts are stored.</summary>
		/// <param name="context">the context of the job with pending tasks.</param>
		/// <returns>the path where the output of pending task attempts are stored.</returns>
		private static Path GetPendingTaskAttemptsPath(JobContext context, Path @out)
		{
			return new Path(GetJobAttemptPath(context, @out), PendingDirName);
		}

		/// <summary>
		/// Compute the path where the output of a task attempt is stored until
		/// that task is committed.
		/// </summary>
		/// <param name="context">the context of the task attempt.</param>
		/// <returns>the path where a task attempt should be stored.</returns>
		public virtual Path GetTaskAttemptPath(TaskAttemptContext context)
		{
			return new Path(GetPendingTaskAttemptsPath(context), context.GetTaskAttemptID().ToString
				());
		}

		/// <summary>
		/// Compute the path where the output of a task attempt is stored until
		/// that task is committed.
		/// </summary>
		/// <param name="context">the context of the task attempt.</param>
		/// <param name="out">The output path to put things in.</param>
		/// <returns>the path where a task attempt should be stored.</returns>
		public static Path GetTaskAttemptPath(TaskAttemptContext context, Path @out)
		{
			return new Path(GetPendingTaskAttemptsPath(context, @out), context.GetTaskAttemptID
				().ToString());
		}

		/// <summary>
		/// Compute the path where the output of a committed task is stored until
		/// the entire job is committed.
		/// </summary>
		/// <param name="context">the context of the task attempt</param>
		/// <returns>
		/// the path where the output of a committed task is stored until
		/// the entire job is committed.
		/// </returns>
		public virtual Path GetCommittedTaskPath(TaskAttemptContext context)
		{
			return GetCommittedTaskPath(GetAppAttemptId(context), context);
		}

		public static Path GetCommittedTaskPath(TaskAttemptContext context, Path @out)
		{
			return GetCommittedTaskPath(GetAppAttemptId(context), context, @out);
		}

		/// <summary>
		/// Compute the path where the output of a committed task is stored until the
		/// entire job is committed for a specific application attempt.
		/// </summary>
		/// <param name="appAttemptId">the id of the application attempt to use</param>
		/// <param name="context">the context of any task.</param>
		/// <returns>the path where the output of a committed task is stored.</returns>
		protected internal virtual Path GetCommittedTaskPath(int appAttemptId, TaskAttemptContext
			 context)
		{
			return new Path(GetJobAttemptPath(appAttemptId), context.GetTaskAttemptID().GetTaskID
				().ToString());
		}

		private static Path GetCommittedTaskPath(int appAttemptId, TaskAttemptContext context
			, Path @out)
		{
			return new Path(GetJobAttemptPath(appAttemptId, @out), context.GetTaskAttemptID()
				.GetTaskID().ToString());
		}

		private class CommittedTaskFilter : PathFilter
		{
			public virtual bool Accept(Path path)
			{
				return !PendingDirName.Equals(path.GetName());
			}
		}

		/// <summary>Get a list of all paths where output from committed tasks are stored.</summary>
		/// <param name="context">the context of the current job</param>
		/// <returns>the list of these Paths/FileStatuses.</returns>
		/// <exception cref="System.IO.IOException"/>
		private FileStatus[] GetAllCommittedTaskPaths(JobContext context)
		{
			Path jobAttemptPath = GetJobAttemptPath(context);
			FileSystem fs = jobAttemptPath.GetFileSystem(context.GetConfiguration());
			return fs.ListStatus(jobAttemptPath, new FileOutputCommitter.CommittedTaskFilter(
				));
		}

		/// <summary>Get the directory that the task should write results into.</summary>
		/// <returns>the work directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetWorkPath()
		{
			return workPath;
		}

		/// <summary>
		/// Create the temporary directory that is the root of all of the task
		/// work directories.
		/// </summary>
		/// <param name="context">the job's context</param>
		/// <exception cref="System.IO.IOException"/>
		public override void SetupJob(JobContext context)
		{
			if (HasOutputPath())
			{
				Path jobAttemptPath = GetJobAttemptPath(context);
				FileSystem fs = jobAttemptPath.GetFileSystem(context.GetConfiguration());
				if (!fs.Mkdirs(jobAttemptPath))
				{
					Log.Error("Mkdirs failed to create " + jobAttemptPath);
				}
			}
			else
			{
				Log.Warn("Output Path is null in setupJob()");
			}
		}

		/// <summary>The job has completed so move all committed tasks to the final output dir.
		/// 	</summary>
		/// <remarks>
		/// The job has completed so move all committed tasks to the final output dir.
		/// Delete the temporary directory, including all of the work directories.
		/// Create a _SUCCESS file to make it as successful.
		/// </remarks>
		/// <param name="context">the job's context</param>
		/// <exception cref="System.IO.IOException"/>
		public override void CommitJob(JobContext context)
		{
			if (HasOutputPath())
			{
				Path finalOutput = GetOutputPath();
				FileSystem fs = finalOutput.GetFileSystem(context.GetConfiguration());
				if (algorithmVersion == 1)
				{
					foreach (FileStatus stat in GetAllCommittedTaskPaths(context))
					{
						MergePaths(fs, stat, finalOutput);
					}
				}
				// delete the _temporary folder and create a _done file in the o/p folder
				CleanupJob(context);
				// True if the job requires output.dir marked on successful job.
				// Note that by default it is set to true.
				if (context.GetConfiguration().GetBoolean(SuccessfulJobOutputDirMarker, true))
				{
					Path markerPath = new Path(outputPath, SucceededFileName);
					fs.Create(markerPath).Close();
				}
			}
			else
			{
				Log.Warn("Output Path is null in commitJob()");
			}
		}

		/// <summary>Merge two paths together.</summary>
		/// <remarks>
		/// Merge two paths together.  Anything in from will be moved into to, if there
		/// are any name conflicts while merging the files or directories in from win.
		/// </remarks>
		/// <param name="fs">the File System to use</param>
		/// <param name="from">the path data is coming from.</param>
		/// <param name="to">the path data is going to.</param>
		/// <exception cref="System.IO.IOException">on any error</exception>
		private void MergePaths(FileSystem fs, FileStatus from, Path to)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Merging data from " + from + " to " + to);
			}
			FileStatus toStat;
			try
			{
				toStat = fs.GetFileStatus(to);
			}
			catch (FileNotFoundException)
			{
				toStat = null;
			}
			if (from.IsFile())
			{
				if (toStat != null)
				{
					if (!fs.Delete(to, true))
					{
						throw new IOException("Failed to delete " + to);
					}
				}
				if (!fs.Rename(from.GetPath(), to))
				{
					throw new IOException("Failed to rename " + from + " to " + to);
				}
			}
			else
			{
				if (from.IsDirectory())
				{
					if (toStat != null)
					{
						if (!toStat.IsDirectory())
						{
							if (!fs.Delete(to, true))
							{
								throw new IOException("Failed to delete " + to);
							}
							RenameOrMerge(fs, from, to);
						}
						else
						{
							//It is a directory so merge everything in the directories
							foreach (FileStatus subFrom in fs.ListStatus(from.GetPath()))
							{
								Path subTo = new Path(to, subFrom.GetPath().GetName());
								MergePaths(fs, subFrom, subTo);
							}
						}
					}
					else
					{
						RenameOrMerge(fs, from, to);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RenameOrMerge(FileSystem fs, FileStatus from, Path to)
		{
			if (algorithmVersion == 1)
			{
				if (!fs.Rename(from.GetPath(), to))
				{
					throw new IOException("Failed to rename " + from + " to " + to);
				}
			}
			else
			{
				fs.Mkdirs(to);
				foreach (FileStatus subFrom in fs.ListStatus(from.GetPath()))
				{
					Path subTo = new Path(to, subFrom.GetPath().GetName());
					MergePaths(fs, subFrom, subTo);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public override void CleanupJob(JobContext context)
		{
			if (HasOutputPath())
			{
				Path pendingJobAttemptsPath = GetPendingJobAttemptsPath();
				FileSystem fs = pendingJobAttemptsPath.GetFileSystem(context.GetConfiguration());
				fs.Delete(pendingJobAttemptsPath, true);
			}
			else
			{
				Log.Warn("Output Path is null in cleanupJob()");
			}
		}

		/// <summary>Delete the temporary directory, including all of the work directories.</summary>
		/// <param name="context">the job's context</param>
		/// <exception cref="System.IO.IOException"/>
		public override void AbortJob(JobContext context, JobStatus.State state)
		{
			// delete the _temporary folder
			CleanupJob(context);
		}

		/// <summary>No task setup required.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetupTask(TaskAttemptContext context)
		{
		}

		// FileOutputCommitter's setupTask doesn't do anything. Because the
		// temporary task directory is created on demand when the 
		// task is writing.
		/// <summary>Move the files from the work directory to the job output directory</summary>
		/// <param name="context">the task context</param>
		/// <exception cref="System.IO.IOException"/>
		public override void CommitTask(TaskAttemptContext context)
		{
			CommitTask(context, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void CommitTask(TaskAttemptContext context, Path taskAttemptPath)
		{
			TaskAttemptID attemptId = context.GetTaskAttemptID();
			if (HasOutputPath())
			{
				context.Progress();
				if (taskAttemptPath == null)
				{
					taskAttemptPath = GetTaskAttemptPath(context);
				}
				FileSystem fs = taskAttemptPath.GetFileSystem(context.GetConfiguration());
				FileStatus taskAttemptDirStatus;
				try
				{
					taskAttemptDirStatus = fs.GetFileStatus(taskAttemptPath);
				}
				catch (FileNotFoundException)
				{
					taskAttemptDirStatus = null;
				}
				if (taskAttemptDirStatus != null)
				{
					if (algorithmVersion == 1)
					{
						Path committedTaskPath = GetCommittedTaskPath(context);
						if (fs.Exists(committedTaskPath))
						{
							if (!fs.Delete(committedTaskPath, true))
							{
								throw new IOException("Could not delete " + committedTaskPath);
							}
						}
						if (!fs.Rename(taskAttemptPath, committedTaskPath))
						{
							throw new IOException("Could not rename " + taskAttemptPath + " to " + committedTaskPath
								);
						}
						Log.Info("Saved output of task '" + attemptId + "' to " + committedTaskPath);
					}
					else
					{
						// directly merge everything from taskAttemptPath to output directory
						MergePaths(fs, taskAttemptDirStatus, outputPath);
						Log.Info("Saved output of task '" + attemptId + "' to " + outputPath);
					}
				}
				else
				{
					Log.Warn("No Output found for " + attemptId);
				}
			}
			else
			{
				Log.Warn("Output Path is null in commitTask()");
			}
		}

		/// <summary>Delete the work directory</summary>
		/// <exception cref="System.IO.IOException"></exception>
		public override void AbortTask(TaskAttemptContext context)
		{
			AbortTask(context, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void AbortTask(TaskAttemptContext context, Path taskAttemptPath)
		{
			if (HasOutputPath())
			{
				context.Progress();
				if (taskAttemptPath == null)
				{
					taskAttemptPath = GetTaskAttemptPath(context);
				}
				FileSystem fs = taskAttemptPath.GetFileSystem(context.GetConfiguration());
				if (!fs.Delete(taskAttemptPath, true))
				{
					Log.Warn("Could not delete " + taskAttemptPath);
				}
			}
			else
			{
				Log.Warn("Output Path is null in abortTask()");
			}
		}

		/// <summary>Did this task write any files in the work directory?</summary>
		/// <param name="context">the task's context</param>
		/// <exception cref="System.IO.IOException"/>
		public override bool NeedsTaskCommit(TaskAttemptContext context)
		{
			return NeedsTaskCommit(context, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual bool NeedsTaskCommit(TaskAttemptContext context, Path taskAttemptPath
			)
		{
			if (HasOutputPath())
			{
				if (taskAttemptPath == null)
				{
					taskAttemptPath = GetTaskAttemptPath(context);
				}
				FileSystem fs = taskAttemptPath.GetFileSystem(context.GetConfiguration());
				return fs.Exists(taskAttemptPath);
			}
			return false;
		}

		[Obsolete]
		public override bool IsRecoverySupported()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverTask(TaskAttemptContext context)
		{
			if (HasOutputPath())
			{
				context.Progress();
				TaskAttemptID attemptId = context.GetTaskAttemptID();
				int previousAttempt = GetAppAttemptId(context) - 1;
				if (previousAttempt < 0)
				{
					throw new IOException("Cannot recover task output for first attempt...");
				}
				Path previousCommittedTaskPath = GetCommittedTaskPath(previousAttempt, context);
				FileSystem fs = previousCommittedTaskPath.GetFileSystem(context.GetConfiguration(
					));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Trying to recover task from " + previousCommittedTaskPath);
				}
				if (algorithmVersion == 1)
				{
					if (fs.Exists(previousCommittedTaskPath))
					{
						Path committedTaskPath = GetCommittedTaskPath(context);
						if (fs.Exists(committedTaskPath))
						{
							if (!fs.Delete(committedTaskPath, true))
							{
								throw new IOException("Could not delete " + committedTaskPath);
							}
						}
						//Rename can fail if the parent directory does not yet exist.
						Path committedParent = committedTaskPath.GetParent();
						fs.Mkdirs(committedParent);
						if (!fs.Rename(previousCommittedTaskPath, committedTaskPath))
						{
							throw new IOException("Could not rename " + previousCommittedTaskPath + " to " + 
								committedTaskPath);
						}
					}
					else
					{
						Log.Warn(attemptId + " had no output to recover.");
					}
				}
				else
				{
					// essentially a no-op, but for backwards compatibility
					// after upgrade to the new fileOutputCommitter,
					// check if there are any output left in committedTaskPath
					if (fs.Exists(previousCommittedTaskPath))
					{
						Log.Info("Recovering task for upgrading scenario, moving files from " + previousCommittedTaskPath
							 + " to " + outputPath);
						FileStatus from = fs.GetFileStatus(previousCommittedTaskPath);
						MergePaths(fs, from, outputPath);
					}
					Log.Info("Done recovering task " + attemptId);
				}
			}
			else
			{
				Log.Warn("Output Path is null in recoverTask()");
			}
		}
	}
}
