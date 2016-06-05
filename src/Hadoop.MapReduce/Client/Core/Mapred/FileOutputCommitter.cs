using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="OutputCommitter"/>
	/// that commits files specified
	/// in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
	/// </summary>
	public class FileOutputCommitter : OutputCommitter
	{
		public static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.mapred.FileOutputCommitter"
			);

		/// <summary>Temporary directory name</summary>
		public const string TempDirName = FileOutputCommitter.PendingDirName;

		public const string SucceededFileName = FileOutputCommitter.SucceededFileName;

		internal const string SuccessfulJobOutputDirMarker = FileOutputCommitter.SuccessfulJobOutputDirMarker;

		private static Path GetOutputPath(JobContext context)
		{
			JobConf conf = context.GetJobConf();
			return FileOutputFormat.GetOutputPath(conf);
		}

		private static Path GetOutputPath(TaskAttemptContext context)
		{
			JobConf conf = context.GetJobConf();
			return FileOutputFormat.GetOutputPath(conf);
		}

		private FileOutputCommitter wrapped = null;

		/// <exception cref="System.IO.IOException"/>
		private FileOutputCommitter GetWrapped(JobContext context)
		{
			if (wrapped == null)
			{
				wrapped = new FileOutputCommitter(GetOutputPath(context), context);
			}
			return wrapped;
		}

		/// <exception cref="System.IO.IOException"/>
		private FileOutputCommitter GetWrapped(TaskAttemptContext context)
		{
			if (wrapped == null)
			{
				wrapped = new FileOutputCommitter(GetOutputPath(context), context);
			}
			return wrapped;
		}

		/// <summary>Compute the path where the output of a given job attempt will be placed.
		/// 	</summary>
		/// <param name="context">
		/// the context of the job.  This is used to get the
		/// application attempt id.
		/// </param>
		/// <returns>the path to store job attempt data.</returns>
		[InterfaceAudience.Private]
		internal virtual Path GetJobAttemptPath(JobContext context)
		{
			Path @out = GetOutputPath(context);
			return @out == null ? null : FileOutputCommitter.GetJobAttemptPath(context, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual Path GetTaskAttemptPath(TaskAttemptContext context)
		{
			Path @out = GetOutputPath(context);
			return @out == null ? null : GetTaskAttemptPath(context, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		private Path GetTaskAttemptPath(TaskAttemptContext context, Path @out)
		{
			Path workPath = FileOutputFormat.GetWorkOutputPath(context.GetJobConf());
			if (workPath == null && @out != null)
			{
				return FileOutputCommitter.GetTaskAttemptPath(context, @out);
			}
			return workPath;
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
		[InterfaceAudience.Private]
		internal virtual Path GetCommittedTaskPath(TaskAttemptContext context)
		{
			Path @out = GetOutputPath(context);
			return @out == null ? null : FileOutputCommitter.GetCommittedTaskPath(context, @out
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetWorkPath(TaskAttemptContext context, Path outputPath)
		{
			return outputPath == null ? null : GetTaskAttemptPath(context, outputPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetupJob(JobContext context)
		{
			GetWrapped(context).SetupJob(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CommitJob(JobContext context)
		{
			GetWrapped(context).CommitJob(context);
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public override void CleanupJob(JobContext context)
		{
			GetWrapped(context).CleanupJob(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AbortJob(JobContext context, int runState)
		{
			JobStatus.State state;
			if (runState == JobStatus.State.Running.GetValue())
			{
				state = JobStatus.State.Running;
			}
			else
			{
				if (runState == JobStatus.State.Succeeded.GetValue())
				{
					state = JobStatus.State.Succeeded;
				}
				else
				{
					if (runState == JobStatus.State.Failed.GetValue())
					{
						state = JobStatus.State.Failed;
					}
					else
					{
						if (runState == JobStatus.State.Prep.GetValue())
						{
							state = JobStatus.State.Prep;
						}
						else
						{
							if (runState == JobStatus.State.Killed.GetValue())
							{
								state = JobStatus.State.Killed;
							}
							else
							{
								throw new ArgumentException(runState + " is not a valid runState.");
							}
						}
					}
				}
			}
			GetWrapped(context).AbortJob(context, state);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetupTask(TaskAttemptContext context)
		{
			GetWrapped(context).SetupTask(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CommitTask(TaskAttemptContext context)
		{
			GetWrapped(context).CommitTask(context, GetTaskAttemptPath(context));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AbortTask(TaskAttemptContext context)
		{
			GetWrapped(context).AbortTask(context, GetTaskAttemptPath(context));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool NeedsTaskCommit(TaskAttemptContext context)
		{
			return GetWrapped(context).NeedsTaskCommit(context, GetTaskAttemptPath(context));
		}

		[Obsolete]
		public override bool IsRecoverySupported()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsRecoverySupported(JobContext context)
		{
			return GetWrapped(context).IsRecoverySupported(context);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverTask(TaskAttemptContext context)
		{
			GetWrapped(context).RecoverTask(context);
		}
	}
}
