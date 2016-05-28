using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task.Annotation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputCommitter"/>
	/// that commits files specified
	/// in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
	/// </summary>
	public class PartialFileOutputCommitter : FileOutputCommitter, PartialOutputCommitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Output.PartialFileOutputCommitter
			));

		/// <exception cref="System.IO.IOException"/>
		public PartialFileOutputCommitter(Path outputPath, TaskAttemptContext context)
			: base(outputPath, context)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public PartialFileOutputCommitter(Path outputPath, JobContext context)
			: base(outputPath, context)
		{
		}

		protected internal override Path GetCommittedTaskPath(int appAttemptId, TaskAttemptContext
			 context)
		{
			return new Path(GetJobAttemptPath(appAttemptId), context.GetTaskAttemptID().ToString
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual FileSystem FsFor(Path p, Configuration conf)
		{
			return p.GetFileSystem(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CleanUpPartialOutputForTask(TaskAttemptContext context)
		{
			// we double check this is never invoked from a non-preemptable subclass.
			// This should never happen, since the invoking codes is checking it too,
			// but it is safer to double check. Errors handling this would produce
			// inconsistent output.
			if (!this.GetType().IsAnnotationPresent(typeof(Checkpointable)))
			{
				throw new InvalidOperationException("Invoking cleanUpPartialOutputForTask() " + "from non @Preemptable class"
					);
			}
			FileSystem fs = FsFor(GetTaskAttemptPath(context), context.GetConfiguration());
			Log.Info("cleanUpPartialOutputForTask: removing everything belonging to " + context
				.GetTaskAttemptID().GetTaskID() + " in: " + GetCommittedTaskPath(context).GetParent
				());
			TaskAttemptID taid = context.GetTaskAttemptID();
			TaskID tid = taid.GetTaskID();
			Path pCommit = GetCommittedTaskPath(context).GetParent();
			// remove any committed output
			for (int i = 0; i < taid.GetId(); ++i)
			{
				TaskAttemptID oldId = new TaskAttemptID(tid, i);
				Path pTask = new Path(pCommit, oldId.ToString());
				if (fs.Exists(pTask) && !fs.Delete(pTask, true))
				{
					throw new IOException("Failed to delete " + pTask);
				}
			}
		}
	}
}
