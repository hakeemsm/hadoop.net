using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop
{
	public class CustomOutputCommitter : OutputCommitter
	{
		public const string JobSetupFileName = "_job_setup";

		public const string JobCommitFileName = "_job_commit";

		public const string JobAbortFileName = "_job_abort";

		public const string TaskSetupFileName = "_task_setup";

		public const string TaskAbortFileName = "_task_abort";

		public const string TaskCommitFileName = "_task_commit";

		/// <exception cref="System.IO.IOException"/>
		public override void SetupJob(JobContext jobContext)
		{
			WriteFile(jobContext.GetJobConf(), JobSetupFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CommitJob(JobContext jobContext)
		{
			base.CommitJob(jobContext);
			WriteFile(jobContext.GetJobConf(), JobCommitFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AbortJob(JobContext jobContext, int status)
		{
			base.AbortJob(jobContext, status);
			WriteFile(jobContext.GetJobConf(), JobAbortFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetupTask(TaskAttemptContext taskContext)
		{
			WriteFile(taskContext.GetJobConf(), TaskSetupFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool NeedsTaskCommit(TaskAttemptContext taskContext)
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CommitTask(TaskAttemptContext taskContext)
		{
			WriteFile(taskContext.GetJobConf(), TaskCommitFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AbortTask(TaskAttemptContext taskContext)
		{
			WriteFile(taskContext.GetJobConf(), TaskAbortFileName);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(JobConf conf, string filename)
		{
			System.Console.Out.WriteLine("writing file ----" + filename);
			Path outputPath = FileOutputFormat.GetOutputPath(conf);
			FileSystem fs = outputPath.GetFileSystem(conf);
			fs.Create(new Path(outputPath, filename)).Close();
		}
	}
}
