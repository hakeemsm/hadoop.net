using System;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>Consume all outputs and put them in /dev/null.</summary>
	public class NullOutputFormat<K, V> : OutputFormat<K, V>
	{
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context)
		{
			return new _RecordWriter_41();
		}

		private sealed class _RecordWriter_41 : RecordWriter<K, V>
		{
			public _RecordWriter_41()
			{
			}

			public override void Write(K key, V value)
			{
			}

			public override void Close(TaskAttemptContext context)
			{
			}
		}

		public override void CheckOutputSpecs(JobContext context)
		{
		}

		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			return new _OutputCommitter_52();
		}

		private sealed class _OutputCommitter_52 : OutputCommitter
		{
			public _OutputCommitter_52()
			{
			}

			public override void AbortTask(TaskAttemptContext taskContext)
			{
			}

			public override void CleanupJob(JobContext jobContext)
			{
			}

			public override void CommitTask(TaskAttemptContext taskContext)
			{
			}

			public override bool NeedsTaskCommit(TaskAttemptContext taskContext)
			{
				return false;
			}

			public override void SetupJob(JobContext jobContext)
			{
			}

			public override void SetupTask(TaskAttemptContext taskContext)
			{
			}

			[Obsolete]
			public override bool IsRecoverySupported()
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RecoverTask(TaskAttemptContext taskContext)
			{
			}
		}
		// Nothing to do for recovering the task.
	}
}
