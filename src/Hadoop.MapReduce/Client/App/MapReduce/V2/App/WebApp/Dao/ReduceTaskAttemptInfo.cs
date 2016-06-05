using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class ReduceTaskAttemptInfo : TaskAttemptInfo
	{
		protected internal long shuffleFinishTime;

		protected internal long mergeFinishTime;

		protected internal long elapsedShuffleTime;

		protected internal long elapsedMergeTime;

		protected internal long elapsedReduceTime;

		public ReduceTaskAttemptInfo()
		{
		}

		public ReduceTaskAttemptInfo(TaskAttempt ta, TaskType type)
			: base(ta, type, false)
		{
			this.shuffleFinishTime = ta.GetShuffleFinishTime();
			this.mergeFinishTime = ta.GetSortFinishTime();
			this.elapsedShuffleTime = Times.Elapsed(this.startTime, this.shuffleFinishTime, false
				);
			if (this.elapsedShuffleTime == -1)
			{
				this.elapsedShuffleTime = 0;
			}
			this.elapsedMergeTime = Times.Elapsed(this.shuffleFinishTime, this.mergeFinishTime
				, false);
			if (this.elapsedMergeTime == -1)
			{
				this.elapsedMergeTime = 0;
			}
			this.elapsedReduceTime = Times.Elapsed(this.mergeFinishTime, this.finishTime, false
				);
			if (this.elapsedReduceTime == -1)
			{
				this.elapsedReduceTime = 0;
			}
		}

		public virtual long GetShuffleFinishTime()
		{
			return this.shuffleFinishTime;
		}

		public virtual long GetMergeFinishTime()
		{
			return this.mergeFinishTime;
		}

		public virtual long GetElapsedShuffleTime()
		{
			return this.elapsedShuffleTime;
		}

		public virtual long GetElapsedMergeTime()
		{
			return this.elapsedMergeTime;
		}

		public virtual long GetElapsedReduceTime()
		{
			return this.elapsedReduceTime;
		}
	}
}
