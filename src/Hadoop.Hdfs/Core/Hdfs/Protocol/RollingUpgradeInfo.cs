using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Rolling upgrade information</summary>
	public class RollingUpgradeInfo : RollingUpgradeStatus
	{
		private readonly long startTime;

		private long finalizeTime;

		private bool createdRollbackImages;

		public RollingUpgradeInfo(string blockPoolId, bool createdRollbackImages, long startTime
			, long finalizeTime)
			: base(blockPoolId, finalizeTime != 0)
		{
			this.createdRollbackImages = createdRollbackImages;
			this.startTime = startTime;
			this.finalizeTime = finalizeTime;
		}

		public virtual bool CreatedRollbackImages()
		{
			return createdRollbackImages;
		}

		public virtual void SetCreatedRollbackImages(bool created)
		{
			this.createdRollbackImages = created;
		}

		public virtual bool IsStarted()
		{
			return startTime != 0;
		}

		/// <returns>The rolling upgrade starting time.</returns>
		public virtual long GetStartTime()
		{
			return startTime;
		}

		public override bool IsFinalized()
		{
			return finalizeTime != 0;
		}

		~RollingUpgradeInfo()
		{
			if (finalizeTime != 0)
			{
				this.finalizeTime = finalizeTime;
				createdRollbackImages = false;
			}
		}

		public virtual long GetFinalizeTime()
		{
			return finalizeTime;
		}

		public override int GetHashCode()
		{
			//only use lower 32 bits
			return base.GetHashCode() ^ (int)startTime ^ (int)finalizeTime;
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeInfo))
				{
					return false;
				}
			}
			Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeInfo that = (Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeInfo
				)obj;
			return base.Equals(that) && this.startTime == that.startTime && this.finalizeTime
				 == that.finalizeTime;
		}

		public override string ToString()
		{
			return base.ToString() + "\n     Start Time: " + (startTime == 0 ? "<NOT STARTED>"
				 : Timestamp2String(startTime)) + "\n  Finalize Time: " + (finalizeTime == 0 ? "<NOT FINALIZED>"
				 : Timestamp2String(finalizeTime));
		}

		private static string Timestamp2String(long timestamp)
		{
			return Sharpen.Extensions.CreateDate(timestamp) + " (=" + timestamp + ")";
		}

		public class Bean
		{
			private readonly string blockPoolId;

			private readonly long startTime;

			private readonly long finalizeTime;

			private readonly bool createdRollbackImages;

			public Bean(RollingUpgradeInfo f)
			{
				this.blockPoolId = f.GetBlockPoolId();
				this.startTime = f.startTime;
				this.finalizeTime = f.finalizeTime;
				this.createdRollbackImages = f.CreatedRollbackImages();
			}

			public virtual string GetBlockPoolId()
			{
				return blockPoolId;
			}

			public virtual long GetStartTime()
			{
				return startTime;
			}

			public virtual long GetFinalizeTime()
			{
				return finalizeTime;
			}

			public virtual bool IsCreatedRollbackImages()
			{
				return createdRollbackImages;
			}
		}
	}
}
