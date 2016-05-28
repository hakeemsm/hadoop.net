using System;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class ContentSummaryComputationContext
	{
		private FSDirectory dir = null;

		private FSNamesystem fsn = null;

		private BlockStoragePolicySuite bsps = null;

		private ContentCounts counts = null;

		private long nextCountLimit = 0;

		private long limitPerRun = 0;

		private long yieldCount = 0;

		private long sleepMilliSec = 0;

		private int sleepNanoSec = 0;

		/// <summary>Constructor</summary>
		/// <param name="dir">The FSDirectory instance</param>
		/// <param name="fsn">The FSNamesystem instance</param>
		/// <param name="limitPerRun">
		/// allowed number of operations in one
		/// locking period. 0 or a negative number means
		/// no limit (i.e. no yielding)
		/// </param>
		public ContentSummaryComputationContext(FSDirectory dir, FSNamesystem fsn, long limitPerRun
			, long sleepMicroSec)
		{
			this.dir = dir;
			this.fsn = fsn;
			this.limitPerRun = limitPerRun;
			this.nextCountLimit = limitPerRun;
			this.counts = new ContentCounts.Builder().Build();
			this.sleepMilliSec = sleepMicroSec / 1000;
			this.sleepNanoSec = (int)((sleepMicroSec % 1000) * 1000);
		}

		/// <summary>Constructor for blocking computation.</summary>
		public ContentSummaryComputationContext(BlockStoragePolicySuite bsps)
			: this(null, null, 0, 1000)
		{
			this.bsps = bsps;
		}

		/// <summary>Return current yield count</summary>
		public virtual long GetYieldCount()
		{
			return yieldCount;
		}

		/// <summary>
		/// Relinquish locks held during computation for a short while
		/// and reacquire them.
		/// </summary>
		/// <remarks>
		/// Relinquish locks held during computation for a short while
		/// and reacquire them. This will give other threads a chance
		/// to acquire the contended locks and run.
		/// </remarks>
		/// <returns>true if locks were released and reacquired.</returns>
		public virtual bool Yield()
		{
			// Are we set up to do this?
			if (limitPerRun <= 0 || dir == null || fsn == null)
			{
				return false;
			}
			// Have we reached the limit?
			long currentCount = counts.GetFileCount() + counts.GetSymlinkCount() + counts.GetDirectoryCount
				() + counts.GetSnapshotableDirectoryCount();
			if (currentCount <= nextCountLimit)
			{
				return false;
			}
			// Update the next limit
			nextCountLimit = currentCount + limitPerRun;
			bool hadDirReadLock = dir.HasReadLock();
			bool hadDirWriteLock = dir.HasWriteLock();
			bool hadFsnReadLock = fsn.HasReadLock();
			bool hadFsnWriteLock = fsn.HasWriteLock();
			// sanity check.
			if (!hadDirReadLock || !hadFsnReadLock || hadDirWriteLock || hadFsnWriteLock || dir
				.GetReadHoldCount() != 1 || fsn.GetReadHoldCount() != 1)
			{
				// cannot relinquish
				return false;
			}
			// unlock
			dir.ReadUnlock();
			fsn.ReadUnlock();
			try
			{
				Sharpen.Thread.Sleep(sleepMilliSec, sleepNanoSec);
			}
			catch (Exception)
			{
			}
			finally
			{
				// reacquire
				fsn.ReadLock();
				dir.ReadLock();
			}
			yieldCount++;
			return true;
		}

		/// <summary>Get the content counts</summary>
		public virtual ContentCounts GetCounts()
		{
			return counts;
		}

		public virtual BlockStoragePolicySuite GetBlockStoragePolicySuite()
		{
			Preconditions.CheckState((bsps != null || fsn != null), "BlockStoragePolicySuite must be either initialized or available via"
				 + " FSNameSystem");
			return (bsps != null) ? bsps : fsn.GetBlockManager().GetStoragePolicySuite();
		}
	}
}
