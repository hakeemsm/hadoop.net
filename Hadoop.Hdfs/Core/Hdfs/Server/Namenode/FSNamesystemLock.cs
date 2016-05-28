using Com.Google.Common.Annotations;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Mimics a ReentrantReadWriteLock so more sophisticated locking capabilities
	/// are possible.
	/// </summary>
	internal class FSNamesystemLock : ReadWriteLock
	{
		[VisibleForTesting]
		protected internal ReentrantReadWriteLock coarseLock;

		internal FSNamesystemLock(bool fair)
		{
			this.coarseLock = new ReentrantReadWriteLock(fair);
		}

		public virtual Lock ReadLock()
		{
			return coarseLock.ReadLock();
		}

		public virtual Lock WriteLock()
		{
			return coarseLock.WriteLock();
		}

		public virtual int GetReadHoldCount()
		{
			return coarseLock.GetReadHoldCount();
		}

		public virtual int GetWriteHoldCount()
		{
			return coarseLock.GetWriteHoldCount();
		}

		public virtual bool IsWriteLockedByCurrentThread()
		{
			return coarseLock.IsWriteLockedByCurrentThread();
		}
	}
}
