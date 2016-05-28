using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Read-write lock interface.</summary>
	public interface RwLock
	{
		/// <summary>Acquire read lock.</summary>
		void ReadLock();

		/// <summary>Release read lock.</summary>
		void ReadUnlock();

		/// <summary>Check if the current thread holds read lock.</summary>
		bool HasReadLock();

		/// <summary>Acquire write lock.</summary>
		void WriteLock();

		/// <summary>Acquire write lock, unless interrupted while waiting</summary>
		/// <exception cref="System.Exception"/>
		void WriteLockInterruptibly();

		/// <summary>Release write lock.</summary>
		void WriteUnlock();

		/// <summary>Check if the current thread holds write lock.</summary>
		bool HasWriteLock();
	}
}
