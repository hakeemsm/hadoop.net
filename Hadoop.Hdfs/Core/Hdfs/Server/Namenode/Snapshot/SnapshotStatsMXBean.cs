using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// This is an interface used to retrieve statistic information related to
	/// snapshots
	/// </summary>
	public interface SnapshotStatsMXBean
	{
		/// <summary>Return the list of snapshottable directories</summary>
		/// <returns>the list of snapshottable directories</returns>
		SnapshottableDirectoryStatus.Bean[] GetSnapshottableDirectories();

		/// <summary>Return the list of snapshots</summary>
		/// <returns>the list of snapshots</returns>
		SnapshotInfo.Bean[] GetSnapshots();
	}
}
