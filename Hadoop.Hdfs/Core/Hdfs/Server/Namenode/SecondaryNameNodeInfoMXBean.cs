using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>JMX information of the secondary NameNode</summary>
	public interface SecondaryNameNodeInfoMXBean : VersionInfoMXBean
	{
		/// <summary>Gets the host and port colon separated.</summary>
		string GetHostAndPort();

		/// <returns>the timestamp of when the SNN starts</returns>
		long GetStartTime();

		/// <returns>the timestamp of the last checkpoint</returns>
		long GetLastCheckpointTime();

		/// <returns>the directories that store the checkpoint images</returns>
		string[] GetCheckpointDirectories();

		/// <returns>the directories that store the edit logs</returns>
		string[] GetCheckpointEditlogDirectories();
	}
}
