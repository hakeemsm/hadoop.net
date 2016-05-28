using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This is the JMX management interface for NameNode status information</summary>
	public interface NameNodeStatusMXBean
	{
		/// <summary>Gets the NameNode role.</summary>
		/// <returns>the NameNode role.</returns>
		string GetNNRole();

		/// <summary>Gets the NameNode state.</summary>
		/// <returns>the NameNode state.</returns>
		string GetState();

		/// <summary>Gets the host and port colon separated.</summary>
		/// <returns>host and port colon separated.</returns>
		string GetHostAndPort();

		/// <summary>Gets if security is enabled.</summary>
		/// <returns>true, if security is enabled.</returns>
		bool IsSecurityEnabled();

		/// <summary>Gets the most recent HA transition time in milliseconds from the epoch.</summary>
		/// <returns>the most recent HA transition time in milliseconds from the epoch.</returns>
		long GetLastHATransitionTime();
	}
}
