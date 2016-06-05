using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This is the JMX management interface for data node information</summary>
	public interface DataNodeMXBean
	{
		/// <summary>Gets the version of Hadoop.</summary>
		/// <returns>the version of Hadoop</returns>
		string GetVersion();

		/// <summary>Gets the rpc port.</summary>
		/// <returns>the rpc port</returns>
		string GetRpcPort();

		/// <summary>Gets the http port.</summary>
		/// <returns>the http port</returns>
		string GetHttpPort();

		/// <summary>Gets the namenode IP addresses</summary>
		/// <returns>the namenode IP addresses that the datanode is talking to</returns>
		string GetNamenodeAddresses();

		/// <summary>Gets the information of each volume on the Datanode.</summary>
		/// <remarks>
		/// Gets the information of each volume on the Datanode. Please
		/// see the implementation for the format of returned information.
		/// </remarks>
		/// <returns>the volume info</returns>
		string GetVolumeInfo();

		/// <summary>Gets the cluster id.</summary>
		/// <returns>the cluster id</returns>
		string GetClusterId();

		/// <summary>
		/// Returns an estimate of the number of Datanode threads
		/// actively transferring blocks.
		/// </summary>
		int GetXceiverCount();

		/// <summary>Gets the network error counts on a per-Datanode basis.</summary>
		IDictionary<string, IDictionary<string, long>> GetDatanodeNetworkCounts();
	}
}
