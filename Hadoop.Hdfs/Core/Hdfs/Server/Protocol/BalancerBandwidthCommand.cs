using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// Balancer bandwidth command instructs each datanode to change its value for
	/// the max amount of network bandwidth it may use during the block balancing
	/// operation.
	/// </summary>
	/// <remarks>
	/// Balancer bandwidth command instructs each datanode to change its value for
	/// the max amount of network bandwidth it may use during the block balancing
	/// operation.
	/// The Balancer Bandwidth Command contains the new bandwidth value as its
	/// payload. The bandwidth value is in bytes per second.
	/// </remarks>
	public class BalancerBandwidthCommand : DatanodeCommand
	{
		private const long BbcDefaultbandwidth = 0L;

		private readonly long bandwidth;

		/// <summary>Balancer Bandwidth Command constructor.</summary>
		/// <remarks>Balancer Bandwidth Command constructor. Sets bandwidth to 0.</remarks>
		internal BalancerBandwidthCommand()
			: this(BbcDefaultbandwidth)
		{
		}

		/// <summary>Balancer Bandwidth Command constructor.</summary>
		/// <param name="bandwidth">Blanacer bandwidth in bytes per second.</param>
		public BalancerBandwidthCommand(long bandwidth)
			: base(DatanodeProtocol.DnaBalancerbandwidthupdate)
		{
			/*
			* A system administrator can tune the balancer bandwidth parameter
			* (dfs.balance.bandwidthPerSec) dynamically by calling
			* "dfsadmin -setBalanacerBandwidth newbandwidth".
			* This class is to define the command which sends the new bandwidth value to
			* each datanode.
			*/
			this.bandwidth = bandwidth;
		}

		/// <summary>Get current value of the max balancer bandwidth in bytes per second.</summary>
		/// <returns>bandwidth Blanacer bandwidth in bytes per second for this datanode.</returns>
		public virtual long GetBalancerBandwidthValue()
		{
			return this.bandwidth;
		}
	}
}
