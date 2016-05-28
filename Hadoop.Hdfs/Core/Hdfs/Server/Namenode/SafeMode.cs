using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>SafeMode related operations.</summary>
	public interface SafeMode
	{
		/// <summary>Check safe mode conditions.</summary>
		/// <remarks>
		/// Check safe mode conditions.
		/// If the corresponding conditions are satisfied,
		/// trigger the system to enter/leave safe mode.
		/// </remarks>
		void CheckSafeMode();

		/// <summary>Is the system in safe mode?</summary>
		bool IsInSafeMode();

		/// <summary>Is the system in startup safe mode, i.e.</summary>
		/// <remarks>
		/// Is the system in startup safe mode, i.e. the system is starting up with
		/// safe mode turned on automatically?
		/// </remarks>
		bool IsInStartupSafeMode();

		/// <summary>Check whether replication queues are being populated.</summary>
		bool IsPopulatingReplQueues();

		/// <summary>Increment number of blocks that reached minimal replication.</summary>
		/// <param name="replication">current replication</param>
		void IncrementSafeBlockCount(int replication);

		/// <summary>Decrement number of blocks that reached minimal replication.</summary>
		void DecrementSafeBlockCount(Block b);
	}
}
