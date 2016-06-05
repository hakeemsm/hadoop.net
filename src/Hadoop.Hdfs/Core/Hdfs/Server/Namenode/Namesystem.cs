using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Namesystem operations.</summary>
	public interface Namesystem : RwLock, SafeMode
	{
		/// <summary>Is this name system running?</summary>
		bool IsRunning();

		/// <summary>Check if the user has superuser privilege.</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		void CheckSuperuserPrivilege();

		/// <returns>the block pool ID</returns>
		string GetBlockPoolId();

		bool IsInStandbyState();

		bool IsGenStampInFuture(Block block);

		void AdjustSafeModeBlockTotals(int deltaSafe, int deltaTotal);

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		void CheckOperation(NameNode.OperationCategory read);

		bool IsInSnapshot(BlockInfoContiguousUnderConstruction blockUC);
	}
}
