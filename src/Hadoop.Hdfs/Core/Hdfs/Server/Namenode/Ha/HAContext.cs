using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Context that is to be used by
	/// <see cref="HAState"/>
	/// for getting/setting the
	/// current state and performing required operations.
	/// </summary>
	public interface HAContext
	{
		/// <summary>
		/// Set the state of the context to given
		/// <paramref name="state"/>
		/// 
		/// </summary>
		void SetState(HAState state);

		/// <summary>Get the state from the context</summary>
		HAState GetState();

		/// <summary>Start the services required in active state</summary>
		/// <exception cref="System.IO.IOException"/>
		void StartActiveServices();

		/// <summary>Stop the services when exiting active state</summary>
		/// <exception cref="System.IO.IOException"/>
		void StopActiveServices();

		/// <summary>Start the services required in standby state</summary>
		/// <exception cref="System.IO.IOException"/>
		void StartStandbyServices();

		/// <summary>Prepare to exit the standby state</summary>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		void PrepareToStopStandbyServices();

		/// <summary>Stop the services when exiting standby state</summary>
		/// <exception cref="System.IO.IOException"/>
		void StopStandbyServices();

		/// <summary>
		/// Take a write-lock on the underlying namesystem
		/// so that no concurrent state transitions or edits
		/// can be made.
		/// </summary>
		void WriteLock();

		/// <summary>
		/// Unlock the lock taken by
		/// <see cref="WriteLock()"/>
		/// </summary>
		void WriteUnlock();

		/// <summary>Verify that the given operation category is allowed in the current state.
		/// 	</summary>
		/// <remarks>
		/// Verify that the given operation category is allowed in the current state.
		/// This is to allow NN implementations (eg BackupNode) to override it with
		/// node-specific handling.
		/// If the operation which is being checked will be taking the FSNS lock, it's
		/// advisable to check the operation category both immediately before and after
		/// taking the lock. This is because clients rely on the StandbyException
		/// thrown by this method in order to trigger client failover, and if a client
		/// first tries to contact the Standby NN, it could block for a long time if
		/// the Standby is holding the lock for a while, e.g. when performing a
		/// checkpoint. See HDFS-4591 for more details.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		void CheckOperation(NameNode.OperationCategory op);

		/// <returns>
		/// true if the node should allow stale reads (ie reads
		/// while the namespace is not up to date)
		/// </returns>
		bool AllowStaleReads();
	}
}
