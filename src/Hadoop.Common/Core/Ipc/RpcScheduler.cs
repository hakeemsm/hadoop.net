

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Implement this interface to be used for RPC scheduling in the fair call queues.
	/// 	</summary>
	public interface RpcScheduler
	{
		/// <summary>Returns priority level greater than zero as a hint for scheduling.</summary>
		int GetPriorityLevel(Schedulable obj);
	}
}
