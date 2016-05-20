using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Provides metrics for Decay scheduler.</summary>
	public interface DecayRpcSchedulerMXBean
	{
		// Get an overview of the requests in history.
		string getSchedulingDecisionSummary();

		string getCallVolumeSummary();

		int getUniqueIdentityCount();

		long getTotalCallVolume();
	}
}
