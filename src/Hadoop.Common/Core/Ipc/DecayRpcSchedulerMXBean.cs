

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Provides metrics for Decay scheduler.</summary>
	public interface DecayRpcSchedulerMXBean
	{
		// Get an overview of the requests in history.
		string GetSchedulingDecisionSummary();

		string GetCallVolumeSummary();

		int GetUniqueIdentityCount();

		long GetTotalCallVolume();
	}
}
