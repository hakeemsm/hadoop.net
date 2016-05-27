using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public interface FairCallQueueMXBean
	{
		// Get the size of each subqueue, the index corrosponding to the priority
		// level.
		int[] GetQueueSizes();

		long[] GetOverflowedCalls();

		int GetRevision();
	}
}
