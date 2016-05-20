using Sharpen;

namespace org.apache.hadoop.ipc
{
	public interface FairCallQueueMXBean
	{
		// Get the size of each subqueue, the index corrosponding to the priority
		// level.
		int[] getQueueSizes();

		long[] getOverflowedCalls();

		int getRevision();
	}
}
