using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public enum TaskCounter
	{
		MapInputRecords,
		MapOutputRecords,
		MapSkippedRecords,
		MapOutputBytes,
		MapOutputMaterializedBytes,
		SplitRawBytes,
		CombineInputRecords,
		CombineOutputRecords,
		ReduceInputGroups,
		ReduceShuffleBytes,
		ReduceInputRecords,
		ReduceOutputRecords,
		ReduceSkippedGroups,
		ReduceSkippedRecords,
		SpilledRecords,
		ShuffledMaps,
		FailedShuffle,
		MergedMapOutputs,
		GcTimeMillis,
		CpuMilliseconds,
		PhysicalMemoryBytes,
		VirtualMemoryBytes,
		CommittedHeapBytes
	}
}
