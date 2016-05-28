using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ProgressSplitsBlock
	{
		internal readonly PeriodicStatsAccumulator progressWallclockTime;

		internal readonly PeriodicStatsAccumulator progressCPUTime;

		internal readonly PeriodicStatsAccumulator progressVirtualMemoryKbytes;

		internal readonly PeriodicStatsAccumulator progressPhysicalMemoryKbytes;

		internal static readonly int[] NullArray = new int[0];

		internal const int WallclockTimeIndex = 0;

		internal const int CpuTimeIndex = 1;

		internal const int VirtualMemoryKbytesIndex = 2;

		internal const int PhysicalMemoryKbytesIndex = 3;

		internal const int DefaultNumberProgressSplits = 12;

		internal ProgressSplitsBlock(int numberSplits)
		{
			/*
			* This object gathers the [currently four] PeriodStatset's that we
			* are gathering for a particular task attempt for packaging and
			* handling as a single object.
			*/
			progressWallclockTime = new CumulativePeriodicStats(numberSplits);
			progressCPUTime = new CumulativePeriodicStats(numberSplits);
			progressVirtualMemoryKbytes = new StatePeriodicStats(numberSplits);
			progressPhysicalMemoryKbytes = new StatePeriodicStats(numberSplits);
		}

		// this coordinates with LoggedTaskAttempt.SplitVectorKind
		internal virtual int[][] Burst()
		{
			int[][] result = new int[4][];
			result[WallclockTimeIndex] = progressWallclockTime.GetValues();
			result[CpuTimeIndex] = progressCPUTime.GetValues();
			result[VirtualMemoryKbytesIndex] = progressVirtualMemoryKbytes.GetValues();
			result[PhysicalMemoryKbytesIndex] = progressPhysicalMemoryKbytes.GetValues();
			return result;
		}

		public static int[] ArrayGet(int[][] burstedBlock, int index)
		{
			return burstedBlock == null ? NullArray : burstedBlock[index];
		}

		public static int[] ArrayGetWallclockTime(int[][] burstedBlock)
		{
			return ArrayGet(burstedBlock, WallclockTimeIndex);
		}

		public static int[] ArrayGetCPUTime(int[][] burstedBlock)
		{
			return ArrayGet(burstedBlock, CpuTimeIndex);
		}

		public static int[] ArrayGetVMemKbytes(int[][] burstedBlock)
		{
			return ArrayGet(burstedBlock, VirtualMemoryKbytesIndex);
		}

		public static int[] ArrayGetPhysMemKbytes(int[][] burstedBlock)
		{
			return ArrayGet(burstedBlock, PhysicalMemoryKbytesIndex);
		}
	}
}
