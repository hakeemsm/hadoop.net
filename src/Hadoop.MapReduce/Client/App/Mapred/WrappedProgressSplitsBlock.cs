using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class WrappedProgressSplitsBlock : ProgressSplitsBlock
	{
		private WrappedPeriodicStatsAccumulator wrappedProgressWallclockTime;

		private WrappedPeriodicStatsAccumulator wrappedProgressCPUTime;

		private WrappedPeriodicStatsAccumulator wrappedProgressVirtualMemoryKbytes;

		private WrappedPeriodicStatsAccumulator wrappedProgressPhysicalMemoryKbytes;

		public WrappedProgressSplitsBlock(int numberSplits)
			: base(numberSplits)
		{
		}

		// Workaround for ProgressSplitBlock being package access
		internal override int[][] Burst()
		{
			return base.Burst();
		}

		public virtual WrappedPeriodicStatsAccumulator GetProgressWallclockTime()
		{
			if (wrappedProgressWallclockTime == null)
			{
				wrappedProgressWallclockTime = new WrappedPeriodicStatsAccumulator(progressWallclockTime
					);
			}
			return wrappedProgressWallclockTime;
		}

		public virtual WrappedPeriodicStatsAccumulator GetProgressCPUTime()
		{
			if (wrappedProgressCPUTime == null)
			{
				wrappedProgressCPUTime = new WrappedPeriodicStatsAccumulator(progressCPUTime);
			}
			return wrappedProgressCPUTime;
		}

		public virtual WrappedPeriodicStatsAccumulator GetProgressVirtualMemoryKbytes()
		{
			if (wrappedProgressVirtualMemoryKbytes == null)
			{
				wrappedProgressVirtualMemoryKbytes = new WrappedPeriodicStatsAccumulator(progressVirtualMemoryKbytes
					);
			}
			return wrappedProgressVirtualMemoryKbytes;
		}

		public virtual WrappedPeriodicStatsAccumulator GetProgressPhysicalMemoryKbytes()
		{
			if (wrappedProgressPhysicalMemoryKbytes == null)
			{
				wrappedProgressPhysicalMemoryKbytes = new WrappedPeriodicStatsAccumulator(progressPhysicalMemoryKbytes
					);
			}
			return wrappedProgressPhysicalMemoryKbytes;
		}
	}
}
