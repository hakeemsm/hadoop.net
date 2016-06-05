using System.Text;
using Mono.Math;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class CpuTimeTracker
	{
		public const int Unavailable = ResourceCalculatorProcessTree.Unavailable;

		internal readonly long MinimumUpdateInterval;

		internal BigInteger cumulativeCpuTime = BigInteger.Zero;

		internal BigInteger lastCumulativeCpuTime = BigInteger.Zero;

		internal long sampleTime;

		internal long lastSampleTime;

		internal float cpuUsage;

		internal BigInteger jiffyLengthInMillis;

		public CpuTimeTracker(long jiffyLengthInMillis)
		{
			// CPU used time since system is on (ms)
			// CPU used time read last time (ms)
			// Unix timestamp while reading the CPU time (ms)
			this.jiffyLengthInMillis = BigInteger.ValueOf(jiffyLengthInMillis);
			this.cpuUsage = Unavailable;
			this.sampleTime = Unavailable;
			this.lastSampleTime = Unavailable;
			MinimumUpdateInterval = 10 * jiffyLengthInMillis;
		}

		/// <summary>Return percentage of cpu time spent over the time since last update.</summary>
		/// <remarks>
		/// Return percentage of cpu time spent over the time since last update.
		/// CPU time spent is based on elapsed jiffies multiplied by amount of
		/// time for 1 core. Thus, if you use 2 cores completely you would have spent
		/// twice the actual time between updates and this will return 200%.
		/// </remarks>
		/// <returns>
		/// Return percentage of cpu usage since last update,
		/// <see cref="Unavailable"/>
		/// if there haven't been 2 updates more than
		/// <see cref="MinimumUpdateInterval"/>
		/// apart
		/// </returns>
		public virtual float GetCpuTrackerUsagePercent()
		{
			if (lastSampleTime == Unavailable || lastSampleTime > sampleTime)
			{
				// lastSampleTime > sampleTime may happen when the system time is changed
				lastSampleTime = sampleTime;
				lastCumulativeCpuTime = cumulativeCpuTime;
				return cpuUsage;
			}
			// When lastSampleTime is sufficiently old, update cpuUsage.
			// Also take a sample of the current time and cumulative CPU time for the
			// use of the next calculation.
			if (sampleTime > lastSampleTime + MinimumUpdateInterval)
			{
				cpuUsage = ((cumulativeCpuTime.Subtract(lastCumulativeCpuTime))) * 100F / ((float
					)(sampleTime - lastSampleTime));
				lastSampleTime = sampleTime;
				lastCumulativeCpuTime = cumulativeCpuTime;
			}
			return cpuUsage;
		}

		public virtual void UpdateElapsedJiffies(BigInteger elapedJiffies, long sampleTime
			)
		{
			this.cumulativeCpuTime = elapedJiffies.Multiply(jiffyLengthInMillis);
			this.sampleTime = sampleTime;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("SampleTime " + this.sampleTime);
			sb.Append(" CummulativeCpuTime " + this.cumulativeCpuTime);
			sb.Append(" LastSampleTime " + this.lastSampleTime);
			sb.Append(" LastCummulativeCpuTime " + this.lastCumulativeCpuTime);
			sb.Append(" CpuUsage " + this.cpuUsage);
			sb.Append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
			return sb.ToString();
		}
	}
}
