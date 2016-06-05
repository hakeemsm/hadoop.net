using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class WrappedPeriodicStatsAccumulator
	{
		private PeriodicStatsAccumulator real;

		public WrappedPeriodicStatsAccumulator(PeriodicStatsAccumulator real)
		{
			//Workaround for PeriodicStateAccumulator being package access
			this.real = real;
		}

		public virtual void Extend(double newProgress, int newValue)
		{
			real.Extend(newProgress, newValue);
		}
	}
}
