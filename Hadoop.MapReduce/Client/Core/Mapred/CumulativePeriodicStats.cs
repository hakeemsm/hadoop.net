using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class is a concrete PeriodicStatsAccumulator that deals with
	/// measurements where the raw data are a measurement of an
	/// accumulation.
	/// </summary>
	/// <remarks>
	/// This class is a concrete PeriodicStatsAccumulator that deals with
	/// measurements where the raw data are a measurement of an
	/// accumulation.  The result in each bucket is the estimate
	/// of the progress-weighted change in that quantity over the
	/// progress range covered by the bucket.
	/// <p>An easy-to-understand example of this kind of quantity would be
	/// a distance traveled.  It makes sense to consider that portion of
	/// the total travel that can be apportioned to each bucket.
	/// </remarks>
	internal class CumulativePeriodicStats : PeriodicStatsAccumulator
	{
		internal int previousValue = 0;

		internal CumulativePeriodicStats(int count)
			: base(count)
		{
		}

		// int's are acceptable here, even though times are normally
		// long's, because these are a difference and an int won't
		// overflow for 24 days.  Tasks can't run for more than about a
		// week for other reasons, and most jobs would be written 
		/// <summary>
		/// accumulates a new reading by keeping a running account of the
		/// value distance from the beginning of the bucket to the end of
		/// this reading
		/// </summary>
		protected internal override void ExtendInternal(double newProgress, int newValue)
		{
			if (state == null)
			{
				return;
			}
			state.currentAccumulation += (double)(newValue - previousValue);
			previousValue = newValue;
		}
	}
}
