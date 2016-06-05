using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class is a concrete PeriodicStatsAccumulator that deals with
	/// measurements where the raw data are a measurement of a
	/// time-varying quantity.
	/// </summary>
	/// <remarks>
	/// This class is a concrete PeriodicStatsAccumulator that deals with
	/// measurements where the raw data are a measurement of a
	/// time-varying quantity.  The result in each bucket is the estimate
	/// of the progress-weighted mean value of that quantity over the
	/// progress range covered by the bucket.
	/// <p>An easy-to-understand example of this kind of quantity would be
	/// a temperature.  It makes sense to consider the mean temperature
	/// over a progress range.
	/// </remarks>
	internal class StatePeriodicStats : PeriodicStatsAccumulator
	{
		internal StatePeriodicStats(int count)
			: base(count)
		{
		}

		/// <summary>
		/// accumulates a new reading by keeping a running account of the
		/// area under the piecewise linear curve marked by pairs of
		/// <c>newProgress, newValue</c>
		/// .
		/// </summary>
		protected internal override void ExtendInternal(double newProgress, int newValue)
		{
			if (state == null)
			{
				return;
			}
			// the effective height of this trapezoid if rectangularized
			double mean = ((double)newValue + (double)state.oldValue) / 2.0D;
			// conceptually mean *  (newProgress - state.oldProgress) / (1 / count)
			state.currentAccumulation += mean * (newProgress - state.oldProgress) * count;
		}
	}
}
