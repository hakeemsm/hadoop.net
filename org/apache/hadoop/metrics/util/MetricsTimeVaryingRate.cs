using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>
	/// The MetricsTimeVaryingRate class is for a rate based metric that
	/// naturally varies over time (e.g.
	/// </summary>
	/// <remarks>
	/// The MetricsTimeVaryingRate class is for a rate based metric that
	/// naturally varies over time (e.g. time taken to create a file).
	/// The rate is averaged at each interval heart beat (the interval
	/// is set in the metrics config file).
	/// This class also keeps track of the min and max rates along with
	/// a method to reset the min-max.
	/// </remarks>
	public class MetricsTimeVaryingRate : org.apache.hadoop.metrics.util.MetricsBase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog("org.apache.hadoop.metrics.util");

		internal class Metrics
		{
			internal int numOperations = 0;

			internal long time = 0;

			// total time or average time
			internal virtual void set(org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.Metrics
				 resetTo)
			{
				numOperations = resetTo.numOperations;
				time = resetTo.time;
			}

			internal virtual void reset()
			{
				numOperations = 0;
				time = 0;
			}
		}

		internal class MinMax
		{
			internal long minTime = -1;

			internal long maxTime = 0;

			internal virtual void set(org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.MinMax
				 newVal)
			{
				minTime = newVal.minTime;
				maxTime = newVal.maxTime;
			}

			internal virtual void reset()
			{
				minTime = -1;
				maxTime = 0;
			}

			internal virtual void update(long time)
			{
				// update min max
				minTime = (minTime == -1) ? time : System.Math.min(minTime, time);
				minTime = System.Math.min(minTime, time);
				maxTime = System.Math.max(maxTime, time);
			}
		}

		private org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.Metrics currentData;

		private org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.Metrics previousIntervalData;

		private org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.MinMax minMax;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsTimeVaryingRate(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry, string description)
			: base(nam, description)
		{
			currentData = new org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.Metrics();
			previousIntervalData = new org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.Metrics
				();
			minMax = new org.apache.hadoop.metrics.util.MetricsTimeVaryingRate.MinMax();
			registry.add(nam, this);
		}

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">
		/// - where the metrics object will be registered
		/// A description of
		/// <see cref="MetricsBase.NO_DESCRIPTION"/>
		/// is used
		/// </param>
		public MetricsTimeVaryingRate(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry)
			: this(nam, registry, NO_DESCRIPTION)
		{
		}

		/// <summary>Increment the metrics for numOps operations</summary>
		/// <param name="numOps">- number of operations</param>
		/// <param name="time">- time for numOps operations</param>
		public virtual void inc(int numOps, long time)
		{
			lock (this)
			{
				currentData.numOperations += numOps;
				currentData.time += time;
				long timePerOps = time / numOps;
				minMax.update(timePerOps);
			}
		}

		/// <summary>Increment the metrics for one operation</summary>
		/// <param name="time">for one operation</param>
		public virtual void inc(long time)
		{
			lock (this)
			{
				currentData.numOperations++;
				currentData.time += time;
				minMax.update(time);
			}
		}

		private void intervalHeartBeat()
		{
			lock (this)
			{
				previousIntervalData.numOperations = currentData.numOperations;
				previousIntervalData.time = (currentData.numOperations == 0) ? 0 : currentData.time
					 / currentData.numOperations;
				currentData.reset();
			}
		}

		/// <summary>Push the delta  metrics to the mr.</summary>
		/// <remarks>
		/// Push the delta  metrics to the mr.
		/// The delta is since the last push/interval.
		/// Note this does NOT push to JMX
		/// (JMX gets the info via
		/// <see cref="getPreviousIntervalAverageTime()"/>
		/// and
		/// <see cref="getPreviousIntervalNumOps()"/>
		/// </remarks>
		/// <param name="mr"/>
		public override void pushMetric(org.apache.hadoop.metrics.MetricsRecord mr)
		{
			lock (this)
			{
				intervalHeartBeat();
				try
				{
					mr.incrMetric(getName() + "_num_ops", getPreviousIntervalNumOps());
					mr.setMetric(getName() + "_avg_time", getPreviousIntervalAverageTime());
				}
				catch (System.Exception e)
				{
					LOG.info("pushMetric failed for " + getName() + "\n", e);
				}
			}
		}

		/// <summary>The number of operations in the previous interval</summary>
		/// <returns>- ops in prev interval</returns>
		public virtual int getPreviousIntervalNumOps()
		{
			lock (this)
			{
				return previousIntervalData.numOperations;
			}
		}

		/// <summary>The average rate of an operation in the previous interval</summary>
		/// <returns>- the average rate.</returns>
		public virtual long getPreviousIntervalAverageTime()
		{
			lock (this)
			{
				return previousIntervalData.time;
			}
		}

		/// <summary>
		/// The min time for a single operation since the last reset
		/// <see cref="resetMinMax()"/>
		/// </summary>
		/// <returns>min time for an operation</returns>
		public virtual long getMinTime()
		{
			lock (this)
			{
				return minMax.minTime;
			}
		}

		/// <summary>
		/// The max time for a single operation since the last reset
		/// <see cref="resetMinMax()"/>
		/// </summary>
		/// <returns>max time for an operation</returns>
		public virtual long getMaxTime()
		{
			lock (this)
			{
				return minMax.maxTime;
			}
		}

		/// <summary>Reset the min max values</summary>
		public virtual void resetMinMax()
		{
			lock (this)
			{
				minMax.reset();
			}
		}
	}
}
