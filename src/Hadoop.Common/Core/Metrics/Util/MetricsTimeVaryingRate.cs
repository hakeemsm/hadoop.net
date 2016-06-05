using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics;


namespace Org.Apache.Hadoop.Metrics.Util
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
	public class MetricsTimeVaryingRate : MetricsBase
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.metrics.util"
			);

		internal class Metrics
		{
			internal int numOperations = 0;

			internal long time = 0;

			// total time or average time
			internal virtual void Set(MetricsTimeVaryingRate.Metrics resetTo)
			{
				numOperations = resetTo.numOperations;
				time = resetTo.time;
			}

			internal virtual void Reset()
			{
				numOperations = 0;
				time = 0;
			}
		}

		internal class MinMax
		{
			internal long minTime = -1;

			internal long maxTime = 0;

			internal virtual void Set(MetricsTimeVaryingRate.MinMax newVal)
			{
				minTime = newVal.minTime;
				maxTime = newVal.maxTime;
			}

			internal virtual void Reset()
			{
				minTime = -1;
				maxTime = 0;
			}

			internal virtual void Update(long time)
			{
				// update min max
				minTime = (minTime == -1) ? time : Math.Min(minTime, time);
				minTime = Math.Min(minTime, time);
				maxTime = Math.Max(maxTime, time);
			}
		}

		private MetricsTimeVaryingRate.Metrics currentData;

		private MetricsTimeVaryingRate.Metrics previousIntervalData;

		private MetricsTimeVaryingRate.MinMax minMax;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsTimeVaryingRate(string nam, MetricsRegistry registry, string description
			)
			: base(nam, description)
		{
			currentData = new MetricsTimeVaryingRate.Metrics();
			previousIntervalData = new MetricsTimeVaryingRate.Metrics();
			minMax = new MetricsTimeVaryingRate.MinMax();
			registry.Add(nam, this);
		}

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">
		/// - where the metrics object will be registered
		/// A description of
		/// <see cref="MetricsBase.NoDescription"/>
		/// is used
		/// </param>
		public MetricsTimeVaryingRate(string nam, MetricsRegistry registry)
			: this(nam, registry, NoDescription)
		{
		}

		/// <summary>Increment the metrics for numOps operations</summary>
		/// <param name="numOps">- number of operations</param>
		/// <param name="time">- time for numOps operations</param>
		public virtual void Inc(int numOps, long time)
		{
			lock (this)
			{
				currentData.numOperations += numOps;
				currentData.time += time;
				long timePerOps = time / numOps;
				minMax.Update(timePerOps);
			}
		}

		/// <summary>Increment the metrics for one operation</summary>
		/// <param name="time">for one operation</param>
		public virtual void Inc(long time)
		{
			lock (this)
			{
				currentData.numOperations++;
				currentData.time += time;
				minMax.Update(time);
			}
		}

		private void IntervalHeartBeat()
		{
			lock (this)
			{
				previousIntervalData.numOperations = currentData.numOperations;
				previousIntervalData.time = (currentData.numOperations == 0) ? 0 : currentData.time
					 / currentData.numOperations;
				currentData.Reset();
			}
		}

		/// <summary>Push the delta  metrics to the mr.</summary>
		/// <remarks>
		/// Push the delta  metrics to the mr.
		/// The delta is since the last push/interval.
		/// Note this does NOT push to JMX
		/// (JMX gets the info via
		/// <see cref="GetPreviousIntervalAverageTime()"/>
		/// and
		/// <see cref="GetPreviousIntervalNumOps()"/>
		/// </remarks>
		/// <param name="mr"/>
		public override void PushMetric(MetricsRecord mr)
		{
			lock (this)
			{
				IntervalHeartBeat();
				try
				{
					mr.IncrMetric(GetName() + "_num_ops", GetPreviousIntervalNumOps());
					mr.SetMetric(GetName() + "_avg_time", GetPreviousIntervalAverageTime());
				}
				catch (Exception e)
				{
					Log.Info("pushMetric failed for " + GetName() + "\n", e);
				}
			}
		}

		/// <summary>The number of operations in the previous interval</summary>
		/// <returns>- ops in prev interval</returns>
		public virtual int GetPreviousIntervalNumOps()
		{
			lock (this)
			{
				return previousIntervalData.numOperations;
			}
		}

		/// <summary>The average rate of an operation in the previous interval</summary>
		/// <returns>- the average rate.</returns>
		public virtual long GetPreviousIntervalAverageTime()
		{
			lock (this)
			{
				return previousIntervalData.time;
			}
		}

		/// <summary>
		/// The min time for a single operation since the last reset
		/// <see cref="ResetMinMax()"/>
		/// </summary>
		/// <returns>min time for an operation</returns>
		public virtual long GetMinTime()
		{
			lock (this)
			{
				return minMax.minTime;
			}
		}

		/// <summary>
		/// The max time for a single operation since the last reset
		/// <see cref="ResetMinMax()"/>
		/// </summary>
		/// <returns>max time for an operation</returns>
		public virtual long GetMaxTime()
		{
			lock (this)
			{
				return minMax.maxTime;
			}
		}

		/// <summary>Reset the min max values</summary>
		public virtual void ResetMinMax()
		{
			lock (this)
			{
				minMax.Reset();
			}
		}
	}
}
