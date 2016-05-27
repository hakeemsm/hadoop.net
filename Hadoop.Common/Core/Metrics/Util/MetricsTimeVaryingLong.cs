using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>
	/// The MetricsTimeVaryingLong class is for a metric that naturally
	/// varies over time (e.g.
	/// </summary>
	/// <remarks>
	/// The MetricsTimeVaryingLong class is for a metric that naturally
	/// varies over time (e.g. number of files created). The metrics is accumulated
	/// over an interval (set in the metrics config file); the metrics is
	/// published at the end of each interval and then
	/// reset to zero. Hence the counter has the value in the current interval.
	/// Note if one wants a time associated with the metric then use
	/// </remarks>
	/// <seealso cref="MetricsTimeVaryingRate"/>
	public class MetricsTimeVaryingLong : MetricsBase
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.metrics.util"
			);

		private long currentValue;

		private long previousIntervalValue;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsTimeVaryingLong(string nam, MetricsRegistry registry, string description
			)
			: base(nam, description)
		{
			currentValue = 0;
			previousIntervalValue = 0;
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
		public MetricsTimeVaryingLong(string nam, MetricsRegistry registry)
			: this(nam, registry, NoDescription)
		{
		}

		/// <summary>Inc metrics for incr vlaue</summary>
		/// <param name="incr">- number of operations</param>
		public virtual void Inc(long incr)
		{
			lock (this)
			{
				currentValue += incr;
			}
		}

		/// <summary>Inc metrics by one</summary>
		public virtual void Inc()
		{
			lock (this)
			{
				currentValue++;
			}
		}

		private void IntervalHeartBeat()
		{
			lock (this)
			{
				previousIntervalValue = currentValue;
				currentValue = 0;
			}
		}

		/// <summary>Push the delta  metrics to the mr.</summary>
		/// <remarks>
		/// Push the delta  metrics to the mr.
		/// The delta is since the last push/interval.
		/// Note this does NOT push to JMX
		/// (JMX gets the info via
		/// <see cref="previousIntervalValue"/>
		/// </remarks>
		/// <param name="mr"/>
		public override void PushMetric(MetricsRecord mr)
		{
			lock (this)
			{
				IntervalHeartBeat();
				try
				{
					mr.IncrMetric(GetName(), GetPreviousIntervalValue());
				}
				catch (Exception e)
				{
					Log.Info("pushMetric failed for " + GetName() + "\n", e);
				}
			}
		}

		/// <summary>The Value at the Previous interval</summary>
		/// <returns>prev interval value</returns>
		public virtual long GetPreviousIntervalValue()
		{
			lock (this)
			{
				return previousIntervalValue;
			}
		}

		/// <summary>The Value at the current interval</summary>
		/// <returns>prev interval value</returns>
		public virtual long GetCurrentIntervalValue()
		{
			lock (this)
			{
				return currentValue;
			}
		}
	}
}
