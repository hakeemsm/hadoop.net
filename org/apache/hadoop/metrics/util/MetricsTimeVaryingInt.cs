using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>
	/// The MetricsTimeVaryingInt class is for a metric that naturally
	/// varies over time (e.g.
	/// </summary>
	/// <remarks>
	/// The MetricsTimeVaryingInt class is for a metric that naturally
	/// varies over time (e.g. number of files created). The metrics is accumulated
	/// over an interval (set in the metrics config file); the metrics is
	/// published at the end of each interval and then
	/// reset to zero. Hence the counter has the value in the current interval.
	/// Note if one wants a time associated with the metric then use
	/// </remarks>
	/// <seealso cref="MetricsTimeVaryingRate"/>
	public class MetricsTimeVaryingInt : org.apache.hadoop.metrics.util.MetricsBase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog("org.apache.hadoop.metrics.util");

		private int currentValue;

		private int previousIntervalValue;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		/// <param name="description">- the description</param>
		public MetricsTimeVaryingInt(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry, string description)
			: base(nam, description)
		{
			currentValue = 0;
			previousIntervalValue = 0;
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
		public MetricsTimeVaryingInt(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry)
			: this(nam, registry, NO_DESCRIPTION)
		{
		}

		/// <summary>Inc metrics for incr vlaue</summary>
		/// <param name="incr">- number of operations</param>
		public virtual void inc(int incr)
		{
			lock (this)
			{
				currentValue += incr;
			}
		}

		/// <summary>Inc metrics by one</summary>
		public virtual void inc()
		{
			lock (this)
			{
				currentValue++;
			}
		}

		private void intervalHeartBeat()
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
		public override void pushMetric(org.apache.hadoop.metrics.MetricsRecord mr)
		{
			lock (this)
			{
				intervalHeartBeat();
				try
				{
					mr.incrMetric(getName(), getPreviousIntervalValue());
				}
				catch (System.Exception e)
				{
					LOG.info("pushMetric failed for " + getName() + "\n", e);
				}
			}
		}

		/// <summary>The Value at the Previous interval</summary>
		/// <returns>prev interval value</returns>
		public virtual int getPreviousIntervalValue()
		{
			lock (this)
			{
				return previousIntervalValue;
			}
		}

		/// <summary>The Value at the current interval</summary>
		/// <returns>prev interval value</returns>
		public virtual int getCurrentIntervalValue()
		{
			lock (this)
			{
				return currentValue;
			}
		}
	}
}
