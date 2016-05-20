using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>
	/// The MetricsLongValue class is for a metric that is not time varied
	/// but changes only when it is set.
	/// </summary>
	/// <remarks>
	/// The MetricsLongValue class is for a metric that is not time varied
	/// but changes only when it is set.
	/// Each time its value is set, it is published only *once* at the next update
	/// call.
	/// </remarks>
	public class MetricsLongValue : org.apache.hadoop.metrics.util.MetricsBase
	{
		private long value;

		private bool changed;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsLongValue(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry, string description)
			: base(nam, description)
		{
			value = 0;
			changed = false;
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
		public MetricsLongValue(string nam, org.apache.hadoop.metrics.util.MetricsRegistry
			 registry)
			: this(nam, registry, NO_DESCRIPTION)
		{
		}

		/// <summary>Set the value</summary>
		/// <param name="newValue"/>
		public virtual void set(long newValue)
		{
			lock (this)
			{
				value = newValue;
				changed = true;
			}
		}

		/// <summary>Get value</summary>
		/// <returns>the value last set</returns>
		public virtual long get()
		{
			lock (this)
			{
				return value;
			}
		}

		/// <summary>Push the metric to the mr.</summary>
		/// <remarks>
		/// Push the metric to the mr.
		/// The metric is pushed only if it was updated since last push
		/// Note this does NOT push to JMX
		/// (JMX gets the info via
		/// <see cref="get()"/>
		/// </remarks>
		/// <param name="mr"/>
		public override void pushMetric(org.apache.hadoop.metrics.MetricsRecord mr)
		{
			lock (this)
			{
				if (changed)
				{
					mr.setMetric(getName(), value);
				}
				changed = false;
			}
		}
	}
}
