using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Util
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
	public class MetricsLongValue : MetricsBase
	{
		private long value;

		private bool changed;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsLongValue(string nam, MetricsRegistry registry, string description)
			: base(nam, description)
		{
			value = 0;
			changed = false;
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
		public MetricsLongValue(string nam, MetricsRegistry registry)
			: this(nam, registry, NoDescription)
		{
		}

		/// <summary>Set the value</summary>
		/// <param name="newValue"/>
		public virtual void Set(long newValue)
		{
			lock (this)
			{
				value = newValue;
				changed = true;
			}
		}

		/// <summary>Get value</summary>
		/// <returns>the value last set</returns>
		public virtual long Get()
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
		/// <see cref="Get()"/>
		/// </remarks>
		/// <param name="mr"/>
		public override void PushMetric(MetricsRecord mr)
		{
			lock (this)
			{
				if (changed)
				{
					mr.SetMetric(GetName(), value);
				}
				changed = false;
			}
		}
	}
}
