using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics;


namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>
	/// The MetricsIntValue class is for a metric that is not time varied
	/// but changes only when it is set.
	/// </summary>
	/// <remarks>
	/// The MetricsIntValue class is for a metric that is not time varied
	/// but changes only when it is set.
	/// Each time its value is set, it is published only *once* at the next update
	/// call.
	/// </remarks>
	public class MetricsIntValue : MetricsBase
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.metrics.util"
			);

		private int value;

		private bool changed;

		/// <summary>Constructor - create a new metric</summary>
		/// <param name="nam">the name of the metrics to be used to publish the metric</param>
		/// <param name="registry">- where the metrics object will be registered</param>
		public MetricsIntValue(string nam, MetricsRegistry registry, string description)
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
		public MetricsIntValue(string nam, MetricsRegistry registry)
			: this(nam, registry, NoDescription)
		{
		}

		/// <summary>Set the value</summary>
		/// <param name="newValue"/>
		public virtual void Set(int newValue)
		{
			lock (this)
			{
				value = newValue;
				changed = true;
			}
		}

		/// <summary>Get value</summary>
		/// <returns>the value last set</returns>
		public virtual int Get()
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
					try
					{
						mr.SetMetric(GetName(), value);
					}
					catch (Exception e)
					{
						Log.Info("pushMetric failed for " + GetName() + "\n", e);
					}
				}
				changed = false;
			}
		}
	}
}
