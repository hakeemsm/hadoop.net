using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>
	/// Watches a stream of long values, maintaining online estimates of specific
	/// quantiles with provably low error bounds.
	/// </summary>
	/// <remarks>
	/// Watches a stream of long values, maintaining online estimates of specific
	/// quantiles with provably low error bounds. This is particularly useful for
	/// accurate high-percentile (e.g. 95th, 99th) latency metrics.
	/// </remarks>
	public class MutableQuantiles : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		[com.google.common.annotations.VisibleForTesting]
		public static readonly org.apache.hadoop.metrics2.util.Quantile[] quantiles = new 
			org.apache.hadoop.metrics2.util.Quantile[] { new org.apache.hadoop.metrics2.util.Quantile
			(0.50, 0.050), new org.apache.hadoop.metrics2.util.Quantile(0.75, 0.025), new org.apache.hadoop.metrics2.util.Quantile
			(0.90, 0.010), new org.apache.hadoop.metrics2.util.Quantile(0.95, 0.005), new org.apache.hadoop.metrics2.util.Quantile
			(0.99, 0.001) };

		private readonly org.apache.hadoop.metrics2.MetricsInfo numInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo[] quantileInfos;

		private readonly int interval;

		private org.apache.hadoop.metrics2.util.SampleQuantiles estimator;

		private long previousCount = 0;

		[com.google.common.annotations.VisibleForTesting]
		protected internal System.Collections.Generic.IDictionary<org.apache.hadoop.metrics2.util.Quantile
			, long> previousSnapshot = null;

		private static readonly java.util.concurrent.ScheduledExecutorService scheduler = 
			java.util.concurrent.Executors.newScheduledThreadPool(1, new com.google.common.util.concurrent.ThreadFactoryBuilder
			().setDaemon(true).setNameFormat("MutableQuantiles-%d").build());

		/// <summary>
		/// Instantiates a new
		/// <see cref="MutableQuantiles"/>
		/// for a metric that rolls itself
		/// over on the specified time interval.
		/// </summary>
		/// <param name="name">of the metric</param>
		/// <param name="description">long-form textual description of the metric</param>
		/// <param name="sampleName">type of items in the stream (e.g., "Ops")</param>
		/// <param name="valueName">type of the values</param>
		/// <param name="interval">rollover interval (in seconds) of the estimator</param>
		public MutableQuantiles(string name, string description, string sampleName, string
			 valueName, int interval)
		{
			string ucName = org.apache.commons.lang.StringUtils.capitalize(name);
			string usName = org.apache.commons.lang.StringUtils.capitalize(sampleName);
			string uvName = org.apache.commons.lang.StringUtils.capitalize(valueName);
			string desc = org.apache.commons.lang.StringUtils.uncapitalize(description);
			string lsName = org.apache.commons.lang.StringUtils.uncapitalize(sampleName);
			string lvName = org.apache.commons.lang.StringUtils.uncapitalize(valueName);
			numInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Num" + usName, string
				.format("Number of %s for %s with %ds interval", lsName, desc, interval));
			// Construct the MetricsInfos for the quantiles, converting to percentiles
			quantileInfos = new org.apache.hadoop.metrics2.MetricsInfo[quantiles.Length];
			string nameTemplate = ucName + "%dthPercentile" + uvName;
			string descTemplate = "%d percentile " + lvName + " with " + interval + " second interval for "
				 + desc;
			for (int i = 0; i < quantiles.Length; i++)
			{
				int percentile = (int)(100 * quantiles[i].quantile);
				quantileInfos[i] = org.apache.hadoop.metrics2.lib.Interns.info(string.format(nameTemplate
					, percentile), string.format(descTemplate, percentile));
			}
			estimator = new org.apache.hadoop.metrics2.util.SampleQuantiles(quantiles);
			this.interval = interval;
			scheduler.scheduleAtFixedRate(new org.apache.hadoop.metrics2.lib.MutableQuantiles.RolloverSample
				(this), interval, interval, java.util.concurrent.TimeUnit.SECONDS);
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			lock (this)
			{
				if (all || changed())
				{
					builder.addGauge(numInfo, previousCount);
					for (int i = 0; i < quantiles.Length; i++)
					{
						long newValue = 0;
						// If snapshot is null, we failed to update since the window was empty
						if (previousSnapshot != null)
						{
							newValue = previousSnapshot[quantiles[i]];
						}
						builder.addGauge(quantileInfos[i], newValue);
					}
					if (changed())
					{
						clearChanged();
					}
				}
			}
		}

		public virtual void add(long value)
		{
			lock (this)
			{
				estimator.insert(value);
			}
		}

		public virtual int getInterval()
		{
			return interval;
		}

		/// <summary>
		/// Runnable used to periodically roll over the internal
		/// <see cref="org.apache.hadoop.metrics2.util.SampleQuantiles"/>
		/// every interval.
		/// </summary>
		private class RolloverSample : java.lang.Runnable
		{
			internal org.apache.hadoop.metrics2.lib.MutableQuantiles parent;

			public RolloverSample(org.apache.hadoop.metrics2.lib.MutableQuantiles parent)
			{
				this.parent = parent;
			}

			public virtual void run()
			{
				lock (parent)
				{
					parent.previousCount = parent.estimator.getCount();
					parent.previousSnapshot = parent.estimator.snapshot();
					parent.estimator.clear();
				}
				parent.setChanged();
			}
		}
	}
}
