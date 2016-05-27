using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
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
	public class MutableQuantiles : MutableMetric
	{
		[VisibleForTesting]
		public static readonly Quantile[] quantiles = new Quantile[] { new Quantile(0.50, 
			0.050), new Quantile(0.75, 0.025), new Quantile(0.90, 0.010), new Quantile(0.95, 
			0.005), new Quantile(0.99, 0.001) };

		private readonly MetricsInfo numInfo;

		private readonly MetricsInfo[] quantileInfos;

		private readonly int interval;

		private SampleQuantiles estimator;

		private long previousCount = 0;

		[VisibleForTesting]
		protected internal IDictionary<Quantile, long> previousSnapshot = null;

		private static readonly ScheduledExecutorService scheduler = Executors.NewScheduledThreadPool
			(1, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("MutableQuantiles-%d"
			).Build());

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
			string ucName = StringUtils.Capitalize(name);
			string usName = StringUtils.Capitalize(sampleName);
			string uvName = StringUtils.Capitalize(valueName);
			string desc = StringUtils.Uncapitalize(description);
			string lsName = StringUtils.Uncapitalize(sampleName);
			string lvName = StringUtils.Uncapitalize(valueName);
			numInfo = Interns.Info(ucName + "Num" + usName, string.Format("Number of %s for %s with %ds interval"
				, lsName, desc, interval));
			// Construct the MetricsInfos for the quantiles, converting to percentiles
			quantileInfos = new MetricsInfo[quantiles.Length];
			string nameTemplate = ucName + "%dthPercentile" + uvName;
			string descTemplate = "%d percentile " + lvName + " with " + interval + " second interval for "
				 + desc;
			for (int i = 0; i < quantiles.Length; i++)
			{
				int percentile = (int)(100 * quantiles[i].quantile);
				quantileInfos[i] = Interns.Info(string.Format(nameTemplate, percentile), string.Format
					(descTemplate, percentile));
			}
			estimator = new SampleQuantiles(quantiles);
			this.interval = interval;
			scheduler.ScheduleAtFixedRate(new MutableQuantiles.RolloverSample(this), interval
				, interval, TimeUnit.Seconds);
		}

		public override void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			lock (this)
			{
				if (all || Changed())
				{
					builder.AddGauge(numInfo, previousCount);
					for (int i = 0; i < quantiles.Length; i++)
					{
						long newValue = 0;
						// If snapshot is null, we failed to update since the window was empty
						if (previousSnapshot != null)
						{
							newValue = previousSnapshot[quantiles[i]];
						}
						builder.AddGauge(quantileInfos[i], newValue);
					}
					if (Changed())
					{
						ClearChanged();
					}
				}
			}
		}

		public virtual void Add(long value)
		{
			lock (this)
			{
				estimator.Insert(value);
			}
		}

		public virtual int GetInterval()
		{
			return interval;
		}

		/// <summary>
		/// Runnable used to periodically roll over the internal
		/// <see cref="Org.Apache.Hadoop.Metrics2.Util.SampleQuantiles"/>
		/// every interval.
		/// </summary>
		private class RolloverSample : Runnable
		{
			internal MutableQuantiles parent;

			public RolloverSample(MutableQuantiles parent)
			{
				this.parent = parent;
			}

			public virtual void Run()
			{
				lock (parent)
				{
					parent.previousCount = parent.estimator.GetCount();
					parent.previousSnapshot = parent.estimator.Snapshot();
					parent.estimator.Clear();
				}
				parent.SetChanged();
			}
		}
	}
}
