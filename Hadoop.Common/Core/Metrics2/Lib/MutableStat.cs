using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A mutable metric with stats.</summary>
	/// <remarks>
	/// A mutable metric with stats.
	/// Useful for keeping throughput/latency stats.
	/// </remarks>
	public class MutableStat : MutableMetric
	{
		private readonly MetricsInfo numInfo;

		private readonly MetricsInfo avgInfo;

		private readonly MetricsInfo stdevInfo;

		private readonly MetricsInfo iMinInfo;

		private readonly MetricsInfo iMaxInfo;

		private readonly MetricsInfo minInfo;

		private readonly MetricsInfo maxInfo;

		private readonly SampleStat intervalStat = new SampleStat();

		private readonly SampleStat prevStat = new SampleStat();

		private readonly SampleStat.MinMax minMax = new SampleStat.MinMax();

		private long numSamples = 0;

		private bool extended = false;

		/// <summary>Construct a sample statistics metric</summary>
		/// <param name="name">of the metric</param>
		/// <param name="description">of the metric</param>
		/// <param name="sampleName">of the metric (e.g. "Ops")</param>
		/// <param name="valueName">of the metric (e.g. "Time", "Latency")</param>
		/// <param name="extended">create extended stats (stdev, min/max etc.) by default.</param>
		public MutableStat(string name, string description, string sampleName, string valueName
			, bool extended)
		{
			string ucName = StringUtils.Capitalize(name);
			string usName = StringUtils.Capitalize(sampleName);
			string uvName = StringUtils.Capitalize(valueName);
			string desc = StringUtils.Uncapitalize(description);
			string lsName = StringUtils.Uncapitalize(sampleName);
			string lvName = StringUtils.Uncapitalize(valueName);
			numInfo = Interns.Info(ucName + "Num" + usName, "Number of " + lsName + " for " +
				 desc);
			avgInfo = Interns.Info(ucName + "Avg" + uvName, "Average " + lvName + " for " + desc
				);
			stdevInfo = Interns.Info(ucName + "Stdev" + uvName, "Standard deviation of " + lvName
				 + " for " + desc);
			iMinInfo = Interns.Info(ucName + "IMin" + uvName, "Interval min " + lvName + " for "
				 + desc);
			iMaxInfo = Interns.Info(ucName + "IMax" + uvName, "Interval max " + lvName + " for "
				 + desc);
			minInfo = Interns.Info(ucName + "Min" + uvName, "Min " + lvName + " for " + desc);
			maxInfo = Interns.Info(ucName + "Max" + uvName, "Max " + lvName + " for " + desc);
			this.extended = extended;
		}

		/// <summary>Construct a snapshot stat metric with extended stat off by default</summary>
		/// <param name="name">of the metric</param>
		/// <param name="description">of the metric</param>
		/// <param name="sampleName">of the metric (e.g. "Ops")</param>
		/// <param name="valueName">of the metric (e.g. "Time", "Latency")</param>
		public MutableStat(string name, string description, string sampleName, string valueName
			)
			: this(name, description, sampleName, valueName, false)
		{
		}

		/// <summary>Set whether to display the extended stats (stdev, min/max etc.) or not</summary>
		/// <param name="extended">enable/disable displaying extended stats</param>
		public virtual void SetExtended(bool extended)
		{
			lock (this)
			{
				this.extended = extended;
			}
		}

		/// <summary>Add a number of samples and their sum to the running stat</summary>
		/// <param name="numSamples">number of samples</param>
		/// <param name="sum">of the samples</param>
		public virtual void Add(long numSamples, long sum)
		{
			lock (this)
			{
				intervalStat.Add(numSamples, sum);
				SetChanged();
			}
		}

		/// <summary>Add a snapshot to the metric</summary>
		/// <param name="value">of the metric</param>
		public virtual void Add(long value)
		{
			lock (this)
			{
				intervalStat.Add(value);
				minMax.Add(value);
				SetChanged();
			}
		}

		public override void Snapshot(MetricsRecordBuilder builder, bool all)
		{
			lock (this)
			{
				if (all || Changed())
				{
					numSamples += intervalStat.NumSamples();
					builder.AddCounter(numInfo, numSamples).AddGauge(avgInfo, LastStat().Mean());
					if (extended)
					{
						builder.AddGauge(stdevInfo, LastStat().Stddev()).AddGauge(iMinInfo, LastStat().Min
							()).AddGauge(iMaxInfo, LastStat().Max()).AddGauge(minInfo, minMax.Min()).AddGauge
							(maxInfo, minMax.Max());
					}
					if (Changed())
					{
						if (numSamples > 0)
						{
							intervalStat.CopyTo(prevStat);
							intervalStat.Reset();
						}
						ClearChanged();
					}
				}
			}
		}

		private SampleStat LastStat()
		{
			return Changed() ? intervalStat : prevStat;
		}

		/// <summary>Reset the all time min max of the metric</summary>
		public virtual void ResetMinMax()
		{
			minMax.Reset();
		}
	}
}
