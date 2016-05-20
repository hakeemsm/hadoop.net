using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>A mutable metric with stats.</summary>
	/// <remarks>
	/// A mutable metric with stats.
	/// Useful for keeping throughput/latency stats.
	/// </remarks>
	public class MutableStat : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		private readonly org.apache.hadoop.metrics2.MetricsInfo numInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo avgInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo stdevInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo iMinInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo iMaxInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo minInfo;

		private readonly org.apache.hadoop.metrics2.MetricsInfo maxInfo;

		private readonly org.apache.hadoop.metrics2.util.SampleStat intervalStat = new org.apache.hadoop.metrics2.util.SampleStat
			();

		private readonly org.apache.hadoop.metrics2.util.SampleStat prevStat = new org.apache.hadoop.metrics2.util.SampleStat
			();

		private readonly org.apache.hadoop.metrics2.util.SampleStat.MinMax minMax = new org.apache.hadoop.metrics2.util.SampleStat.MinMax
			();

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
			string ucName = org.apache.commons.lang.StringUtils.capitalize(name);
			string usName = org.apache.commons.lang.StringUtils.capitalize(sampleName);
			string uvName = org.apache.commons.lang.StringUtils.capitalize(valueName);
			string desc = org.apache.commons.lang.StringUtils.uncapitalize(description);
			string lsName = org.apache.commons.lang.StringUtils.uncapitalize(sampleName);
			string lvName = org.apache.commons.lang.StringUtils.uncapitalize(valueName);
			numInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Num" + usName, "Number of "
				 + lsName + " for " + desc);
			avgInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Avg" + uvName, "Average "
				 + lvName + " for " + desc);
			stdevInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Stdev" + uvName
				, "Standard deviation of " + lvName + " for " + desc);
			iMinInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "IMin" + uvName, 
				"Interval min " + lvName + " for " + desc);
			iMaxInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "IMax" + uvName, 
				"Interval max " + lvName + " for " + desc);
			minInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Min" + uvName, "Min "
				 + lvName + " for " + desc);
			maxInfo = org.apache.hadoop.metrics2.lib.Interns.info(ucName + "Max" + uvName, "Max "
				 + lvName + " for " + desc);
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
		public virtual void setExtended(bool extended)
		{
			lock (this)
			{
				this.extended = extended;
			}
		}

		/// <summary>Add a number of samples and their sum to the running stat</summary>
		/// <param name="numSamples">number of samples</param>
		/// <param name="sum">of the samples</param>
		public virtual void add(long numSamples, long sum)
		{
			lock (this)
			{
				intervalStat.add(numSamples, sum);
				setChanged();
			}
		}

		/// <summary>Add a snapshot to the metric</summary>
		/// <param name="value">of the metric</param>
		public virtual void add(long value)
		{
			lock (this)
			{
				intervalStat.add(value);
				minMax.add(value);
				setChanged();
			}
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			lock (this)
			{
				if (all || changed())
				{
					numSamples += intervalStat.numSamples();
					builder.addCounter(numInfo, numSamples).addGauge(avgInfo, lastStat().mean());
					if (extended)
					{
						builder.addGauge(stdevInfo, lastStat().stddev()).addGauge(iMinInfo, lastStat().min
							()).addGauge(iMaxInfo, lastStat().max()).addGauge(minInfo, minMax.min()).addGauge
							(maxInfo, minMax.max());
					}
					if (changed())
					{
						if (numSamples > 0)
						{
							intervalStat.copyTo(prevStat);
							intervalStat.reset();
						}
						clearChanged();
					}
				}
			}
		}

		private org.apache.hadoop.metrics2.util.SampleStat lastStat()
		{
			return changed() ? intervalStat : prevStat;
		}

		/// <summary>Reset the all time min max of the metric</summary>
		public virtual void resetMinMax()
		{
			minMax.reset();
		}
	}
}
