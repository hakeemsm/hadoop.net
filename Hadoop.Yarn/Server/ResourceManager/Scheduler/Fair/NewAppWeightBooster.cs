using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// A
	/// <see cref="WeightAdjuster"/>
	/// implementation that gives a weight boost to new jobs
	/// for a certain amount of time -- by default, a 3x weight boost for 60 seconds.
	/// This can be used to make shorter jobs finish faster, emulating Shortest Job
	/// First scheduling while not starving long jobs.
	/// </summary>
	public class NewAppWeightBooster : Configured, WeightAdjuster
	{
		private const float DefaultFactor = 3;

		private const long DefaultDuration = 5 * 60 * 1000;

		private float factor;

		private long duration;

		public override void SetConf(Configuration conf)
		{
			if (conf != null)
			{
				factor = conf.GetFloat("mapred.newjobweightbooster.factor", DefaultFactor);
				duration = conf.GetLong("mapred.newjobweightbooster.duration", DefaultDuration);
			}
			base.SetConf(conf);
		}

		public virtual double AdjustWeight(FSAppAttempt app, double curWeight)
		{
			long start = app.GetStartTime();
			long now = Runtime.CurrentTimeMillis();
			if (now - start < duration)
			{
				return curWeight * factor;
			}
			else
			{
				return curWeight;
			}
		}
	}
}
