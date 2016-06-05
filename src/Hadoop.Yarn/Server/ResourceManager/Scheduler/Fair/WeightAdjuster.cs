using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// A pluggable object for altering the weights of apps in the fair scheduler,
	/// which is used for example by
	/// <see cref="NewAppWeightBooster"/>
	/// to give higher
	/// weight to new jobs so that short jobs finish faster.
	/// May implement
	/// <see cref="Org.Apache.Hadoop.Conf.Configurable"/>
	/// to access configuration parameters.
	/// </summary>
	public interface WeightAdjuster
	{
		double AdjustWeight(FSAppAttempt app, double curWeight);
	}
}
