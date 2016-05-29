using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public enum ApplicationState
	{
		New,
		Initing,
		Running,
		FinishingContainersWait,
		ApplicationResourcesCleaningup,
		Finished
	}
}
