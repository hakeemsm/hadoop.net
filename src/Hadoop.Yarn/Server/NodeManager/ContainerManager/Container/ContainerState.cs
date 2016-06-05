using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public enum ContainerState
	{
		New,
		Localizing,
		LocalizationFailed,
		Localized,
		Running,
		ExitedWithSuccess,
		ExitedWithFailure,
		Killing,
		ContainerCleanedupAfterKill,
		ContainerResourcesCleaningup,
		Done
	}
}
