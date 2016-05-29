using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public enum LocalizationEventType
	{
		InitApplicationResources,
		InitContainerResources,
		CacheCleanup,
		CleanupContainerResources,
		DestroyApplicationResources,
		ContainerResourcesLocalized
	}
}
