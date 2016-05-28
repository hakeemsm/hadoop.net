using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	public abstract class ContainerLauncher : EventHandler<ContainerLauncherEvent>
	{
		public enum EventType
		{
			ContainerRemoteLaunch,
			ContainerRemoteCleanup
		}
	}

	public static class ContainerLauncherConstants
	{
	}
}
