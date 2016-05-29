using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public enum RMContainerState
	{
		New,
		Reserved,
		Allocated,
		Acquired,
		Running,
		Completed,
		Expired,
		Released,
		Killed
	}
}
