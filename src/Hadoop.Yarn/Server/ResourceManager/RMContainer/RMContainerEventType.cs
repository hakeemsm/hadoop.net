using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public enum RMContainerEventType
	{
		Start,
		Acquired,
		Kill,
		Reserved,
		Launched,
		Finished,
		Released,
		Expire,
		Recover
	}
}
