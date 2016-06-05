using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public enum WritingHistoryEventType
	{
		AppStart,
		AppFinish,
		AppAttemptStart,
		AppAttemptFinish,
		ContainerStart,
		ContainerFinish
	}
}
