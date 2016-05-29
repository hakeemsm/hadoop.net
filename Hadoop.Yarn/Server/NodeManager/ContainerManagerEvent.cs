using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class ContainerManagerEvent : AbstractEvent<ContainerManagerEventType>
	{
		public ContainerManagerEvent(ContainerManagerEventType type)
			: base(type)
		{
		}
	}
}
