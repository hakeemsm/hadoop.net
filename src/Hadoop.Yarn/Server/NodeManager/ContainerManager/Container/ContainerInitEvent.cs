using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerInitEvent : ContainerEvent
	{
		public ContainerInitEvent(ContainerId c)
			: base(c, ContainerEventType.InitContainer)
		{
		}
	}
}
