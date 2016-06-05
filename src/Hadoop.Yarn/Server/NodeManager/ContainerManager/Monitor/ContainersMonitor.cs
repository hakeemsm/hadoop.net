using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public interface ContainersMonitor : Org.Apache.Hadoop.Service.Service, EventHandler
		<ContainersMonitorEvent>, ResourceView
	{
	}
}
