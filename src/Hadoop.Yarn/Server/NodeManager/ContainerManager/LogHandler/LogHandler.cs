using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler
{
	public interface LogHandler : EventHandler<LogHandlerEvent>
	{
		void Handle(LogHandlerEvent @event);
	}
}
