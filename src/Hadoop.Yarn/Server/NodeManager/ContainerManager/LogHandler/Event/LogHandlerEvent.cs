using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event
{
	public class LogHandlerEvent : AbstractEvent<LogHandlerEventType>
	{
		public LogHandlerEvent(LogHandlerEventType type)
			: base(type)
		{
		}
	}
}
