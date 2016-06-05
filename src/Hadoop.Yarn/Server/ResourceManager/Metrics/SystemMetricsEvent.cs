using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class SystemMetricsEvent : AbstractEvent<SystemMetricsEventType>
	{
		public SystemMetricsEvent(SystemMetricsEventType type)
			: base(type)
		{
		}

		public SystemMetricsEvent(SystemMetricsEventType type, long timestamp)
			: base(type, timestamp)
		{
		}
	}
}
