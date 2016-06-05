using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class NMLivelinessMonitor : AbstractLivelinessMonitor<NodeId>
	{
		private EventHandler dispatcher;

		public NMLivelinessMonitor(Dispatcher d)
			: base("NMLivelinessMonitor", new SystemClock())
		{
			this.dispatcher = d.GetEventHandler();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			int expireIntvl = conf.GetInt(YarnConfiguration.RmNmExpiryIntervalMs, YarnConfiguration
				.DefaultRmNmExpiryIntervalMs);
			SetExpireInterval(expireIntvl);
			SetMonitorInterval(expireIntvl / 3);
			base.ServiceInit(conf);
		}

		protected override void Expire(NodeId id)
		{
			dispatcher.Handle(new RMNodeEvent(id, RMNodeEventType.Expire));
		}
	}
}
