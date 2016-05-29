using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class AMLivelinessMonitor : AbstractLivelinessMonitor<ApplicationAttemptId
		>
	{
		private EventHandler dispatcher;

		public AMLivelinessMonitor(Dispatcher d)
			: base("AMLivelinessMonitor", new SystemClock())
		{
			this.dispatcher = d.GetEventHandler();
		}

		public AMLivelinessMonitor(Dispatcher d, Clock clock)
			: base("AMLivelinessMonitor", clock)
		{
			this.dispatcher = d.GetEventHandler();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			int expireIntvl = conf.GetInt(YarnConfiguration.RmAmExpiryIntervalMs, YarnConfiguration
				.DefaultRmAmExpiryIntervalMs);
			SetExpireInterval(expireIntvl);
			SetMonitorInterval(expireIntvl / 3);
		}

		protected override void Expire(ApplicationAttemptId id)
		{
			dispatcher.Handle(new RMAppAttemptEvent(id, RMAppAttemptEventType.Expire));
		}
	}
}
