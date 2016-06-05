using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class ContainerAllocationExpirer : AbstractLivelinessMonitor<ContainerId>
	{
		private EventHandler dispatcher;

		public ContainerAllocationExpirer(Dispatcher d)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.ContainerAllocationExpirer
				).FullName, new SystemClock())
		{
			this.dispatcher = d.GetEventHandler();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			int expireIntvl = conf.GetInt(YarnConfiguration.RmContainerAllocExpiryIntervalMs, 
				YarnConfiguration.DefaultRmContainerAllocExpiryIntervalMs);
			SetExpireInterval(expireIntvl);
			SetMonitorInterval(expireIntvl / 3);
			base.ServiceInit(conf);
		}

		protected override void Expire(ContainerId containerId)
		{
			dispatcher.Handle(new ContainerExpiredSchedulerEvent(containerId));
		}
	}
}
