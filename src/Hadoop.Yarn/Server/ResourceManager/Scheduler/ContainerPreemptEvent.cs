using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Simple event class used to communicate containers unreservations, preemption, killing
	/// 	</summary>
	public class ContainerPreemptEvent : SchedulerEvent
	{
		private readonly ApplicationAttemptId aid;

		private readonly RMContainer container;

		public ContainerPreemptEvent(ApplicationAttemptId aid, RMContainer container, SchedulerEventType
			 type)
			: base(type)
		{
			this.aid = aid;
			this.container = container;
		}

		public virtual RMContainer GetContainer()
		{
			return this.container;
		}

		public virtual ApplicationAttemptId GetAppId()
		{
			return aid;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder(base.ToString());
			sb.Append(" ").Append(GetAppId());
			sb.Append(" ").Append(GetContainer().GetContainerId());
			return sb.ToString();
		}
	}
}
