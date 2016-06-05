using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class AppAddedSchedulerEvent : SchedulerEvent
	{
		private readonly ApplicationId applicationId;

		private readonly string queue;

		private readonly string user;

		private readonly ReservationId reservationID;

		private readonly bool isAppRecovering;

		public AppAddedSchedulerEvent(ApplicationId applicationId, string queue, string user
			)
			: this(applicationId, queue, user, false, null)
		{
		}

		public AppAddedSchedulerEvent(ApplicationId applicationId, string queue, string user
			, ReservationId reservationID)
			: this(applicationId, queue, user, false, reservationID)
		{
		}

		public AppAddedSchedulerEvent(ApplicationId applicationId, string queue, string user
			, bool isAppRecovering, ReservationId reservationID)
			: base(SchedulerEventType.AppAdded)
		{
			this.applicationId = applicationId;
			this.queue = queue;
			this.user = user;
			this.reservationID = reservationID;
			this.isAppRecovering = isAppRecovering;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return applicationId;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual bool GetIsAppRecovering()
		{
			return isAppRecovering;
		}

		public virtual ReservationId GetReservationID()
		{
			return reservationID;
		}
	}
}
