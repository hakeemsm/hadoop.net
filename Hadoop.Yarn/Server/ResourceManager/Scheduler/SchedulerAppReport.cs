using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// Represents an application attempt, and the resources that the attempt is
	/// using.
	/// </summary>
	public class SchedulerAppReport
	{
		private readonly ICollection<RMContainer> live;

		private readonly ICollection<RMContainer> reserved;

		private readonly bool pending;

		public SchedulerAppReport(SchedulerApplicationAttempt app)
		{
			this.live = app.GetLiveContainers();
			this.reserved = app.GetReservedContainers();
			this.pending = app.IsPending();
		}

		/// <summary>Get the list of live containers</summary>
		/// <returns>All of the live containers</returns>
		public virtual ICollection<RMContainer> GetLiveContainers()
		{
			return live;
		}

		/// <summary>Get the list of reserved containers</summary>
		/// <returns>All of the reserved containers.</returns>
		public virtual ICollection<RMContainer> GetReservedContainers()
		{
			return reserved;
		}

		/// <summary>Is this application pending?</summary>
		/// <returns>true if it is else false.</returns>
		public virtual bool IsPending()
		{
			return pending;
		}
	}
}
