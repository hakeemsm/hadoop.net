using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// An in memory implementation of a reservation allocation using the
	/// <see cref="RLESparseResourceAllocation"/>
	/// </summary>
	internal class InMemoryReservationAllocation : ReservationAllocation
	{
		private readonly string planName;

		private readonly ReservationId reservationID;

		private readonly string user;

		private readonly ReservationDefinition contract;

		private readonly long startTime;

		private readonly long endTime;

		private readonly IDictionary<ReservationInterval, ReservationRequest> allocationRequests;

		private bool hasGang = false;

		private long acceptedAt = -1;

		private RLESparseResourceAllocation resourcesOverTime;

		internal InMemoryReservationAllocation(ReservationId reservationID, ReservationDefinition
			 contract, string user, string planName, long startTime, long endTime, IDictionary
			<ReservationInterval, ReservationRequest> allocationRequests, ResourceCalculator
			 calculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc)
		{
			this.contract = contract;
			this.startTime = startTime;
			this.endTime = endTime;
			this.reservationID = reservationID;
			this.user = user;
			this.allocationRequests = allocationRequests;
			this.planName = planName;
			resourcesOverTime = new RLESparseResourceAllocation(calculator, minAlloc);
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> r in allocationRequests)
			{
				resourcesOverTime.AddInterval(r.Key, r.Value);
				if (r.Value.GetConcurrency() > 1)
				{
					hasGang = true;
				}
			}
		}

		public virtual ReservationId GetReservationId()
		{
			return reservationID;
		}

		public virtual ReservationDefinition GetReservationDefinition()
		{
			return contract;
		}

		public virtual long GetStartTime()
		{
			return startTime;
		}

		public virtual long GetEndTime()
		{
			return endTime;
		}

		public virtual IDictionary<ReservationInterval, ReservationRequest> GetAllocationRequests
			()
		{
			return Sharpen.Collections.UnmodifiableMap(allocationRequests);
		}

		public virtual string GetPlanName()
		{
			return planName;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual bool ContainsGangs()
		{
			return hasGang;
		}

		public virtual void SetAcceptanceTimestamp(long acceptedAt)
		{
			this.acceptedAt = acceptedAt;
		}

		public virtual long GetAcceptanceTime()
		{
			return acceptedAt;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourcesAtTime(long
			 tick)
		{
			if (tick < startTime || tick >= endTime)
			{
				return Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);
			}
			return Resources.Clone(resourcesOverTime.GetCapacityAtTime(tick));
		}

		public override string ToString()
		{
			StringBuilder sBuf = new StringBuilder();
			sBuf.Append(GetReservationId()).Append(" user:").Append(GetUser()).Append(" startTime: "
				).Append(GetStartTime()).Append(" endTime: ").Append(GetEndTime()).Append(" alloc:["
				).Append(resourcesOverTime.ToString()).Append("] ");
			return sBuf.ToString();
		}

		public virtual int CompareTo(ReservationAllocation other)
		{
			// reverse order of acceptance
			if (this.GetAcceptanceTime() > other.GetAcceptanceTime())
			{
				return -1;
			}
			if (this.GetAcceptanceTime() < other.GetAcceptanceTime())
			{
				return 1;
			}
			return 0;
		}

		public override int GetHashCode()
		{
			return reservationID.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.InMemoryReservationAllocation
				 other = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.InMemoryReservationAllocation
				)obj;
			return this.reservationID.Equals(other.GetReservationId());
		}
	}
}
