using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>This represents the time duration of the reservation</summary>
	public class ReservationInterval : Comparable<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationInterval
		>
	{
		private readonly long startTime;

		private readonly long endTime;

		public ReservationInterval(long startTime, long endTime)
		{
			this.startTime = startTime;
			this.endTime = endTime;
		}

		/// <summary>Get the start time of the reservation interval</summary>
		/// <returns>the startTime</returns>
		public virtual long GetStartTime()
		{
			return startTime;
		}

		/// <summary>Get the end time of the reservation interval</summary>
		/// <returns>the endTime</returns>
		public virtual long GetEndTime()
		{
			return endTime;
		}

		/// <summary>Returns whether the interval is active at the specified instant of time</summary>
		/// <param name="tick">the instance of the time to check</param>
		/// <returns>true if active, false otherwise</returns>
		public virtual bool IsOverlap(long tick)
		{
			return (startTime <= tick && tick <= endTime);
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationInterval
			 anotherInterval)
		{
			long diff = 0;
			if (startTime == anotherInterval.GetStartTime())
			{
				diff = endTime - anotherInterval.GetEndTime();
			}
			else
			{
				diff = startTime - anotherInterval.GetStartTime();
			}
			if (diff < 0)
			{
				return -1;
			}
			else
			{
				if (diff > 0)
				{
					return 1;
				}
				else
				{
					return 0;
				}
			}
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + (int)(endTime ^ ((long)(((ulong)endTime) >> 32)));
			result = prime * result + (int)(startTime ^ ((long)(((ulong)startTime) >> 32)));
			return result;
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
			if (!(obj is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationInterval
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationInterval other
				 = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationInterval
				)obj;
			if (endTime != other.endTime)
			{
				return false;
			}
			if (startTime != other.startTime)
			{
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			return "[" + startTime + ", " + endTime + "]";
		}
	}
}
