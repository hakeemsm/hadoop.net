using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	internal class InMemoryPlan : Plan
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.InMemoryPlan
			));

		private static readonly Resource ZeroResource = Resource.NewInstance(0, 0);

		private SortedDictionary<ReservationInterval, ICollection<InMemoryReservationAllocation
			>> currentReservations = new SortedDictionary<ReservationInterval, ICollection<InMemoryReservationAllocation
			>>();

		private RLESparseResourceAllocation rleSparseVector;

		private IDictionary<string, RLESparseResourceAllocation> userResourceAlloc = new 
			Dictionary<string, RLESparseResourceAllocation>();

		private IDictionary<ReservationId, InMemoryReservationAllocation> reservationTable
			 = new Dictionary<ReservationId, InMemoryReservationAllocation>();

		private readonly ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock
			();

		private readonly Lock readLock = readWriteLock.ReadLock();

		private readonly Lock writeLock = readWriteLock.WriteLock();

		private readonly SharingPolicy policy;

		private readonly ReservationAgent agent;

		private readonly long step;

		private readonly ResourceCalculator resCalc;

		private readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc;

		private readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAlloc;

		private readonly string queueName;

		private readonly QueueMetrics queueMetrics;

		private readonly Planner replanner;

		private readonly bool getMoveOnExpiry;

		private readonly Clock clock;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource totalCapacity;

		internal InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy, ReservationAgent
			 agent, Org.Apache.Hadoop.Yarn.Api.Records.Resource totalCapacity, long step, ResourceCalculator
			 resCalc, Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAlloc, string queueName, Planner replanner, bool getMoveOnExpiry)
			: this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc, maxAlloc
				, queueName, replanner, getMoveOnExpiry, new UTCClock())
		{
		}

		internal InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy, ReservationAgent
			 agent, Org.Apache.Hadoop.Yarn.Api.Records.Resource totalCapacity, long step, ResourceCalculator
			 resCalc, Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAlloc, string queueName, Planner replanner, bool getMoveOnExpiry, Clock clock
			)
		{
			this.queueMetrics = queueMetrics;
			this.policy = policy;
			this.agent = agent;
			this.step = step;
			this.totalCapacity = totalCapacity;
			this.resCalc = resCalc;
			this.minAlloc = minAlloc;
			this.maxAlloc = maxAlloc;
			this.rleSparseVector = new RLESparseResourceAllocation(resCalc, minAlloc);
			this.queueName = queueName;
			this.replanner = replanner;
			this.getMoveOnExpiry = getMoveOnExpiry;
			this.clock = clock;
		}

		public virtual QueueMetrics GetQueueMetrics()
		{
			return queueMetrics;
		}

		private void IncrementAllocation(ReservationAllocation reservation)
		{
			System.Diagnostics.Debug.Assert((readWriteLock.IsWriteLockedByCurrentThread()));
			IDictionary<ReservationInterval, ReservationRequest> allocationRequests = reservation
				.GetAllocationRequests();
			// check if we have encountered the user earlier and if not add an entry
			string user = reservation.GetUser();
			RLESparseResourceAllocation resAlloc = userResourceAlloc[user];
			if (resAlloc == null)
			{
				resAlloc = new RLESparseResourceAllocation(resCalc, minAlloc);
				userResourceAlloc[user] = resAlloc;
			}
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> r in allocationRequests)
			{
				resAlloc.AddInterval(r.Key, r.Value);
				rleSparseVector.AddInterval(r.Key, r.Value);
			}
		}

		private void DecrementAllocation(ReservationAllocation reservation)
		{
			System.Diagnostics.Debug.Assert((readWriteLock.IsWriteLockedByCurrentThread()));
			IDictionary<ReservationInterval, ReservationRequest> allocationRequests = reservation
				.GetAllocationRequests();
			string user = reservation.GetUser();
			RLESparseResourceAllocation resAlloc = userResourceAlloc[user];
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> r in allocationRequests)
			{
				resAlloc.RemoveInterval(r.Key, r.Value);
				rleSparseVector.RemoveInterval(r.Key, r.Value);
			}
			if (resAlloc.IsEmpty())
			{
				Sharpen.Collections.Remove(userResourceAlloc, user);
			}
		}

		public virtual ICollection<ReservationAllocation> GetAllReservations()
		{
			readLock.Lock();
			try
			{
				if (currentReservations != null)
				{
					ICollection<ReservationAllocation> flattenedReservations = new HashSet<ReservationAllocation
						>();
					foreach (ICollection<InMemoryReservationAllocation> reservationEntries in currentReservations
						.Values)
					{
						Sharpen.Collections.AddAll(flattenedReservations, reservationEntries);
					}
					return flattenedReservations;
				}
				else
				{
					return null;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual bool AddReservation(ReservationAllocation reservation)
		{
			// Verify the allocation is memory based otherwise it is not supported
			InMemoryReservationAllocation inMemReservation = (InMemoryReservationAllocation)reservation;
			if (inMemReservation.GetUser() == null)
			{
				string errMsg = "The specified Reservation with ID " + inMemReservation.GetReservationId
					() + " is not mapped to any user";
				Log.Error(errMsg);
				throw new ArgumentException(errMsg);
			}
			writeLock.Lock();
			try
			{
				if (reservationTable.Contains(inMemReservation.GetReservationId()))
				{
					string errMsg = "The specified Reservation with ID " + inMemReservation.GetReservationId
						() + " already exists";
					Log.Error(errMsg);
					throw new ArgumentException(errMsg);
				}
				// Validate if we can accept this reservation, throws exception if
				// validation fails
				policy.Validate(this, inMemReservation);
				// we record here the time in which the allocation has been accepted
				reservation.SetAcceptanceTimestamp(clock.GetTime());
				ReservationInterval searchInterval = new ReservationInterval(inMemReservation.GetStartTime
					(), inMemReservation.GetEndTime());
				ICollection<InMemoryReservationAllocation> reservations = currentReservations[searchInterval
					];
				if (reservations == null)
				{
					reservations = new HashSet<InMemoryReservationAllocation>();
				}
				if (!reservations.AddItem(inMemReservation))
				{
					Log.Error("Unable to add reservation: {} to plan.", inMemReservation.GetReservationId
						());
					return false;
				}
				currentReservations[searchInterval] = reservations;
				reservationTable[inMemReservation.GetReservationId()] = inMemReservation;
				IncrementAllocation(inMemReservation);
				Log.Info("Sucessfully added reservation: {} to plan.", inMemReservation.GetReservationId
					());
				return true;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual bool UpdateReservation(ReservationAllocation reservation)
		{
			writeLock.Lock();
			bool result = false;
			try
			{
				ReservationId resId = reservation.GetReservationId();
				ReservationAllocation currReservation = GetReservationById(resId);
				if (currReservation == null)
				{
					string errMsg = "The specified Reservation with ID " + resId + " does not exist in the plan";
					Log.Error(errMsg);
					throw new ArgumentException(errMsg);
				}
				// validate if we can accept this reservation, throws exception if
				// validation fails
				policy.Validate(this, reservation);
				if (!RemoveReservation(currReservation))
				{
					Log.Error("Unable to replace reservation: {} from plan.", reservation.GetReservationId
						());
					return result;
				}
				try
				{
					result = AddReservation(reservation);
				}
				catch (PlanningException e)
				{
					Log.Error("Unable to update reservation: {} from plan due to {}.", reservation.GetReservationId
						(), e.Message);
				}
				if (result)
				{
					Log.Info("Sucessfully updated reservation: {} in plan.", reservation.GetReservationId
						());
					return result;
				}
				else
				{
					// rollback delete
					AddReservation(currReservation);
					Log.Info("Rollbacked update reservation: {} from plan.", reservation.GetReservationId
						());
					return result;
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private bool RemoveReservation(ReservationAllocation reservation)
		{
			System.Diagnostics.Debug.Assert((readWriteLock.IsWriteLockedByCurrentThread()));
			ReservationInterval searchInterval = new ReservationInterval(reservation.GetStartTime
				(), reservation.GetEndTime());
			ICollection<InMemoryReservationAllocation> reservations = currentReservations[searchInterval
				];
			if (reservations != null)
			{
				if (!reservations.Remove(reservation))
				{
					Log.Error("Unable to remove reservation: {} from plan.", reservation.GetReservationId
						());
					return false;
				}
				if (reservations.IsEmpty())
				{
					Sharpen.Collections.Remove(currentReservations, searchInterval);
				}
			}
			else
			{
				string errMsg = "The specified Reservation with ID " + reservation.GetReservationId
					() + " does not exist in the plan";
				Log.Error(errMsg);
				throw new ArgumentException(errMsg);
			}
			Sharpen.Collections.Remove(reservationTable, reservation.GetReservationId());
			DecrementAllocation(reservation);
			Log.Info("Sucessfully deleted reservation: {} in plan.", reservation.GetReservationId
				());
			return true;
		}

		public virtual bool DeleteReservation(ReservationId reservationID)
		{
			writeLock.Lock();
			try
			{
				ReservationAllocation reservation = GetReservationById(reservationID);
				if (reservation == null)
				{
					string errMsg = "The specified Reservation with ID " + reservationID + " does not exist in the plan";
					Log.Error(errMsg);
					throw new ArgumentException(errMsg);
				}
				return RemoveReservation(reservation);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual void ArchiveCompletedReservations(long tick)
		{
			// Since we are looking for old reservations, read lock is optimal
			Log.Debug("Running archival at time: {}", tick);
			IList<InMemoryReservationAllocation> expiredReservations = new AList<InMemoryReservationAllocation
				>();
			readLock.Lock();
			// archive reservations and delete the ones which are beyond
			// the reservation policy "window"
			try
			{
				long archivalTime = tick - policy.GetValidWindow();
				ReservationInterval searchInterval = new ReservationInterval(archivalTime, archivalTime
					);
				SortedDictionary<ReservationInterval, ICollection<InMemoryReservationAllocation>>
					 reservations = currentReservations.HeadMap(searchInterval, true);
				if (!reservations.IsEmpty())
				{
					foreach (ICollection<InMemoryReservationAllocation> reservationEntries in reservations
						.Values)
					{
						foreach (InMemoryReservationAllocation reservation in reservationEntries)
						{
							if (reservation.GetEndTime() <= archivalTime)
							{
								expiredReservations.AddItem(reservation);
							}
						}
					}
				}
			}
			finally
			{
				readLock.Unlock();
			}
			if (expiredReservations.IsEmpty())
			{
				return;
			}
			// Need write lock only if there are any reservations to be deleted
			writeLock.Lock();
			try
			{
				foreach (InMemoryReservationAllocation expiredReservation in expiredReservations)
				{
					RemoveReservation(expiredReservation);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual ICollection<ReservationAllocation> GetReservationsAtTime(long tick
			)
		{
			ReservationInterval searchInterval = new ReservationInterval(tick, long.MaxValue);
			readLock.Lock();
			try
			{
				SortedDictionary<ReservationInterval, ICollection<InMemoryReservationAllocation>>
					 reservations = currentReservations.HeadMap(searchInterval, true);
				if (!reservations.IsEmpty())
				{
					ICollection<ReservationAllocation> flattenedReservations = new HashSet<ReservationAllocation
						>();
					foreach (ICollection<InMemoryReservationAllocation> reservationEntries in reservations
						.Values)
					{
						foreach (InMemoryReservationAllocation reservation in reservationEntries)
						{
							if (reservation.GetEndTime() > tick)
							{
								flattenedReservations.AddItem(reservation);
							}
						}
					}
					return Sharpen.Collections.UnmodifiableSet(flattenedReservations);
				}
				else
				{
					return Sharpen.Collections.EmptySet();
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetStep()
		{
			return step;
		}

		public virtual SharingPolicy GetSharingPolicy()
		{
			return policy;
		}

		public virtual ReservationAgent GetReservationAgent()
		{
			return agent;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetConsumptionForUser(
			string user, long t)
		{
			readLock.Lock();
			try
			{
				RLESparseResourceAllocation userResAlloc = userResourceAlloc[user];
				if (userResAlloc != null)
				{
					return userResAlloc.GetCapacityAtTime(t);
				}
				else
				{
					return Resources.Clone(ZeroResource);
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetTotalCommittedResources
			(long t)
		{
			readLock.Lock();
			try
			{
				return rleSparseVector.GetCapacityAtTime(t);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual ReservationAllocation GetReservationById(ReservationId reservationID
			)
		{
			if (reservationID == null)
			{
				return null;
			}
			readLock.Lock();
			try
			{
				return reservationTable[reservationID];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetTotalCapacity()
		{
			readLock.Lock();
			try
			{
				return Resources.Clone(totalCapacity);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinimumAllocation()
		{
			return Resources.Clone(minAlloc);
		}

		public virtual void SetTotalCapacity(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			cap)
		{
			writeLock.Lock();
			try
			{
				totalCapacity = Resources.Clone(cap);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual long GetEarliestStartTime()
		{
			readLock.Lock();
			try
			{
				return rleSparseVector.GetEarliestStartTime();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetLastEndTime()
		{
			readLock.Lock();
			try
			{
				return rleSparseVector.GetLatestEndTime();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual ResourceCalculator GetResourceCalculator()
		{
			return resCalc;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocation()
		{
			return Resources.Clone(maxAlloc);
		}

		public virtual string ToCumulativeString()
		{
			readLock.Lock();
			try
			{
				return rleSparseVector.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Planner GetReplanner()
		{
			return replanner;
		}

		public virtual bool GetMoveOnExpiry()
		{
			return getMoveOnExpiry;
		}

		public override string ToString()
		{
			readLock.Lock();
			try
			{
				StringBuilder planStr = new StringBuilder("In-memory Plan: ");
				planStr.Append("Parent Queue: ").Append(queueName).Append("Total Capacity: ").Append
					(totalCapacity).Append("Step: ").Append(step);
				foreach (ReservationAllocation reservation in GetAllReservations())
				{
					planStr.Append(reservation);
				}
				return planStr.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
