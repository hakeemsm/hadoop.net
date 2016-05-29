using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Gson.Stream;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This is a run length encoded sparse data structure that maintains resource
	/// allocations over time
	/// </summary>
	public class RLESparseResourceAllocation
	{
		private const int Threshold = 100;

		private static readonly Resource ZeroResource = Resource.NewInstance(0, 0);

		private SortedDictionary<long, Resource> cumulativeCapacity = new SortedDictionary
			<long, Resource>();

		private readonly ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock
			();

		private readonly Lock readLock = readWriteLock.ReadLock();

		private readonly Lock writeLock = readWriteLock.WriteLock();

		private readonly ResourceCalculator resourceCalculator;

		private readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc;

		public RLESparseResourceAllocation(ResourceCalculator resourceCalculator, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 minAlloc)
		{
			this.resourceCalculator = resourceCalculator;
			this.minAlloc = minAlloc;
		}

		private bool IsSameAsPrevious(long key, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 capacity)
		{
			KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> previous = cumulativeCapacity
				.LowerEntry(key);
			return (previous != null && previous.Value.Equals(capacity));
		}

		private bool IsSameAsNext(long key, Org.Apache.Hadoop.Yarn.Api.Records.Resource capacity
			)
		{
			KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> next = cumulativeCapacity
				.HigherEntry(key);
			return (next != null && next.Value.Equals(capacity));
		}

		/// <summary>Add a resource for the specified interval</summary>
		/// <param name="reservationInterval">
		/// the interval for which the resource is to be
		/// added
		/// </param>
		/// <param name="capacity">the resource to be added</param>
		/// <returns>true if addition is successful, false otherwise</returns>
		public virtual bool AddInterval(ReservationInterval reservationInterval, ReservationRequest
			 capacity)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totCap = Resources.Multiply(capacity.
				GetCapability(), (float)capacity.GetNumContainers());
			if (totCap.Equals(ZeroResource))
			{
				return true;
			}
			writeLock.Lock();
			try
			{
				long startKey = reservationInterval.GetStartTime();
				long endKey = reservationInterval.GetEndTime();
				NavigableMap<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> ticks = cumulativeCapacity
					.HeadMap(endKey, false);
				if (ticks != null && !ticks.IsEmpty())
				{
					Org.Apache.Hadoop.Yarn.Api.Records.Resource updatedCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> lowEntry = ticks.
						FloorEntry(startKey);
					if (lowEntry == null)
					{
						// This is the earliest starting interval
						cumulativeCapacity[startKey] = totCap;
					}
					else
					{
						updatedCapacity = Resources.Add(lowEntry.Value, totCap);
						// Add a new tick only if the updated value is different
						// from the previous tick
						if ((startKey == lowEntry.Key) && (IsSameAsPrevious(lowEntry.Key, updatedCapacity
							)))
						{
							Sharpen.Collections.Remove(cumulativeCapacity, lowEntry.Key);
						}
						else
						{
							cumulativeCapacity[startKey] = updatedCapacity;
						}
					}
					// Increase all the capacities of overlapping intervals
					ICollection<KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource>> overlapSet
						 = ticks.TailMap(startKey, false);
					foreach (KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> entry in 
						overlapSet)
					{
						updatedCapacity = Resources.Add(entry.Value, totCap);
						entry.SetValue(updatedCapacity);
					}
				}
				else
				{
					// This is the first interval to be added
					cumulativeCapacity[startKey] = totCap;
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Resource nextTick = cumulativeCapacity[endKey];
				if (nextTick != null)
				{
					// If there is overlap, remove the duplicate entry
					if (IsSameAsPrevious(endKey, nextTick))
					{
						Sharpen.Collections.Remove(cumulativeCapacity, endKey);
					}
				}
				else
				{
					// Decrease capacity as this is end of the interval
					cumulativeCapacity[endKey] = Resources.Subtract(cumulativeCapacity.FloorEntry(endKey
						).Value, totCap);
				}
				return true;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <summary>Add multiple resources for the specified interval</summary>
		/// <param name="reservationInterval">
		/// the interval for which the resource is to be
		/// added
		/// </param>
		/// <param name="ReservationRequests">the resources to be added</param>
		/// <param name="clusterResource">the total resources in the cluster</param>
		/// <returns>true if addition is successful, false otherwise</returns>
		public virtual bool AddCompositeInterval(ReservationInterval reservationInterval, 
			IList<ReservationRequest> ReservationRequests, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			ReservationRequest aggregateReservationRequest = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<ReservationRequest>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			foreach (ReservationRequest ReservationRequest in ReservationRequests)
			{
				Resources.AddTo(capacity, Resources.Multiply(ReservationRequest.GetCapability(), 
					ReservationRequest.GetNumContainers()));
			}
			aggregateReservationRequest.SetNumContainers((int)Math.Ceil(Resources.Divide(resourceCalculator
				, clusterResource, capacity, minAlloc)));
			aggregateReservationRequest.SetCapability(minAlloc);
			return AddInterval(reservationInterval, aggregateReservationRequest);
		}

		/// <summary>Removes a resource for the specified interval</summary>
		/// <param name="reservationInterval">
		/// the interval for which the resource is to be
		/// removed
		/// </param>
		/// <param name="capacity">the resource to be removed</param>
		/// <returns>true if removal is successful, false otherwise</returns>
		public virtual bool RemoveInterval(ReservationInterval reservationInterval, ReservationRequest
			 capacity)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totCap = Resources.Multiply(capacity.
				GetCapability(), (float)capacity.GetNumContainers());
			if (totCap.Equals(ZeroResource))
			{
				return true;
			}
			writeLock.Lock();
			try
			{
				long startKey = reservationInterval.GetStartTime();
				long endKey = reservationInterval.GetEndTime();
				// update the start key
				NavigableMap<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> ticks = cumulativeCapacity
					.HeadMap(endKey, false);
				// Decrease all the capacities of overlapping intervals
				SortedDictionary<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> overlapSet = 
					ticks.TailMap(startKey);
				if (overlapSet != null && !overlapSet.IsEmpty())
				{
					Org.Apache.Hadoop.Yarn.Api.Records.Resource updatedCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					long currentKey = -1;
					for (IEnumerator<KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource>>
						 overlapEntries = overlapSet.GetEnumerator(); overlapEntries.HasNext(); )
					{
						KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> entry = overlapEntries
							.Next();
						currentKey = entry.Key;
						updatedCapacity = Resources.Subtract(entry.Value, totCap);
						// update each entry between start and end key
						cumulativeCapacity[currentKey] = updatedCapacity;
					}
					// Remove the first overlap entry if it is same as previous after
					// updation
					long firstKey = overlapSet.FirstKey();
					if (IsSameAsPrevious(firstKey, overlapSet[firstKey]))
					{
						Sharpen.Collections.Remove(cumulativeCapacity, firstKey);
					}
					// Remove the next entry if it is same as end entry after updation
					if ((currentKey != -1) && (IsSameAsNext(currentKey, updatedCapacity)))
					{
						Sharpen.Collections.Remove(cumulativeCapacity, cumulativeCapacity.HigherKey(currentKey
							));
					}
				}
				return true;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <summary>Returns the capacity, i.e.</summary>
		/// <remarks>
		/// Returns the capacity, i.e. total resources allocated at the specified point
		/// of time
		/// </remarks>
		/// <param name="tick">the time (UTC in ms) at which the capacity is requested</param>
		/// <returns>the resources allocated at the specified time</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetCapacityAtTime(long
			 tick)
		{
			readLock.Lock();
			try
			{
				KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> closestStep = cumulativeCapacity
					.FloorEntry(tick);
				if (closestStep != null)
				{
					return Resources.Clone(closestStep.Value);
				}
				return Resources.Clone(ZeroResource);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Get the timestamp of the earliest resource allocation</summary>
		/// <returns>the timestamp of the first resource allocation</returns>
		public virtual long GetEarliestStartTime()
		{
			readLock.Lock();
			try
			{
				if (cumulativeCapacity.IsEmpty())
				{
					return -1;
				}
				else
				{
					return cumulativeCapacity.FirstKey();
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Get the timestamp of the latest resource allocation</summary>
		/// <returns>the timestamp of the last resource allocation</returns>
		public virtual long GetLatestEndTime()
		{
			readLock.Lock();
			try
			{
				if (cumulativeCapacity.IsEmpty())
				{
					return -1;
				}
				else
				{
					return cumulativeCapacity.LastKey();
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Returns true if there are no non-zero entries</summary>
		/// <returns>true if there are no allocations or false otherwise</returns>
		public virtual bool IsEmpty()
		{
			readLock.Lock();
			try
			{
				if (cumulativeCapacity.IsEmpty())
				{
					return true;
				}
				// Deletion leaves a single zero entry so check for that
				if (cumulativeCapacity.Count == 1)
				{
					return cumulativeCapacity.FirstEntry().Value.Equals(ZeroResource);
				}
				return false;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public override string ToString()
		{
			StringBuilder ret = new StringBuilder();
			readLock.Lock();
			try
			{
				if (cumulativeCapacity.Count > Threshold)
				{
					ret.Append("Number of steps: ").Append(cumulativeCapacity.Count).Append(" earliest entry: "
						).Append(cumulativeCapacity.FirstKey()).Append(" latest entry: ").Append(cumulativeCapacity
						.LastKey());
				}
				else
				{
					foreach (KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> r in cumulativeCapacity)
					{
						ret.Append(r.Key).Append(": ").Append(r.Value).Append("\n ");
					}
				}
				return ret.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>
		/// Returns the JSON string representation of the current resources allocated
		/// over time
		/// </summary>
		/// <returns>
		/// the JSON string representation of the current resources allocated
		/// over time
		/// </returns>
		public virtual string ToMemJSONString()
		{
			StringWriter json = new StringWriter();
			JsonWriter jsonWriter = new JsonWriter(json);
			readLock.Lock();
			try
			{
				jsonWriter.BeginObject();
				// jsonWriter.name("timestamp").value("resource");
				foreach (KeyValuePair<long, Org.Apache.Hadoop.Yarn.Api.Records.Resource> r in cumulativeCapacity)
				{
					jsonWriter.Name(r.Key.ToString()).Value(r.Value.ToString());
				}
				jsonWriter.EndObject();
				jsonWriter.Close();
				return json.ToString();
			}
			catch (IOException)
			{
				// This should not happen
				return string.Empty;
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
