using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>Balancing policy.</summary>
	/// <remarks>
	/// Balancing policy.
	/// Since a datanode may contain multiple block pools,
	/// <see cref="Pool"/>
	/// implies
	/// <see cref="Node"/>
	/// but NOT the other way around
	/// </remarks>
	internal abstract class BalancingPolicy
	{
		internal readonly EnumCounters<StorageType> totalCapacities = new EnumCounters<StorageType
			>(typeof(StorageType));

		internal readonly EnumCounters<StorageType> totalUsedSpaces = new EnumCounters<StorageType
			>(typeof(StorageType));

		internal readonly EnumDoubles<StorageType> avgUtilizations = new EnumDoubles<StorageType
			>(typeof(StorageType));

		internal virtual void Reset()
		{
			totalCapacities.Reset();
			totalUsedSpaces.Reset();
			avgUtilizations.Reset();
		}

		/// <summary>Get the policy name.</summary>
		internal abstract string GetName();

		/// <summary>Accumulate used space and capacity.</summary>
		internal abstract void AccumulateSpaces(DatanodeStorageReport r);

		internal virtual void InitAvgUtilization()
		{
			foreach (StorageType t in StorageType.AsList())
			{
				long capacity = totalCapacities.Get(t);
				if (capacity > 0L)
				{
					double avg = totalUsedSpaces.Get(t) * 100.0 / capacity;
					avgUtilizations.Set(t, avg);
				}
			}
		}

		internal virtual double GetAvgUtilization(StorageType t)
		{
			return avgUtilizations.Get(t);
		}

		/// <returns>
		/// the utilization of a particular storage type of a datanode;
		/// or return null if the datanode does not have such storage type.
		/// </returns>
		internal abstract double GetUtilization(DatanodeStorageReport r, StorageType t);

		public override string ToString()
		{
			return typeof(BalancingPolicy).Name + "." + GetType().Name;
		}

		/// <summary>
		/// Get all
		/// <see cref="BalancingPolicy"/>
		/// instances
		/// </summary>
		internal static BalancingPolicy Parse(string s)
		{
			BalancingPolicy[] all = new BalancingPolicy[] { BalancingPolicy.Node.Instance, BalancingPolicy.Pool
				.Instance };
			foreach (BalancingPolicy p in all)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(p.GetName(), s))
				{
					return p;
				}
			}
			throw new ArgumentException("Cannot parse string \"" + s + "\"");
		}

		/// <summary>Cluster is balanced if each node is balanced.</summary>
		internal class Node : BalancingPolicy
		{
			internal static readonly BalancingPolicy.Node Instance = new BalancingPolicy.Node
				();

			private Node()
			{
			}

			internal override string GetName()
			{
				return "datanode";
			}

			internal override void AccumulateSpaces(DatanodeStorageReport r)
			{
				foreach (StorageReport s in r.GetStorageReports())
				{
					StorageType t = s.GetStorage().GetStorageType();
					totalCapacities.Add(t, s.GetCapacity());
					totalUsedSpaces.Add(t, s.GetDfsUsed());
				}
			}

			internal override double GetUtilization(DatanodeStorageReport r, StorageType t)
			{
				long capacity = 0L;
				long dfsUsed = 0L;
				foreach (StorageReport s in r.GetStorageReports())
				{
					if (s.GetStorage().GetStorageType() == t)
					{
						capacity += s.GetCapacity();
						dfsUsed += s.GetDfsUsed();
					}
				}
				return capacity == 0L ? null : dfsUsed * 100.0 / capacity;
			}
		}

		/// <summary>Cluster is balanced if each pool in each node is balanced.</summary>
		internal class Pool : BalancingPolicy
		{
			internal static readonly BalancingPolicy.Pool Instance = new BalancingPolicy.Pool
				();

			private Pool()
			{
			}

			internal override string GetName()
			{
				return "blockpool";
			}

			internal override void AccumulateSpaces(DatanodeStorageReport r)
			{
				foreach (StorageReport s in r.GetStorageReports())
				{
					StorageType t = s.GetStorage().GetStorageType();
					totalCapacities.Add(t, s.GetCapacity());
					totalUsedSpaces.Add(t, s.GetBlockPoolUsed());
				}
			}

			internal override double GetUtilization(DatanodeStorageReport r, StorageType t)
			{
				long capacity = 0L;
				long blockPoolUsed = 0L;
				foreach (StorageReport s in r.GetStorageReports())
				{
					if (s.GetStorage().GetStorageType() == t)
					{
						capacity += s.GetCapacity();
						blockPoolUsed += s.GetBlockPoolUsed();
					}
				}
				return capacity == 0L ? null : blockPoolUsed * 100.0 / capacity;
			}
		}
	}
}
