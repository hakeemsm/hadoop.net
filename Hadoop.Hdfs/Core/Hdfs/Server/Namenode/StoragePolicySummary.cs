using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Aggregate the storage type information for a set of blocks</summary>
	public class StoragePolicySummary
	{
		internal IDictionary<StoragePolicySummary.StorageTypeAllocation, long> storageComboCounts
			 = new Dictionary<StoragePolicySummary.StorageTypeAllocation, long>();

		internal readonly BlockStoragePolicy[] storagePolicies;

		internal int totalBlocks;

		internal StoragePolicySummary(BlockStoragePolicy[] storagePolicies)
		{
			this.storagePolicies = storagePolicies;
		}

		// Add a storage type combination
		internal virtual void Add(StorageType[] storageTypes, BlockStoragePolicy policy)
		{
			StoragePolicySummary.StorageTypeAllocation storageCombo = new StoragePolicySummary.StorageTypeAllocation
				(storageTypes, policy);
			long count = storageComboCounts[storageCombo];
			if (count == null)
			{
				storageComboCounts[storageCombo] = 1l;
				storageCombo.SetActualStoragePolicy(GetStoragePolicy(storageCombo.GetStorageTypes
					()));
			}
			else
			{
				storageComboCounts[storageCombo] = count + 1;
			}
			totalBlocks++;
		}

		// sort the storageType combinations based on the total blocks counts
		// in descending order
		internal static IList<KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long
			>> SortByComparator(IDictionary<StoragePolicySummary.StorageTypeAllocation, long
			> unsortMap)
		{
			IList<KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long>> storageAllocations
				 = new List<KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long>>(unsortMap
				);
			// Sorting the list based on values
			storageAllocations.Sort(new _IComparer_74());
			return storageAllocations;
		}

		private sealed class _IComparer_74 : IComparer<KeyValuePair<StoragePolicySummary.StorageTypeAllocation
			, long>>
		{
			public _IComparer_74()
			{
			}

			public int Compare(KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long>
				 o1, KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long> o2)
			{
				return o2.Value.CompareTo(o1.Value);
			}
		}

		public override string ToString()
		{
			StringBuilder compliantBlocksSB = new StringBuilder();
			compliantBlocksSB.Append("\nBlocks satisfying the specified storage policy:");
			compliantBlocksSB.Append("\nStorage Policy                  # of blocks       % of blocks\n"
				);
			StringBuilder nonCompliantBlocksSB = new StringBuilder();
			Formatter compliantFormatter = new Formatter(compliantBlocksSB);
			Formatter nonCompliantFormatter = new Formatter(nonCompliantBlocksSB);
			NumberFormat percentFormat = NumberFormat.GetPercentInstance();
			percentFormat.SetMinimumFractionDigits(4);
			percentFormat.SetMaximumFractionDigits(4);
			foreach (KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long> storageComboCount
				 in SortByComparator(storageComboCounts))
			{
				double percent = (double)storageComboCount.Value / (double)totalBlocks;
				StoragePolicySummary.StorageTypeAllocation sta = storageComboCount.Key;
				if (sta.PolicyMatches())
				{
					compliantFormatter.Format("%-25s %10d  %20s%n", sta.GetStoragePolicyDescriptor(), 
						storageComboCount.Value, percentFormat.Format(percent));
				}
				else
				{
					if (nonCompliantBlocksSB.Length == 0)
					{
						nonCompliantBlocksSB.Append("\nBlocks NOT satisfying the specified storage policy:"
							);
						nonCompliantBlocksSB.Append("\nStorage Policy                  ");
						nonCompliantBlocksSB.Append("Specified Storage Policy      # of blocks       % of blocks\n"
							);
					}
					nonCompliantFormatter.Format("%-35s %-20s %10d  %20s%n", sta.GetStoragePolicyDescriptor
						(), sta.GetSpecifiedStoragePolicy().GetName(), storageComboCount.Value, percentFormat
						.Format(percent));
				}
			}
			if (nonCompliantBlocksSB.Length == 0)
			{
				nonCompliantBlocksSB.Append("\nAll blocks satisfy specified storage policy.\n");
			}
			compliantFormatter.Close();
			nonCompliantFormatter.Close();
			return compliantBlocksSB.ToString() + nonCompliantBlocksSB;
		}

		/// <param name="storageTypes">- sorted array of storageTypes</param>
		/// <returns>Storage Policy which matches the specific storage Combination</returns>
		private BlockStoragePolicy GetStoragePolicy(StorageType[] storageTypes)
		{
			foreach (BlockStoragePolicy storagePolicy in storagePolicies)
			{
				StorageType[] policyStorageTypes = storagePolicy.GetStorageTypes();
				policyStorageTypes = Arrays.CopyOf(policyStorageTypes, policyStorageTypes.Length);
				Arrays.Sort(policyStorageTypes);
				if (policyStorageTypes.Length <= storageTypes.Length)
				{
					int i = 0;
					for (; i < policyStorageTypes.Length; i++)
					{
						if (policyStorageTypes[i] != storageTypes[i])
						{
							break;
						}
					}
					if (i < policyStorageTypes.Length)
					{
						continue;
					}
					int j = policyStorageTypes.Length;
					for (; j < storageTypes.Length; j++)
					{
						if (policyStorageTypes[i - 1] != storageTypes[j])
						{
							break;
						}
					}
					if (j == storageTypes.Length)
					{
						return storagePolicy;
					}
				}
			}
			return null;
		}

		/// <summary>Internal class which represents a unique Storage type combination</summary>
		internal class StorageTypeAllocation
		{
			private readonly BlockStoragePolicy specifiedStoragePolicy;

			private readonly StorageType[] storageTypes;

			private BlockStoragePolicy actualStoragePolicy;

			internal StorageTypeAllocation(StorageType[] storageTypes, BlockStoragePolicy specifiedStoragePolicy
				)
			{
				Arrays.Sort(storageTypes);
				this.storageTypes = storageTypes;
				this.specifiedStoragePolicy = specifiedStoragePolicy;
			}

			internal virtual StorageType[] GetStorageTypes()
			{
				return storageTypes;
			}

			internal virtual BlockStoragePolicy GetSpecifiedStoragePolicy()
			{
				return specifiedStoragePolicy;
			}

			internal virtual void SetActualStoragePolicy(BlockStoragePolicy actualStoragePolicy
				)
			{
				this.actualStoragePolicy = actualStoragePolicy;
			}

			internal virtual BlockStoragePolicy GetActualStoragePolicy()
			{
				return actualStoragePolicy;
			}

			private static string GetStorageAllocationAsString(IDictionary<StorageType, int> 
				storageType_countmap)
			{
				StringBuilder sb = new StringBuilder();
				foreach (KeyValuePair<StorageType, int> storageTypeCountEntry in storageType_countmap)
				{
					sb.Append(storageTypeCountEntry.Key.ToString() + ":" + storageTypeCountEntry.Value
						 + ",");
				}
				if (sb.Length > 1)
				{
					Sharpen.Runtime.DeleteCharAt(sb, sb.Length - 1);
				}
				return sb.ToString();
			}

			private string GetStorageAllocationAsString()
			{
				IDictionary<StorageType, int> storageType_countmap = new EnumMap<StorageType, int
					>(typeof(StorageType));
				foreach (StorageType storageType in storageTypes)
				{
					int count = storageType_countmap[storageType];
					if (count == null)
					{
						storageType_countmap[storageType] = 1;
					}
					else
					{
						storageType_countmap[storageType] = count + 1;
					}
				}
				return (GetStorageAllocationAsString(storageType_countmap));
			}

			internal virtual string GetStoragePolicyDescriptor()
			{
				StringBuilder storagePolicyDescriptorSB = new StringBuilder();
				if (actualStoragePolicy != null)
				{
					storagePolicyDescriptorSB.Append(GetStorageAllocationAsString()).Append("(").Append
						(actualStoragePolicy.GetName()).Append(")");
				}
				else
				{
					storagePolicyDescriptorSB.Append(GetStorageAllocationAsString());
				}
				return storagePolicyDescriptorSB.ToString();
			}

			internal virtual bool PolicyMatches()
			{
				return specifiedStoragePolicy.Equals(actualStoragePolicy);
			}

			public override string ToString()
			{
				return specifiedStoragePolicy.GetName() + "|" + GetStoragePolicyDescriptor();
			}

			public override int GetHashCode()
			{
				return Objects.Hash(specifiedStoragePolicy, Arrays.HashCode(storageTypes));
			}

			public override bool Equals(object another)
			{
				return (another is StoragePolicySummary.StorageTypeAllocation && Objects.Equals(specifiedStoragePolicy
					, ((StoragePolicySummary.StorageTypeAllocation)another).specifiedStoragePolicy) 
					&& Arrays.Equals(storageTypes, ((StoragePolicySummary.StorageTypeAllocation)another
					).storageTypes));
			}
		}
	}
}
