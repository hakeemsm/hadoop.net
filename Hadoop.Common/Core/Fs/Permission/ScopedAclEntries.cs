using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>Groups a list of ACL entries into separate lists for access entries vs.</summary>
	/// <remarks>
	/// Groups a list of ACL entries into separate lists for access entries vs.
	/// default entries.
	/// </remarks>
	public sealed class ScopedAclEntries
	{
		private const int PivotNotFound = -1;

		private readonly IList<AclEntry> accessEntries;

		private readonly IList<AclEntry> defaultEntries;

		/// <summary>Creates a new ScopedAclEntries from the given list.</summary>
		/// <remarks>
		/// Creates a new ScopedAclEntries from the given list.  It is assumed that the
		/// list is already sorted such that all access entries precede all default
		/// entries.
		/// </remarks>
		/// <param name="aclEntries">List<AclEntry> to separate</param>
		public ScopedAclEntries(IList<AclEntry> aclEntries)
		{
			int pivot = CalculatePivotOnDefaultEntries(aclEntries);
			if (pivot != PivotNotFound)
			{
				accessEntries = pivot != 0 ? aclEntries.SubList(0, pivot) : Sharpen.Collections.EmptyList
					<AclEntry>();
				defaultEntries = aclEntries.SubList(pivot, aclEntries.Count);
			}
			else
			{
				accessEntries = aclEntries;
				defaultEntries = Sharpen.Collections.EmptyList();
			}
		}

		/// <summary>Returns access entries.</summary>
		/// <returns>
		/// List<AclEntry> containing just access entries, or an empty list if
		/// there are no access entries
		/// </returns>
		public IList<AclEntry> GetAccessEntries()
		{
			return accessEntries;
		}

		/// <summary>Returns default entries.</summary>
		/// <returns>
		/// List<AclEntry> containing just default entries, or an empty list if
		/// there are no default entries
		/// </returns>
		public IList<AclEntry> GetDefaultEntries()
		{
			return defaultEntries;
		}

		/// <summary>
		/// Returns the pivot point in the list between the access entries and the
		/// default entries.
		/// </summary>
		/// <remarks>
		/// Returns the pivot point in the list between the access entries and the
		/// default entries.  This is the index of the first element in the list that is
		/// a default entry.
		/// </remarks>
		/// <param name="aclBuilder">ArrayList<AclEntry> containing entries to build</param>
		/// <returns>int pivot point, or -1 if list contains no default entries</returns>
		private static int CalculatePivotOnDefaultEntries(IList<AclEntry> aclBuilder)
		{
			for (int i = 0; i < aclBuilder.Count; ++i)
			{
				if (aclBuilder[i].GetScope() == AclEntryScope.Default)
				{
					return i;
				}
			}
			return PivotNotFound;
		}
	}
}
