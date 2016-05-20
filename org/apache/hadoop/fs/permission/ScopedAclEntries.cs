using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>Groups a list of ACL entries into separate lists for access entries vs.</summary>
	/// <remarks>
	/// Groups a list of ACL entries into separate lists for access entries vs.
	/// default entries.
	/// </remarks>
	public sealed class ScopedAclEntries
	{
		private const int PIVOT_NOT_FOUND = -1;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> accessEntries;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> defaultEntries;

		/// <summary>Creates a new ScopedAclEntries from the given list.</summary>
		/// <remarks>
		/// Creates a new ScopedAclEntries from the given list.  It is assumed that the
		/// list is already sorted such that all access entries precede all default
		/// entries.
		/// </remarks>
		/// <param name="aclEntries">List<AclEntry> to separate</param>
		public ScopedAclEntries(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> aclEntries)
		{
			int pivot = calculatePivotOnDefaultEntries(aclEntries);
			if (pivot != PIVOT_NOT_FOUND)
			{
				accessEntries = pivot != 0 ? aclEntries.subList(0, pivot) : java.util.Collections
					.emptyList<org.apache.hadoop.fs.permission.AclEntry>();
				defaultEntries = aclEntries.subList(pivot, aclEntries.Count);
			}
			else
			{
				accessEntries = aclEntries;
				defaultEntries = java.util.Collections.emptyList();
			}
		}

		/// <summary>Returns access entries.</summary>
		/// <returns>
		/// List<AclEntry> containing just access entries, or an empty list if
		/// there are no access entries
		/// </returns>
		public System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry>
			 getAccessEntries()
		{
			return accessEntries;
		}

		/// <summary>Returns default entries.</summary>
		/// <returns>
		/// List<AclEntry> containing just default entries, or an empty list if
		/// there are no default entries
		/// </returns>
		public System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry>
			 getDefaultEntries()
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
		private static int calculatePivotOnDefaultEntries(System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> aclBuilder)
		{
			for (int i = 0; i < aclBuilder.Count; ++i)
			{
				if (aclBuilder[i].getScope() == org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT)
				{
					return i;
				}
			}
			return PIVOT_NOT_FOUND;
		}
	}
}
