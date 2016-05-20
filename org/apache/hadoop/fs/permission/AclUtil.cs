using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>AclUtil contains utility methods for manipulating ACLs.</summary>
	public sealed class AclUtil
	{
		/// <summary>Given permissions and extended ACL entries, returns the full logical ACL.
		/// 	</summary>
		/// <param name="perm">FsPermission containing permissions</param>
		/// <param name="entries">List<AclEntry> containing extended ACL entries</param>
		/// <returns>List<AclEntry> containing full logical ACL</returns>
		public static System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> getAclFromPermAndEntries(org.apache.hadoop.fs.permission.FsPermission perm, System.Collections.Generic.IList
			<org.apache.hadoop.fs.permission.AclEntry> entries)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> acl = 
				com.google.common.collect.Lists.newArrayListWithCapacity(entries.Count + 3);
			// Owner entry implied by owner permission bits.
			acl.add(new org.apache.hadoop.fs.permission.AclEntry.Builder().setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.ACCESS).setType(org.apache.hadoop.fs.permission.AclEntryType.USER).setPermission
				(perm.getUserAction()).build());
			// All extended access ACL entries.
			bool hasAccessAcl = false;
			System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.permission.AclEntry> 
				entryIter = entries.GetEnumerator();
			org.apache.hadoop.fs.permission.AclEntry curEntry = null;
			while (entryIter.MoveNext())
			{
				curEntry = entryIter.Current;
				if (curEntry.getScope() == org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT)
				{
					break;
				}
				hasAccessAcl = true;
				acl.add(curEntry);
			}
			// Mask entry implied by group permission bits, or group entry if there is
			// no access ACL (only default ACL).
			acl.add(new org.apache.hadoop.fs.permission.AclEntry.Builder().setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.ACCESS).setType(hasAccessAcl ? org.apache.hadoop.fs.permission.AclEntryType.MASK
				 : org.apache.hadoop.fs.permission.AclEntryType.GROUP).setPermission(perm.getGroupAction
				()).build());
			// Other entry implied by other bits.
			acl.add(new org.apache.hadoop.fs.permission.AclEntry.Builder().setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.ACCESS).setType(org.apache.hadoop.fs.permission.AclEntryType.OTHER).setPermission
				(perm.getOtherAction()).build());
			// Default ACL entries.
			if (curEntry != null && curEntry.getScope() == org.apache.hadoop.fs.permission.AclEntryScope
				.DEFAULT)
			{
				acl.add(curEntry);
				while (entryIter.MoveNext())
				{
					acl.add(entryIter.Current);
				}
			}
			return acl;
		}

		/// <summary>Translates the given permission bits to the equivalent minimal ACL.</summary>
		/// <param name="perm">FsPermission to translate</param>
		/// <returns>
		/// List<AclEntry> containing exactly 3 entries representing the owner,
		/// group and other permissions
		/// </returns>
		public static System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> getMinimalAcl(org.apache.hadoop.fs.permission.FsPermission perm)
		{
			return com.google.common.collect.Lists.newArrayList(new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setScope(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS).setType(org.apache.hadoop.fs.permission.AclEntryType
				.USER).setPermission(perm.getUserAction()).build(), new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setScope(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS).setType(org.apache.hadoop.fs.permission.AclEntryType
				.GROUP).setPermission(perm.getGroupAction()).build(), new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setScope(org.apache.hadoop.fs.permission.AclEntryScope.ACCESS).setType(org.apache.hadoop.fs.permission.AclEntryType
				.OTHER).setPermission(perm.getOtherAction()).build());
		}

		/// <summary>
		/// Checks if the given entries represent a minimal ACL (contains exactly 3
		/// entries).
		/// </summary>
		/// <param name="entries">List<AclEntry> entries to check</param>
		/// <returns>boolean true if the entries represent a minimal ACL</returns>
		public static bool isMinimalAcl(System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry
			> entries)
		{
			return entries.Count == 3;
		}

		/// <summary>There is no reason to instantiate this class.</summary>
		private AclUtil()
		{
		}
	}
}
