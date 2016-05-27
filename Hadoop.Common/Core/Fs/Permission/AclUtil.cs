using System.Collections.Generic;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>AclUtil contains utility methods for manipulating ACLs.</summary>
	public sealed class AclUtil
	{
		/// <summary>Given permissions and extended ACL entries, returns the full logical ACL.
		/// 	</summary>
		/// <param name="perm">FsPermission containing permissions</param>
		/// <param name="entries">List<AclEntry> containing extended ACL entries</param>
		/// <returns>List<AclEntry> containing full logical ACL</returns>
		public static IList<AclEntry> GetAclFromPermAndEntries(FsPermission perm, IList<AclEntry
			> entries)
		{
			IList<AclEntry> acl = Lists.NewArrayListWithCapacity(entries.Count + 3);
			// Owner entry implied by owner permission bits.
			acl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType(AclEntryType
				.User).SetPermission(perm.GetUserAction()).Build());
			// All extended access ACL entries.
			bool hasAccessAcl = false;
			IEnumerator<AclEntry> entryIter = entries.GetEnumerator();
			AclEntry curEntry = null;
			while (entryIter.HasNext())
			{
				curEntry = entryIter.Next();
				if (curEntry.GetScope() == AclEntryScope.Default)
				{
					break;
				}
				hasAccessAcl = true;
				acl.AddItem(curEntry);
			}
			// Mask entry implied by group permission bits, or group entry if there is
			// no access ACL (only default ACL).
			acl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType(hasAccessAcl
				 ? AclEntryType.Mask : AclEntryType.Group).SetPermission(perm.GetGroupAction()).
				Build());
			// Other entry implied by other bits.
			acl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType(AclEntryType
				.Other).SetPermission(perm.GetOtherAction()).Build());
			// Default ACL entries.
			if (curEntry != null && curEntry.GetScope() == AclEntryScope.Default)
			{
				acl.AddItem(curEntry);
				while (entryIter.HasNext())
				{
					acl.AddItem(entryIter.Next());
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
		public static IList<AclEntry> GetMinimalAcl(FsPermission perm)
		{
			return Lists.NewArrayList(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
				(AclEntryType.User).SetPermission(perm.GetUserAction()).Build(), new AclEntry.Builder
				().SetScope(AclEntryScope.Access).SetType(AclEntryType.Group).SetPermission(perm
				.GetGroupAction()).Build(), new AclEntry.Builder().SetScope(AclEntryScope.Access
				).SetType(AclEntryType.Other).SetPermission(perm.GetOtherAction()).Build());
		}

		/// <summary>
		/// Checks if the given entries represent a minimal ACL (contains exactly 3
		/// entries).
		/// </summary>
		/// <param name="entries">List<AclEntry> entries to check</param>
		/// <returns>boolean true if the entries represent a minimal ACL</returns>
		public static bool IsMinimalAcl(IList<AclEntry> entries)
		{
			return entries.Count == 3;
		}

		/// <summary>There is no reason to instantiate this class.</summary>
		private AclUtil()
		{
		}
	}
}
