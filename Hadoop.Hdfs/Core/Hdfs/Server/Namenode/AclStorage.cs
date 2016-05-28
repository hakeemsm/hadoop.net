using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// AclStorage contains utility methods that define how ACL data is stored in the
	/// namespace.
	/// </summary>
	/// <remarks>
	/// AclStorage contains utility methods that define how ACL data is stored in the
	/// namespace.
	/// If an inode has an ACL, then the ACL bit is set in the inode's
	/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
	/// and the inode also contains an
	/// <see cref="AclFeature"/>
	/// .  For
	/// the access ACL, the owner and other entries are identical to the owner and
	/// other bits stored in FsPermission, so we reuse those.  The access mask entry
	/// is stored into the group permission bits of FsPermission.  This is consistent
	/// with other file systems' implementations of ACLs and eliminates the need for
	/// special handling in various parts of the codebase.  For example, if a user
	/// calls chmod to change group permission bits on a file with an ACL, then the
	/// expected behavior is to change the ACL's mask entry.  By saving the mask entry
	/// into the group permission bits, chmod continues to work correctly without
	/// special handling.  All remaining access entries (named users and named groups)
	/// are stored as explicit
	/// <see cref="Org.Apache.Hadoop.FS.Permission.AclEntry"/>
	/// instances in a list inside the
	/// AclFeature.  Additionally, all default entries are stored in the AclFeature.
	/// The methods in this class encapsulate these rules for reading or writing the
	/// ACL entries to the appropriate location.
	/// The methods in this class assume that input ACL entry lists have already been
	/// validated and sorted according to the rules enforced by
	/// <see cref="AclTransformation"/>
	/// .
	/// </remarks>
	public sealed class AclStorage
	{
		private static readonly ReferenceCountMap<AclFeature> UniqueAclFeatures = new ReferenceCountMap
			<AclFeature>();

		/// <summary>
		/// If a default ACL is defined on a parent directory, then copies that default
		/// ACL to a newly created child file or directory.
		/// </summary>
		/// <param name="child">INode newly created child</param>
		public static void CopyINodeDefaultAcl(INode child)
		{
			INodeDirectory parent = child.GetParent();
			AclFeature parentAclFeature = parent.GetAclFeature();
			if (parentAclFeature == null || !(child.IsFile() || child.IsDirectory()))
			{
				return;
			}
			// Split parent's entries into access vs. default.
			IList<AclEntry> featureEntries = GetEntriesFromAclFeature(parent.GetAclFeature());
			ScopedAclEntries scopedEntries = new ScopedAclEntries(featureEntries);
			IList<AclEntry> parentDefaultEntries = scopedEntries.GetDefaultEntries();
			// The parent may have an access ACL but no default ACL.  If so, exit.
			if (parentDefaultEntries.IsEmpty())
			{
				return;
			}
			// Pre-allocate list size for access entries to copy from parent.
			IList<AclEntry> accessEntries = Lists.NewArrayListWithCapacity(parentDefaultEntries
				.Count);
			FsPermission childPerm = child.GetFsPermission();
			// Copy each default ACL entry from parent to new child's access ACL.
			bool parentDefaultIsMinimal = AclUtil.IsMinimalAcl(parentDefaultEntries);
			foreach (AclEntry entry in parentDefaultEntries)
			{
				AclEntryType type = entry.GetType();
				string name = entry.GetName();
				AclEntry.Builder builder = new AclEntry.Builder().SetScope(AclEntryScope.Access).
					SetType(type).SetName(name);
				// The child's initial permission bits are treated as the mode parameter,
				// which can filter copied permission values for owner, mask and other.
				FsAction permission;
				if (type == AclEntryType.User && name == null)
				{
					permission = entry.GetPermission().And(childPerm.GetUserAction());
				}
				else
				{
					if (type == AclEntryType.Group && parentDefaultIsMinimal)
					{
						// This only happens if the default ACL is a minimal ACL: exactly 3
						// entries corresponding to owner, group and other.  In this case,
						// filter the group permissions.
						permission = entry.GetPermission().And(childPerm.GetGroupAction());
					}
					else
					{
						if (type == AclEntryType.Mask)
						{
							// Group bits from mode parameter filter permission of mask entry.
							permission = entry.GetPermission().And(childPerm.GetGroupAction());
						}
						else
						{
							if (type == AclEntryType.Other)
							{
								permission = entry.GetPermission().And(childPerm.GetOtherAction());
							}
							else
							{
								permission = entry.GetPermission();
							}
						}
					}
				}
				builder.SetPermission(permission);
				accessEntries.AddItem(builder.Build());
			}
			// A new directory also receives a copy of the parent's default ACL.
			IList<AclEntry> defaultEntries = child.IsDirectory() ? parentDefaultEntries : Sharpen.Collections
				.EmptyList<AclEntry>();
			FsPermission newPerm;
			if (!AclUtil.IsMinimalAcl(accessEntries) || !defaultEntries.IsEmpty())
			{
				// Save the new ACL to the child.
				child.AddAclFeature(CreateAclFeature(accessEntries, defaultEntries));
				newPerm = CreateFsPermissionForExtendedAcl(accessEntries, childPerm);
			}
			else
			{
				// The child is receiving a minimal ACL.
				newPerm = CreateFsPermissionForMinimalAcl(accessEntries, childPerm);
			}
			child.SetPermission(newPerm);
		}

		/// <summary>Reads the existing extended ACL entries of an inode.</summary>
		/// <remarks>
		/// Reads the existing extended ACL entries of an inode.  This method returns
		/// only the extended ACL entries stored in the AclFeature.  If the inode does
		/// not have an ACL, then this method returns an empty list.  This method
		/// supports querying by snapshot ID.
		/// </remarks>
		/// <param name="inode">INode to read</param>
		/// <param name="snapshotId">int ID of snapshot to read</param>
		/// <returns>List<AclEntry> containing extended inode ACL entries</returns>
		public static IList<AclEntry> ReadINodeAcl(INode inode, int snapshotId)
		{
			AclFeature f = inode.GetAclFeature(snapshotId);
			return GetEntriesFromAclFeature(f);
		}

		/// <summary>Reads the existing extended ACL entries of an INodeAttribute object.</summary>
		/// <param name="inodeAttr">INode to read</param>
		/// <returns>List<AclEntry> containing extended inode ACL entries</returns>
		public static IList<AclEntry> ReadINodeAcl(INodeAttributes inodeAttr)
		{
			AclFeature f = inodeAttr.GetAclFeature();
			return GetEntriesFromAclFeature(f);
		}

		/// <summary>Build list of AclEntries from the AclFeature</summary>
		/// <param name="aclFeature">AclFeature</param>
		/// <returns>List of entries</returns>
		[VisibleForTesting]
		internal static ImmutableList<AclEntry> GetEntriesFromAclFeature(AclFeature aclFeature
			)
		{
			if (aclFeature == null)
			{
				return ImmutableList.Of<AclEntry>();
			}
			ImmutableList.Builder<AclEntry> b = new ImmutableList.Builder<AclEntry>();
			for (int pos = 0; pos < aclFeature.GetEntriesSize(); pos++)
			{
				entry = aclFeature.GetEntryAt(pos);
				b.Add(AclEntryStatusFormat.ToAclEntry(entry));
			}
			return ((ImmutableList<AclEntry>)b.Build());
		}

		/// <summary>Reads the existing ACL of an inode.</summary>
		/// <remarks>
		/// Reads the existing ACL of an inode.  This method always returns the full
		/// logical ACL of the inode after reading relevant data from the inode's
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
		/// and
		/// <see cref="AclFeature"/>
		/// .  Note that every inode
		/// logically has an ACL, even if no ACL has been set explicitly.  If the inode
		/// does not have an extended ACL, then the result is a minimal ACL consising of
		/// exactly 3 entries that correspond to the owner, group and other permissions.
		/// This method always reads the inode's current state and does not support
		/// querying by snapshot ID.  This is because the method is intended to support
		/// ACL modification APIs, which always apply a delta on top of current state.
		/// </remarks>
		/// <param name="inode">INode to read</param>
		/// <returns>List<AclEntry> containing all logical inode ACL entries</returns>
		public static IList<AclEntry> ReadINodeLogicalAcl(INode inode)
		{
			FsPermission perm = inode.GetFsPermission();
			AclFeature f = inode.GetAclFeature();
			if (f == null)
			{
				return AclUtil.GetMinimalAcl(perm);
			}
			IList<AclEntry> existingAcl;
			// Split ACL entries stored in the feature into access vs. default.
			IList<AclEntry> featureEntries = GetEntriesFromAclFeature(f);
			ScopedAclEntries scoped = new ScopedAclEntries(featureEntries);
			IList<AclEntry> accessEntries = scoped.GetAccessEntries();
			IList<AclEntry> defaultEntries = scoped.GetDefaultEntries();
			// Pre-allocate list size for the explicit entries stored in the feature
			// plus the 3 implicit entries (owner, group and other) from the permission
			// bits.
			existingAcl = Lists.NewArrayListWithCapacity(featureEntries.Count + 3);
			if (!accessEntries.IsEmpty())
			{
				// Add owner entry implied from user permission bits.
				existingAcl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.User).SetPermission(perm.GetUserAction()).Build());
				// Next add all named user and group entries taken from the feature.
				Sharpen.Collections.AddAll(existingAcl, accessEntries);
				// Add mask entry implied from group permission bits.
				existingAcl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.Mask).SetPermission(perm.GetGroupAction()).Build());
				// Add other entry implied from other permission bits.
				existingAcl.AddItem(new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.Other).SetPermission(perm.GetOtherAction()).Build());
			}
			else
			{
				// It's possible that there is a default ACL but no access ACL. In this
				// case, add the minimal access ACL implied by the permission bits.
				Sharpen.Collections.AddAll(existingAcl, AclUtil.GetMinimalAcl(perm));
			}
			// Add all default entries after the access entries.
			Sharpen.Collections.AddAll(existingAcl, defaultEntries);
			// The above adds entries in the correct order, so no need to sort here.
			return existingAcl;
		}

		/// <summary>Updates an inode with a new ACL.</summary>
		/// <remarks>
		/// Updates an inode with a new ACL.  This method takes a full logical ACL and
		/// stores the entries to the inode's
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
		/// and
		/// <see cref="AclFeature"/>
		/// .
		/// </remarks>
		/// <param name="inode">INode to update</param>
		/// <param name="newAcl">List<AclEntry> containing new ACL entries</param>
		/// <param name="snapshotId">int latest snapshot ID of inode</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException">if the ACL is invalid for the given inode
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if quota limit is exceeded
		/// 	</exception>
		public static void UpdateINodeAcl(INode inode, IList<AclEntry> newAcl, int snapshotId
			)
		{
			System.Diagnostics.Debug.Assert(newAcl.Count >= 3);
			FsPermission perm = inode.GetFsPermission();
			FsPermission newPerm;
			if (!AclUtil.IsMinimalAcl(newAcl))
			{
				// This is an extended ACL.  Split entries into access vs. default.
				ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
				IList<AclEntry> accessEntries = scoped.GetAccessEntries();
				IList<AclEntry> defaultEntries = scoped.GetDefaultEntries();
				// Only directories may have a default ACL.
				if (!defaultEntries.IsEmpty() && !inode.IsDirectory())
				{
					throw new AclException("Invalid ACL: only directories may have a default ACL.");
				}
				// Attach entries to the feature.
				if (inode.GetAclFeature() != null)
				{
					inode.RemoveAclFeature(snapshotId);
				}
				inode.AddAclFeature(CreateAclFeature(accessEntries, defaultEntries), snapshotId);
				newPerm = CreateFsPermissionForExtendedAcl(accessEntries, perm);
			}
			else
			{
				// This is a minimal ACL.  Remove the ACL feature if it previously had one.
				if (inode.GetAclFeature() != null)
				{
					inode.RemoveAclFeature(snapshotId);
				}
				newPerm = CreateFsPermissionForMinimalAcl(newAcl, perm);
			}
			inode.SetPermission(newPerm, snapshotId);
		}

		/// <summary>There is no reason to instantiate this class.</summary>
		private AclStorage()
		{
		}

		/// <summary>Creates an AclFeature from the given ACL entries.</summary>
		/// <param name="accessEntries">List<AclEntry> access ACL entries</param>
		/// <param name="defaultEntries">List<AclEntry> default ACL entries</param>
		/// <returns>AclFeature containing the required ACL entries</returns>
		private static AclFeature CreateAclFeature(IList<AclEntry> accessEntries, IList<AclEntry
			> defaultEntries)
		{
			// Pre-allocate list size for the explicit entries stored in the feature,
			// which is all entries minus the 3 entries implicitly stored in the
			// permission bits.
			IList<AclEntry> featureEntries = Lists.NewArrayListWithCapacity((accessEntries.Count
				 - 3) + defaultEntries.Count);
			// For the access ACL, the feature only needs to hold the named user and
			// group entries.  For a correctly sorted ACL, these will be in a
			// predictable range.
			if (!AclUtil.IsMinimalAcl(accessEntries))
			{
				Sharpen.Collections.AddAll(featureEntries, accessEntries.SubList(1, accessEntries
					.Count - 2));
			}
			// Add all default entries to the feature.
			Sharpen.Collections.AddAll(featureEntries, defaultEntries);
			return new AclFeature(AclEntryStatusFormat.ToInt(featureEntries));
		}

		/// <summary>
		/// Creates the new FsPermission for an inode that is receiving an extended
		/// ACL, based on its access ACL entries.
		/// </summary>
		/// <remarks>
		/// Creates the new FsPermission for an inode that is receiving an extended
		/// ACL, based on its access ACL entries.  For a correctly sorted ACL, the
		/// first entry is the owner and the last 2 entries are the mask and other
		/// entries respectively.  Also preserve sticky bit and toggle ACL bit on.
		/// Note that this method intentionally copies the permissions of the mask
		/// entry into the FsPermission group permissions.  This is consistent with the
		/// POSIX ACLs model, which presents the mask as the permissions of the group
		/// class.
		/// </remarks>
		/// <param name="accessEntries">List<AclEntry> access ACL entries</param>
		/// <param name="existingPerm">FsPermission existing permissions</param>
		/// <returns>FsPermission new permissions</returns>
		private static FsPermission CreateFsPermissionForExtendedAcl(IList<AclEntry> accessEntries
			, FsPermission existingPerm)
		{
			return new FsPermission(accessEntries[0].GetPermission(), accessEntries[accessEntries
				.Count - 2].GetPermission(), accessEntries[accessEntries.Count - 1].GetPermission
				(), existingPerm.GetStickyBit());
		}

		/// <summary>
		/// Creates the new FsPermission for an inode that is receiving a minimal ACL,
		/// based on its access ACL entries.
		/// </summary>
		/// <remarks>
		/// Creates the new FsPermission for an inode that is receiving a minimal ACL,
		/// based on its access ACL entries.  For a correctly sorted ACL, the owner,
		/// group and other permissions are in order.  Also preserve sticky bit and
		/// toggle ACL bit off.
		/// </remarks>
		/// <param name="accessEntries">List<AclEntry> access ACL entries</param>
		/// <param name="existingPerm">FsPermission existing permissions</param>
		/// <returns>FsPermission new permissions</returns>
		private static FsPermission CreateFsPermissionForMinimalAcl(IList<AclEntry> accessEntries
			, FsPermission existingPerm)
		{
			return new FsPermission(accessEntries[0].GetPermission(), accessEntries[1].GetPermission
				(), accessEntries[2].GetPermission(), existingPerm.GetStickyBit());
		}

		[VisibleForTesting]
		public static ReferenceCountMap<AclFeature> GetUniqueAclFeatures()
		{
			return UniqueAclFeatures;
		}

		/// <summary>Add reference for the said AclFeature</summary>
		/// <param name="aclFeature"/>
		/// <returns>Referenced AclFeature</returns>
		public static AclFeature AddAclFeature(AclFeature aclFeature)
		{
			return UniqueAclFeatures.Put(aclFeature);
		}

		/// <summary>Remove reference to the AclFeature</summary>
		/// <param name="aclFeature"/>
		public static void RemoveAclFeature(AclFeature aclFeature)
		{
			UniqueAclFeatures.Remove(aclFeature);
		}
	}
}
