using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Class that helps in checking file system permission.</summary>
	/// <remarks>
	/// Class that helps in checking file system permission.
	/// The state of this class need not be synchronized as it has data structures that
	/// are read-only.
	/// Some of the helper methods are gaurded by
	/// <see cref="FSNamesystem.ReadLock()"/>
	/// .
	/// </remarks>
	internal class FSPermissionChecker : INodeAttributeProvider.AccessControlEnforcer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(UserGroupInformation)
			);

		/// <returns>
		/// a string for throwing
		/// <see cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// 
		/// </returns>
		private string ToAccessControlString(INodeAttributes inodeAttrib, string path, FsAction
			 access, FsPermission mode)
		{
			return ToAccessControlString(inodeAttrib, path, access, mode, false);
		}

		/// <returns>
		/// a string for throwing
		/// <see cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// 
		/// </returns>
		private string ToAccessControlString(INodeAttributes inodeAttrib, string path, FsAction
			 access, FsPermission mode, bool deniedFromAcl)
		{
			StringBuilder sb = new StringBuilder("Permission denied: ").Append("user=").Append
				(GetUser()).Append(", ").Append("access=").Append(access).Append(", ").Append("inode=\""
				).Append(path).Append("\":").Append(inodeAttrib.GetUserName()).Append(':').Append
				(inodeAttrib.GetGroupName()).Append(':').Append(inodeAttrib.IsDirectory() ? 'd' : 
				'-').Append(mode);
			if (deniedFromAcl)
			{
				sb.Append("+");
			}
			return sb.ToString();
		}

		private readonly string fsOwner;

		private readonly string supergroup;

		private readonly UserGroupInformation callerUgi;

		private readonly string user;

		private readonly ICollection<string> groups;

		private readonly bool isSuper;

		private readonly INodeAttributeProvider attributeProvider;

		internal FSPermissionChecker(string fsOwner, string supergroup, UserGroupInformation
			 callerUgi, INodeAttributeProvider attributeProvider)
		{
			this.fsOwner = fsOwner;
			this.supergroup = supergroup;
			this.callerUgi = callerUgi;
			HashSet<string> s = new HashSet<string>(Arrays.AsList(callerUgi.GetGroupNames()));
			groups = Sharpen.Collections.UnmodifiableSet(s);
			user = callerUgi.GetShortUserName();
			isSuper = user.Equals(fsOwner) || groups.Contains(supergroup);
			this.attributeProvider = attributeProvider;
		}

		public virtual bool ContainsGroup(string group)
		{
			return groups.Contains(group);
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual ICollection<string> GetGroups()
		{
			return groups;
		}

		public virtual bool IsSuperUser()
		{
			return isSuper;
		}

		public virtual INodeAttributeProvider GetAttributesProvider()
		{
			return attributeProvider;
		}

		/// <summary>Verify if the caller has the required permission.</summary>
		/// <remarks>
		/// Verify if the caller has the required permission. This will result into
		/// an exception if the caller is not allowed to access the resource.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void CheckSuperuserPrivilege()
		{
			if (!IsSuperUser())
			{
				throw new AccessControlException("Access denied for user " + GetUser() + ". Superuser privilege is required"
					);
			}
		}

		/// <summary>Check whether current user have permissions to access the path.</summary>
		/// <remarks>
		/// Check whether current user have permissions to access the path.
		/// Traverse is always checked.
		/// Parent path means the parent directory for the path.
		/// Ancestor path means the last (the closest) existing ancestor directory
		/// of the path.
		/// Note that if the parent path exists,
		/// then the parent path and the ancestor path are the same.
		/// For example, suppose the path is "/foo/bar/baz".
		/// No matter baz is a file or a directory,
		/// the parent path is "/foo/bar".
		/// If bar exists, then the ancestor path is also "/foo/bar".
		/// If bar does not exist and foo exists,
		/// then the ancestor path is "/foo".
		/// Further, if both foo and bar do not exist,
		/// then the ancestor path is "/".
		/// </remarks>
		/// <param name="doCheckOwner">Require user to be the owner of the path?</param>
		/// <param name="ancestorAccess">The access required by the ancestor of the path.</param>
		/// <param name="parentAccess">The access required by the parent of the path.</param>
		/// <param name="access">The access required by the path.</param>
		/// <param name="subAccess">
		/// If path is a directory,
		/// it is the access required of the path and all the sub-directories.
		/// If path is not a directory, there is no effect.
		/// </param>
		/// <param name="ignoreEmptyDir">Ignore permission checking for empty directory?</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// Caller of this method must hold that lock.
		/// </exception>
		internal virtual void CheckPermission(INodesInPath inodesInPath, bool doCheckOwner
			, FsAction ancestorAccess, FsAction parentAccess, FsAction access, FsAction subAccess
			, bool ignoreEmptyDir)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("ACCESS CHECK: " + this + ", doCheckOwner=" + doCheckOwner + ", ancestorAccess="
					 + ancestorAccess + ", parentAccess=" + parentAccess + ", access=" + access + ", subAccess="
					 + subAccess + ", ignoreEmptyDir=" + ignoreEmptyDir);
			}
			// check if (parentAccess != null) && file exists, then check sb
			// If resolveLink, the check is performed on the link target.
			int snapshotId = inodesInPath.GetPathSnapshotId();
			INode[] inodes = inodesInPath.GetINodesArray();
			INodeAttributes[] inodeAttrs = new INodeAttributes[inodes.Length];
			byte[][] pathByNameArr = new byte[inodes.Length][];
			for (int i = 0; i < inodes.Length && inodes[i] != null; i++)
			{
				if (inodes[i] != null)
				{
					pathByNameArr[i] = inodes[i].GetLocalNameBytes();
					inodeAttrs[i] = GetINodeAttrs(pathByNameArr, i, inodes[i], snapshotId);
				}
			}
			string path = inodesInPath.GetPath();
			int ancestorIndex = inodes.Length - 2;
			INodeAttributeProvider.AccessControlEnforcer enforcer = GetAttributesProvider().GetExternalAccessControlEnforcer
				(this);
			enforcer.CheckPermission(fsOwner, supergroup, callerUgi, inodeAttrs, inodes, pathByNameArr
				, snapshotId, path, ancestorIndex, doCheckOwner, ancestorAccess, parentAccess, access
				, subAccess, ignoreEmptyDir);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void CheckPermission(string fsOwner, string supergroup, UserGroupInformation
			 callerUgi, INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr
			, int snapshotId, string path, int ancestorIndex, bool doCheckOwner, FsAction ancestorAccess
			, FsAction parentAccess, FsAction access, FsAction subAccess, bool ignoreEmptyDir
			)
		{
			for (; ancestorIndex >= 0 && inodes[ancestorIndex] == null; ancestorIndex--)
			{
			}
			CheckTraverse(inodeAttrs, path, ancestorIndex);
			INodeAttributes last = inodeAttrs[inodeAttrs.Length - 1];
			if (parentAccess != null && parentAccess.Implies(FsAction.Write) && inodeAttrs.Length
				 > 1 && last != null)
			{
				CheckStickyBit(inodeAttrs[inodeAttrs.Length - 2], last);
			}
			if (ancestorAccess != null && inodeAttrs.Length > 1)
			{
				Check(inodeAttrs, path, ancestorIndex, ancestorAccess);
			}
			if (parentAccess != null && inodeAttrs.Length > 1)
			{
				Check(inodeAttrs, path, inodeAttrs.Length - 2, parentAccess);
			}
			if (access != null)
			{
				Check(last, path, access);
			}
			if (subAccess != null)
			{
				INode rawLast = inodes[inodeAttrs.Length - 1];
				CheckSubAccess(pathByNameArr, inodeAttrs.Length - 1, rawLast, snapshotId, subAccess
					, ignoreEmptyDir);
			}
			if (doCheckOwner)
			{
				CheckOwner(last);
			}
		}

		private INodeAttributes GetINodeAttrs(byte[][] pathByNameArr, int pathIdx, INode 
			inode, int snapshotId)
		{
			INodeAttributes inodeAttrs = inode.GetSnapshotINode(snapshotId);
			if (GetAttributesProvider() != null)
			{
				string[] elements = new string[pathIdx + 1];
				for (int i = 0; i < elements.Length; i++)
				{
					elements[i] = DFSUtil.Bytes2String(pathByNameArr[i]);
				}
				inodeAttrs = GetAttributesProvider().GetAttributes(elements, inodeAttrs);
			}
			return inodeAttrs;
		}

		/// <summary>
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void CheckOwner(INodeAttributes inode)
		{
			if (GetUser().Equals(inode.GetUserName()))
			{
				return;
			}
			throw new AccessControlException("Permission denied. user=" + GetUser() + " is not the owner of inode="
				 + inode);
		}

		/// <summary>
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void CheckTraverse(INodeAttributes[] inodes, string path, int last)
		{
			for (int j = 0; j <= last; j++)
			{
				Check(inodes[j], path, FsAction.Execute);
			}
		}

		/// <summary>
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void CheckSubAccess(byte[][] pathByNameArr, int pathIdx, INode inode, int
			 snapshotId, FsAction access, bool ignoreEmptyDir)
		{
			if (inode == null || !inode.IsDirectory())
			{
				return;
			}
			Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
			for (directories.Push(inode.AsDirectory()); !directories.IsEmpty(); )
			{
				INodeDirectory d = directories.Pop();
				ReadOnlyList<INode> cList = d.GetChildrenList(snapshotId);
				if (!(cList.IsEmpty() && ignoreEmptyDir))
				{
					//TODO have to figure this out with inodeattribute provider
					Check(GetINodeAttrs(pathByNameArr, pathIdx, d, snapshotId), inode.GetFullPathName
						(), access);
				}
				foreach (INode child in cList)
				{
					if (child.IsDirectory())
					{
						directories.Push(child.AsDirectory());
					}
				}
			}
		}

		/// <summary>
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void Check(INodeAttributes[] inodes, string path, int i, FsAction access)
		{
			Check(i >= 0 ? inodes[i] : null, path, access);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void Check(INodeAttributes inode, string path, FsAction access)
		{
			if (inode == null)
			{
				return;
			}
			FsPermission mode = inode.GetFsPermission();
			AclFeature aclFeature = inode.GetAclFeature();
			if (aclFeature != null)
			{
				// It's possible that the inode has a default ACL but no access ACL.
				int firstEntry = aclFeature.GetEntryAt(0);
				if (AclEntryStatusFormat.GetScope(firstEntry) == AclEntryScope.Access)
				{
					CheckAccessAcl(inode, path, access, mode, aclFeature);
					return;
				}
			}
			if (GetUser().Equals(inode.GetUserName()))
			{
				//user class
				if (mode.GetUserAction().Implies(access))
				{
					return;
				}
			}
			else
			{
				if (GetGroups().Contains(inode.GetGroupName()))
				{
					//group class
					if (mode.GetGroupAction().Implies(access))
					{
						return;
					}
				}
				else
				{
					//other class
					if (mode.GetOtherAction().Implies(access))
					{
						return;
					}
				}
			}
			throw new AccessControlException(ToAccessControlString(inode, path, access, mode)
				);
		}

		/// <summary>Checks requested access against an Access Control List.</summary>
		/// <remarks>
		/// Checks requested access against an Access Control List.  This method relies
		/// on finding the ACL data in the relevant portions of
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
		/// and
		/// <see cref="AclFeature"/>
		/// as implemented in the logic of
		/// <see cref="AclStorage"/>
		/// .  This
		/// method also relies on receiving the ACL entries in sorted order.  This is
		/// assumed to be true, because the ACL modification methods in
		/// <see cref="AclTransformation"/>
		/// sort the resulting entries.
		/// More specifically, this method depends on these invariants in an ACL:
		/// - The list must be sorted.
		/// - Each entry in the list must be unique by scope + type + name.
		/// - There is exactly one each of the unnamed user/group/other entries.
		/// - The mask entry must not have a name.
		/// - The other entry must not have a name.
		/// - Default entries may be present, but they are ignored during enforcement.
		/// </remarks>
		/// <param name="inode">INodeAttributes accessed inode</param>
		/// <param name="snapshotId">int snapshot ID</param>
		/// <param name="access">FsAction requested permission</param>
		/// <param name="mode">FsPermission mode from inode</param>
		/// <param name="aclFeature">AclFeature of inode</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the ACL denies permission
		/// 	</exception>
		private void CheckAccessAcl(INodeAttributes inode, string path, FsAction access, 
			FsPermission mode, AclFeature aclFeature)
		{
			bool foundMatch = false;
			// Use owner entry from permission bits if user is owner.
			if (GetUser().Equals(inode.GetUserName()))
			{
				if (mode.GetUserAction().Implies(access))
				{
					return;
				}
				foundMatch = true;
			}
			// Check named user and group entries if user was not denied by owner entry.
			if (!foundMatch)
			{
				for (int pos = 0; pos < aclFeature.GetEntriesSize(); pos++)
				{
					entry = aclFeature.GetEntryAt(pos);
					if (AclEntryStatusFormat.GetScope(entry) == AclEntryScope.Default)
					{
						break;
					}
					AclEntryType type = AclEntryStatusFormat.GetType(entry);
					string name = AclEntryStatusFormat.GetName(entry);
					if (type == AclEntryType.User)
					{
						// Use named user entry with mask from permission bits applied if user
						// matches name.
						if (GetUser().Equals(name))
						{
							FsAction masked = AclEntryStatusFormat.GetPermission(entry).And(mode.GetGroupAction
								());
							if (masked.Implies(access))
							{
								return;
							}
							foundMatch = true;
							break;
						}
					}
					else
					{
						if (type == AclEntryType.Group)
						{
							// Use group entry (unnamed or named) with mask from permission bits
							// applied if user is a member and entry grants access.  If user is a
							// member of multiple groups that have entries that grant access, then
							// it doesn't matter which is chosen, so exit early after first match.
							string group = name == null ? inode.GetGroupName() : name;
							if (GetGroups().Contains(group))
							{
								FsAction masked = AclEntryStatusFormat.GetPermission(entry).And(mode.GetGroupAction
									());
								if (masked.Implies(access))
								{
									return;
								}
								foundMatch = true;
							}
						}
					}
				}
			}
			// Use other entry if user was not denied by an earlier match.
			if (!foundMatch && mode.GetOtherAction().Implies(access))
			{
				return;
			}
			throw new AccessControlException(ToAccessControlString(inode, path, access, mode)
				);
		}

		/// <summary>
		/// Guarded by
		/// <see cref="FSNamesystem.ReadLock()"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void CheckStickyBit(INodeAttributes parent, INodeAttributes inode)
		{
			if (!parent.GetFsPermission().GetStickyBit())
			{
				return;
			}
			// If this user is the directory owner, return
			if (parent.GetUserName().Equals(GetUser()))
			{
				return;
			}
			// if this user is the file owner, return
			if (inode.GetUserName().Equals(GetUser()))
			{
				return;
			}
			throw new AccessControlException("Permission denied by sticky bit setting:" + " user="
				 + GetUser() + ", inode=" + inode);
		}

		/// <summary>Whether a cache pool can be accessed by the current context</summary>
		/// <param name="pool">CachePool being accessed</param>
		/// <param name="access">type of action being performed on the cache pool</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if pool cannot be accessed
		/// 	</exception>
		public virtual void CheckPermission(CachePool pool, FsAction access)
		{
			FsPermission mode = pool.GetMode();
			if (IsSuperUser())
			{
				return;
			}
			if (GetUser().Equals(pool.GetOwnerName()) && mode.GetUserAction().Implies(access))
			{
				return;
			}
			if (GetGroups().Contains(pool.GetGroupName()) && mode.GetGroupAction().Implies(access
				))
			{
				return;
			}
			if (mode.GetOtherAction().Implies(access))
			{
				return;
			}
			throw new AccessControlException("Permission denied while accessing pool " + pool
				.GetPoolName() + ": user " + GetUser() + " does not have " + access.ToString() +
				 " permissions.");
		}
	}
}
