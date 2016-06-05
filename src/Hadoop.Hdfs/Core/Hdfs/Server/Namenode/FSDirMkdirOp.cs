using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirMkdirOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus Mkdirs(FSNamesystem fsn, string src, PermissionStatus
			 permissions, bool createParent)
		{
			FSDirectory fsd = fsn.GetFSDirectory();
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.mkdirs: " + src);
			}
			if (!DFSUtil.IsValidName(src))
			{
				throw new InvalidPathException(src);
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = fsd.GetINodesInPath4Write(src);
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckTraverse(pc, iip);
				}
				INode lastINode = iip.GetLastINode();
				if (lastINode != null && lastINode.IsFile())
				{
					throw new FileAlreadyExistsException("Path is not a directory: " + src);
				}
				INodesInPath existing = lastINode != null ? iip : iip.GetExistingINodes();
				if (lastINode == null)
				{
					if (fsd.IsPermissionEnabled())
					{
						fsd.CheckAncestorAccess(pc, iip, FsAction.Write);
					}
					if (!createParent)
					{
						fsd.VerifyParentDir(iip, src);
					}
					// validate that we have enough inodes. This is, at best, a
					// heuristic because the mkdirs() operation might need to
					// create multiple inodes.
					fsn.CheckFsObjectLimit();
					IList<string> nonExisting = iip.GetPath(existing.Length(), iip.Length() - existing
						.Length());
					int length = nonExisting.Count;
					if (length > 1)
					{
						IList<string> ancestors = nonExisting.SubList(0, length - 1);
						// Ensure that the user can traversal the path by adding implicit
						// u+wx permission to all ancestor directories
						existing = CreateChildrenDirectories(fsd, existing, ancestors, AddImplicitUwx(permissions
							, permissions));
						if (existing == null)
						{
							throw new IOException("Failed to create directory: " + src);
						}
					}
					if ((existing = CreateChildrenDirectories(fsd, existing, nonExisting.SubList(length
						 - 1, length), permissions)) == null)
					{
						throw new IOException("Failed to create directory: " + src);
					}
				}
				return fsd.GetAuditFileInfo(existing);
			}
			finally
			{
				fsd.WriteUnlock();
			}
		}

		/// <summary>
		/// For a given absolute path, create all ancestors as directories along the
		/// path.
		/// </summary>
		/// <remarks>
		/// For a given absolute path, create all ancestors as directories along the
		/// path. All ancestors inherit their parent's permission plus an implicit
		/// u+wx permission. This is used by create() and addSymlink() for
		/// implicitly creating all directories along the path.
		/// For example, path="/foo/bar/spam", "/foo" is an existing directory,
		/// "/foo/bar" is not existing yet, the function will create directory bar.
		/// </remarks>
		/// <returns>
		/// a tuple which contains both the new INodesInPath (with all the
		/// existing and newly created directories) and the last component in the
		/// relative path. Or return null if there are errors.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal static KeyValuePair<INodesInPath, string> CreateAncestorDirectories(FSDirectory
			 fsd, INodesInPath iip, PermissionStatus permission)
		{
			string last = new string(iip.GetLastLocalName(), Charsets.Utf8);
			INodesInPath existing = iip.GetExistingINodes();
			IList<string> children = iip.GetPath(existing.Length(), iip.Length() - existing.Length
				());
			int size = children.Count;
			if (size > 1)
			{
				// otherwise all ancestors have been created
				IList<string> directories = children.SubList(0, size - 1);
				INode parentINode = existing.GetLastINode();
				// Ensure that the user can traversal the path by adding implicit
				// u+wx permission to all ancestor directories
				existing = CreateChildrenDirectories(fsd, existing, directories, AddImplicitUwx(parentINode
					.GetPermissionStatus(), permission));
				if (existing == null)
				{
					return null;
				}
			}
			return new AbstractMap.SimpleImmutableEntry<INodesInPath, string>(existing, last);
		}

		/// <summary>
		/// Create the directory
		/// <c>parent</c>
		/// /
		/// <paramref name="children"/>
		/// and all ancestors
		/// along the path.
		/// </summary>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="existing">
		/// The INodesInPath instance containing all the existing
		/// ancestral INodes
		/// </param>
		/// <param name="children">
		/// The relative path from the parent towards children,
		/// starting with "/"
		/// </param>
		/// <param name="perm">
		/// the permission of the directory. Note that all ancestors
		/// created along the path has implicit
		/// <c>u+wx</c>
		/// permissions.
		/// </param>
		/// <returns>
		/// 
		/// <see cref="INodesInPath"/>
		/// which contains all inodes to the
		/// target directory, After the execution parentPath points to the path of
		/// the returned INodesInPath. The function return null if the operation has
		/// failed.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static INodesInPath CreateChildrenDirectories(FSDirectory fsd, INodesInPath
			 existing, IList<string> children, PermissionStatus perm)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			foreach (string component in children)
			{
				existing = CreateSingleDirectory(fsd, existing, component, perm);
				if (existing == null)
				{
					return null;
				}
			}
			return existing;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		internal static void MkdirForEditLog(FSDirectory fsd, long inodeId, string src, PermissionStatus
			 permissions, IList<AclEntry> aclEntries, long timestamp)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath iip = fsd.GetINodesInPath(src, false);
			byte[] localName = iip.GetLastLocalName();
			INodesInPath existing = iip.GetParentINodesInPath();
			Preconditions.CheckState(existing.GetLastINode() != null);
			UnprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries, timestamp
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodesInPath CreateSingleDirectory(FSDirectory fsd, INodesInPath existing
			, string localName, PermissionStatus perm)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			existing = UnprotectedMkdir(fsd, fsd.AllocateNewInodeId(), existing, Sharpen.Runtime.GetBytesForString
				(localName, Charsets.Utf8), perm, null, Time.Now());
			if (existing == null)
			{
				return null;
			}
			INode newNode = existing.GetLastINode();
			// Directory creation also count towards FilesCreated
			// to match count of FilesDeleted metric.
			NameNode.GetNameNodeMetrics().IncrFilesCreated();
			string cur = existing.GetPath();
			fsd.GetEditLog().LogMkDir(cur, newNode);
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("mkdirs: created directory " + cur);
			}
			return existing;
		}

		private static PermissionStatus AddImplicitUwx(PermissionStatus parentPerm, PermissionStatus
			 perm)
		{
			FsPermission p = parentPerm.GetPermission();
			FsPermission ancestorPerm = new FsPermission(p.GetUserAction().Or(FsAction.WriteExecute
				), p.GetGroupAction(), p.GetOtherAction());
			return new PermissionStatus(perm.GetUserName(), perm.GetGroupName(), ancestorPerm
				);
		}

		/// <summary>create a directory at path specified by parent</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		private static INodesInPath UnprotectedMkdir(FSDirectory fsd, long inodeId, INodesInPath
			 parent, byte[] name, PermissionStatus permission, IList<AclEntry> aclEntries, long
			 timestamp)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			System.Diagnostics.Debug.Assert(parent.GetLastINode() != null);
			if (!parent.GetLastINode().IsDirectory())
			{
				throw new FileAlreadyExistsException("Parent path is not a directory: " + parent.
					GetPath() + " " + DFSUtil.Bytes2String(name));
			}
			INodeDirectory dir = new INodeDirectory(inodeId, name, permission, timestamp);
			INodesInPath iip = fsd.AddLastINode(parent, dir, true);
			if (iip != null && aclEntries != null)
			{
				AclStorage.UpdateINodeAcl(dir, aclEntries, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
			}
			return iip;
		}
	}
}
