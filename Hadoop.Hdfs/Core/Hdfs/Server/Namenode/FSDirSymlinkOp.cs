using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirSymlinkOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus CreateSymlinkInt(FSNamesystem fsn, string target, 
			string linkArg, PermissionStatus dirPerms, bool createParent, bool logRetryCache
			)
		{
			FSDirectory fsd = fsn.GetFSDirectory();
			string link = linkArg;
			if (!DFSUtil.IsValidName(link))
			{
				throw new InvalidPathException("Invalid link name: " + link);
			}
			if (FSDirectory.IsReservedName(target) || target.IsEmpty())
			{
				throw new InvalidPathException("Invalid target name: " + target);
			}
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.createSymlink: target=" + target +
					 " link=" + link);
			}
			FSPermissionChecker pc = fsn.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(link);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				link = fsd.ResolvePath(pc, link, pathComponents);
				iip = fsd.GetINodesInPath4Write(link, false);
				if (!createParent)
				{
					fsd.VerifyParentDir(iip, link);
				}
				if (!fsd.IsValidToCreate(link, iip))
				{
					throw new IOException("failed to create link " + link + " either because the filename is invalid or the file exists"
						);
				}
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckAncestorAccess(pc, iip, FsAction.Write);
				}
				// validate that we have enough inodes.
				fsn.CheckFsObjectLimit();
				// add symbolic link to namespace
				AddSymlink(fsd, link, iip, target, dirPerms, createParent, logRetryCache);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			NameNode.GetNameNodeMetrics().IncrCreateSymlinkOps();
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal static INodeSymlink UnprotectedAddSymlink(FSDirectory fsd, INodesInPath 
			iip, byte[] localName, long id, string target, long mtime, long atime, PermissionStatus
			 perm)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime, target);
			symlink.SetLocalName(localName);
			return fsd.AddINode(iip, symlink) != null ? symlink : null;
		}

		/// <summary>Add the given symbolic link to the fs.</summary>
		/// <remarks>Add the given symbolic link to the fs. Record it in the edits log.</remarks>
		/// <exception cref="System.IO.IOException"/>
		private static INodeSymlink AddSymlink(FSDirectory fsd, string path, INodesInPath
			 iip, string target, PermissionStatus dirPerms, bool createParent, bool logRetryCache
			)
		{
			long mtime = Time.Now();
			byte[] localName = iip.GetLastLocalName();
			if (createParent)
			{
				KeyValuePair<INodesInPath, string> e = FSDirMkdirOp.CreateAncestorDirectories(fsd
					, iip, dirPerms);
				if (e == null)
				{
					return null;
				}
				iip = INodesInPath.Append(e.Key, null, localName);
			}
			string userName = dirPerms.GetUserName();
			long id = fsd.AllocateNewInodeId();
			PermissionStatus perm = new PermissionStatus(userName, null, FsPermission.GetDefault
				());
			INodeSymlink newNode = UnprotectedAddSymlink(fsd, iip.GetExistingINodes(), localName
				, id, target, mtime, mtime, perm);
			if (newNode == null)
			{
				NameNode.stateChangeLog.Info("addSymlink: failed to add " + path);
				return null;
			}
			fsd.GetEditLog().LogSymlink(path, target, mtime, mtime, newNode, logRetryCache);
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("addSymlink: " + path + " is added");
			}
			return newNode;
		}
	}
}
