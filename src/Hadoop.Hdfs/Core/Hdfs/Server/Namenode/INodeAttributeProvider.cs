using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public abstract class INodeAttributeProvider
	{
		/// <summary>
		/// The AccessControlEnforcer allows implementations to override the
		/// default File System permission checking logic enforced on a file system
		/// object
		/// </summary>
		public interface AccessControlEnforcer
		{
			/// <summary>Checks permission on a file system object.</summary>
			/// <remarks>
			/// Checks permission on a file system object. Has to throw an Exception
			/// if the filesystem object is not accessessible by the calling Ugi.
			/// </remarks>
			/// <param name="fsOwner">Filesystem owner (The Namenode user)</param>
			/// <param name="supergroup">super user geoup</param>
			/// <param name="callerUgi">UserGroupInformation of the caller</param>
			/// <param name="inodeAttrs">
			/// Array of INode attributes for each path element in the
			/// the path
			/// </param>
			/// <param name="inodes">Array of INodes for each path element in the path</param>
			/// <param name="pathByNameArr">Array of byte arrays of the LocalName</param>
			/// <param name="snapshotId">the snapshotId of the requested path</param>
			/// <param name="path">Path String</param>
			/// <param name="ancestorIndex">Index of ancestor</param>
			/// <param name="doCheckOwner">perform ownership check</param>
			/// <param name="ancestorAccess">The access required by the ancestor of the path.</param>
			/// <param name="parentAccess">The access required by the parent of the path.</param>
			/// <param name="access">The access required by the path.</param>
			/// <param name="subAccess">
			/// If path is a directory, It is the access required of
			/// the path and all the sub-directories. If path is not a
			/// directory, there should ideally be no effect.
			/// </param>
			/// <param name="ignoreEmptyDir">Ignore permission checking for empty directory?</param>
			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			void CheckPermission(string fsOwner, string supergroup, UserGroupInformation callerUgi
				, INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr, int snapshotId
				, string path, int ancestorIndex, bool doCheckOwner, FsAction ancestorAccess, FsAction
				 parentAccess, FsAction access, FsAction subAccess, bool ignoreEmptyDir);
		}

		/// <summary>Initialize the provider.</summary>
		/// <remarks>
		/// Initialize the provider. This method is called at NameNode startup
		/// time.
		/// </remarks>
		public abstract void Start();

		/// <summary>Shutdown the provider.</summary>
		/// <remarks>Shutdown the provider. This method is called at NameNode shutdown time.</remarks>
		public abstract void Stop();

		[VisibleForTesting]
		internal virtual string[] GetPathElements(string path)
		{
			path = path.Trim();
			if (path[0] != Path.SeparatorChar)
			{
				throw new ArgumentException("It must be an absolute path: " + path);
			}
			int numOfElements = StringUtils.CountMatches(path, Path.Separator);
			if (path.Length > 1 && path.EndsWith(Path.Separator))
			{
				numOfElements--;
			}
			string[] pathElements = new string[numOfElements];
			int elementIdx = 0;
			int idx = 0;
			int found = path.IndexOf(Path.SeparatorChar, idx);
			while (found > -1)
			{
				if (found > idx)
				{
					pathElements[elementIdx++] = Sharpen.Runtime.Substring(path, idx, found);
				}
				idx = found + 1;
				found = path.IndexOf(Path.SeparatorChar, idx);
			}
			if (idx < path.Length)
			{
				pathElements[elementIdx] = Sharpen.Runtime.Substring(path, idx);
			}
			return pathElements;
		}

		public virtual INodeAttributes GetAttributes(string fullPath, INodeAttributes inode
			)
		{
			return GetAttributes(GetPathElements(fullPath), inode);
		}

		public abstract INodeAttributes GetAttributes(string[] pathElements, INodeAttributes
			 inode);

		/// <summary>
		/// Can be over-ridden by implementations to provide a custom Access Control
		/// Enforcer that can provide an alternate implementation of the
		/// default permission checking logic.
		/// </summary>
		/// <param name="defaultEnforcer">The Default AccessControlEnforcer</param>
		/// <returns>The AccessControlEnforcer to use</returns>
		public virtual INodeAttributeProvider.AccessControlEnforcer GetExternalAccessControlEnforcer
			(INodeAttributeProvider.AccessControlEnforcer defaultEnforcer)
		{
			return defaultEnforcer;
		}
	}
}
