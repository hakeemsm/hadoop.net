using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>InodeTree implements a mount-table as a tree of inodes.</summary>
	/// <remarks>
	/// InodeTree implements a mount-table as a tree of inodes.
	/// It is used to implement ViewFs and ViewFileSystem.
	/// In order to use it the caller must subclass it and implement
	/// the abstract methods
	/// <see cref="InodeTree{T}.GetTargetFileSystem(INodeDir{T})"/>
	/// , etc.
	/// The mountable is initialized from the config variables as
	/// specified in
	/// <see cref="ViewFs"/>
	/// </remarks>
	/// <?/>
	internal abstract class InodeTree<T>
	{
		internal enum ResultKind
		{
			isInternalDir,
			isExternalDir
		}

		internal static readonly Path SlashPath = new Path("/");

		internal readonly InodeTree.INodeDir<T> root;

		internal readonly string homedirPrefix;

		internal IList<InodeTree.MountPoint<T>> mountPoints = new AList<InodeTree.MountPoint
			<T>>();

		internal class MountPoint<T>
		{
			internal string src;

			internal InodeTree.INodeLink<T> target;

			internal MountPoint(string srcPath, InodeTree.INodeLink<T> mountLink)
			{
				// the root of the mount table
				// the homedir config value for this mount table
				src = srcPath;
				target = mountLink;
			}
		}

		/// <summary>Breaks file path into component names.</summary>
		/// <param name="path"/>
		/// <returns>array of names component names</returns>
		internal static string[] BreakIntoPathComponents(string path)
		{
			return path == null ? null : path.Split(Path.Separator);
		}

		/// <summary>Internal class for inode tree</summary>
		/// <?/>
		internal abstract class INode<T>
		{
			internal readonly string fullPath;

			public INode(string pathToNode, UserGroupInformation aUgi)
			{
				// the full path to the root
				fullPath = pathToNode;
			}
		}

		/// <summary>Internal class to represent an internal dir of the mount table</summary>
		/// <?/>
		internal class INodeDir<T> : InodeTree.INode<T>
		{
			internal readonly IDictionary<string, InodeTree.INode<T>> children = new Dictionary
				<string, InodeTree.INode<T>>();

			internal T InodeDirFs = null;

			internal bool isRoot = false;

			internal INodeDir(string pathToNode, UserGroupInformation aUgi)
				: base(pathToNode, aUgi)
			{
			}

			// file system of this internal directory of mountT
			/// <exception cref="System.IO.FileNotFoundException"/>
			internal virtual InodeTree.INode<T> Resolve(string pathComponent)
			{
				InodeTree.INode<T> result = ResolveInternal(pathComponent);
				if (result == null)
				{
					throw new FileNotFoundException();
				}
				return result;
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			internal virtual InodeTree.INode<T> ResolveInternal(string pathComponent)
			{
				return children[pathComponent];
			}

			/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
			internal virtual InodeTree.INodeDir<T> AddDir(string pathComponent, UserGroupInformation
				 aUgi)
			{
				if (children.Contains(pathComponent))
				{
					throw new FileAlreadyExistsException();
				}
				InodeTree.INodeDir<T> newDir = new InodeTree.INodeDir<T>(fullPath + (isRoot ? string.Empty
					 : "/") + pathComponent, aUgi);
				children[pathComponent] = newDir;
				return newDir;
			}

			/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
			internal virtual void AddLink(string pathComponent, InodeTree.INodeLink<T> link)
			{
				if (children.Contains(pathComponent))
				{
					throw new FileAlreadyExistsException();
				}
				children[pathComponent] = link;
			}
		}

		/// <summary>
		/// In internal class to represent a mount link
		/// A mount link can be single dir link or a merge dir link.
		/// </summary>
		/// <remarks>
		/// In internal class to represent a mount link
		/// A mount link can be single dir link or a merge dir link.
		/// A merge dir link is  a merge (junction) of links to dirs:
		/// example : &lt;merge of 2 dirs
		/// /users -&gt; hdfs:nn1//users
		/// /users -&gt; hdfs:nn2//users
		/// For a merge, each target is checked to be dir when created but if target
		/// is changed later it is then ignored (a dir with null entries)
		/// </remarks>
		internal class INodeLink<T> : InodeTree.INode<T>
		{
			internal readonly bool isMergeLink;

			internal readonly URI[] targetDirLinkList;

			internal readonly T targetFileSystem;

			/// <summary>Construct a mergeLink</summary>
			internal INodeLink(string pathToNode, UserGroupInformation aUgi, T targetMergeFs, 
				URI[] aTargetDirLinkList)
				: base(pathToNode, aUgi)
			{
				// true if MergeLink
				// file system object created from the link.
				targetFileSystem = targetMergeFs;
				targetDirLinkList = aTargetDirLinkList;
				isMergeLink = true;
			}

			/// <summary>Construct a simple link (i.e.</summary>
			/// <remarks>Construct a simple link (i.e. not a mergeLink)</remarks>
			internal INodeLink(string pathToNode, UserGroupInformation aUgi, T targetFs, URI 
				aTargetDirLink)
				: base(pathToNode, aUgi)
			{
				targetFileSystem = targetFs;
				targetDirLinkList = new URI[1];
				targetDirLinkList[0] = aTargetDirLink;
				isMergeLink = false;
			}

			/// <summary>
			/// Get the target of the link
			/// If a merge link then it returned as "," separated URI list.
			/// </summary>
			internal virtual Path GetTargetLink()
			{
				// is merge link - use "," as separator between the merged URIs
				//String result = targetDirLinkList[0].toString();
				StringBuilder result = new StringBuilder(targetDirLinkList[0].ToString());
				for (int i = 1; i < targetDirLinkList.Length; ++i)
				{
					result.Append(',').Append(targetDirLinkList[i].ToString());
				}
				return new Path(result.ToString());
			}
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		private void CreateLink(string src, string target, bool isLinkMerge, UserGroupInformation
			 aUgi)
		{
			// Validate that src is valid absolute path
			Path srcPath = new Path(src);
			if (!srcPath.IsAbsoluteAndSchemeAuthorityNull())
			{
				throw new IOException("ViewFs:Non absolute mount name in config:" + src);
			}
			string[] srcPaths = BreakIntoPathComponents(src);
			InodeTree.INodeDir<T> curInode = root;
			int i;
			// Ignore first initial slash, process all except last component
			for (i = 1; i < srcPaths.Length - 1; i++)
			{
				string iPath = srcPaths[i];
				InodeTree.INode<T> nextInode = curInode.ResolveInternal(iPath);
				if (nextInode == null)
				{
					InodeTree.INodeDir<T> newDir = curInode.AddDir(iPath, aUgi);
					newDir.InodeDirFs = GetTargetFileSystem(newDir);
					nextInode = newDir;
				}
				if (nextInode is InodeTree.INodeLink)
				{
					// Error - expected a dir but got a link
					throw new FileAlreadyExistsException("Path " + nextInode.fullPath + " already exists as link"
						);
				}
				else
				{
					System.Diagnostics.Debug.Assert((nextInode is InodeTree.INodeDir));
					curInode = (InodeTree.INodeDir<T>)nextInode;
				}
			}
			// Now process the last component
			// Add the link in 2 cases: does not exist or a link exists
			string iPath_1 = srcPaths[i];
			// last component
			if (curInode.ResolveInternal(iPath_1) != null)
			{
				//  directory/link already exists
				StringBuilder strB = new StringBuilder(srcPaths[0]);
				for (int j = 1; j <= i; ++j)
				{
					strB.Append('/').Append(srcPaths[j]);
				}
				throw new FileAlreadyExistsException("Path " + strB + " already exists as dir; cannot create link here"
					);
			}
			InodeTree.INodeLink<T> newLink;
			string fullPath = curInode.fullPath + (curInode == root ? string.Empty : "/") + iPath_1;
			if (isLinkMerge)
			{
				// Target is list of URIs
				string[] targetsList = StringUtils.GetStrings(target);
				URI[] targetsListURI = new URI[targetsList.Length];
				int k = 0;
				foreach (string itarget in targetsList)
				{
					targetsListURI[k++] = new URI(itarget);
				}
				newLink = new InodeTree.INodeLink<T>(fullPath, aUgi, GetTargetFileSystem(targetsListURI
					), targetsListURI);
			}
			else
			{
				newLink = new InodeTree.INodeLink<T>(fullPath, aUgi, GetTargetFileSystem(new URI(
					target)), new URI(target));
			}
			curInode.AddLink(iPath_1, newLink);
			mountPoints.AddItem(new InodeTree.MountPoint<T>(src, newLink));
		}

		/// <summary>
		/// The user of this class must subclass and implement the following
		/// 3 abstract methods.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal abstract T GetTargetFileSystem(URI uri);

		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal abstract T GetTargetFileSystem(InodeTree.INodeDir<T> dir);

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		protected internal abstract T GetTargetFileSystem(URI[] mergeFsURIList);

		/// <summary>Create Inode Tree from the specified mount-table specified in Config</summary>
		/// <param name="config">
		/// - the mount table keys are prefixed with
		/// FsConstants.CONFIG_VIEWFS_PREFIX
		/// </param>
		/// <param name="viewName">- the name of the mount table - if null use defaultMT name
		/// 	</param>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal InodeTree(Configuration config, string viewName)
		{
			string vName = viewName;
			if (vName == null)
			{
				vName = Constants.ConfigViewfsDefaultMountTable;
			}
			homedirPrefix = ConfigUtil.GetHomeDirValue(config, vName);
			root = new InodeTree.INodeDir<T>("/", UserGroupInformation.GetCurrentUser());
			root.InodeDirFs = GetTargetFileSystem(root);
			root.isRoot = true;
			string mtPrefix = Constants.ConfigViewfsPrefix + "." + vName + ".";
			string linkPrefix = Constants.ConfigViewfsLink + ".";
			string linkMergePrefix = Constants.ConfigViewfsLinkMerge + ".";
			bool gotMountTableEntry = false;
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			foreach (KeyValuePair<string, string> si in config)
			{
				string key = si.Key;
				if (key.StartsWith(mtPrefix))
				{
					gotMountTableEntry = true;
					bool isMergeLink = false;
					string src = Sharpen.Runtime.Substring(key, mtPrefix.Length);
					if (src.StartsWith(linkPrefix))
					{
						src = Sharpen.Runtime.Substring(src, linkPrefix.Length);
					}
					else
					{
						if (src.StartsWith(linkMergePrefix))
						{
							// A merge link
							isMergeLink = true;
							src = Sharpen.Runtime.Substring(src, linkMergePrefix.Length);
						}
						else
						{
							if (src.StartsWith(Constants.ConfigViewfsHomedir))
							{
								// ignore - we set home dir from config
								continue;
							}
							else
							{
								throw new IOException("ViewFs: Cannot initialize: Invalid entry in Mount table in config: "
									 + src);
							}
						}
					}
					string target = si.Value;
					// link or merge link
					CreateLink(src, target, isMergeLink, ugi);
				}
			}
			if (!gotMountTableEntry)
			{
				throw new IOException("ViewFs: Cannot initialize: Empty Mount table in config for "
					 + vName == null ? "viewfs:///" : ("viewfs://" + vName + "/"));
			}
		}

		/// <summary>Resolve returns ResolveResult.</summary>
		/// <remarks>
		/// Resolve returns ResolveResult.
		/// The caller can continue the resolution of the remainingPath
		/// in the targetFileSystem.
		/// If the input pathname leads to link to another file system then
		/// the targetFileSystem is the one denoted by the link (except it is
		/// file system chrooted to link target.
		/// If the input pathname leads to an internal mount-table entry then
		/// the target file system is one that represents the internal inode.
		/// </remarks>
		internal class ResolveResult<T>
		{
			internal readonly InodeTree.ResultKind kind;

			internal readonly T targetFileSystem;

			internal readonly string resolvedPath;

			internal readonly Path remainingPath;

			internal ResolveResult(InodeTree.ResultKind k, T targetFs, string resolveP, Path 
				remainingP)
			{
				// to resolve in the target FileSystem
				kind = k;
				targetFileSystem = targetFs;
				resolvedPath = resolveP;
				remainingPath = remainingP;
			}

			// isInternalDir of path resolution completed within the mount table 
			internal virtual bool IsInternalDir()
			{
				return (kind == InodeTree.ResultKind.isInternalDir);
			}
		}

		/// <summary>Resolve the pathname p relative to root InodeDir</summary>
		/// <param name="p">- inout path</param>
		/// <param name="resolveLastComponent"></param>
		/// <returns>ResolveResult which allows further resolution of the remaining path</returns>
		/// <exception cref="System.IO.FileNotFoundException"/>
		internal virtual InodeTree.ResolveResult<T> Resolve(string p, bool resolveLastComponent
			)
		{
			// TO DO: - more efficient to not split the path, but simply compare
			string[] path = BreakIntoPathComponents(p);
			if (path.Length <= 1)
			{
				// special case for when path is "/"
				InodeTree.ResolveResult<T> res = new InodeTree.ResolveResult<T>(InodeTree.ResultKind
					.isInternalDir, root.InodeDirFs, root.fullPath, SlashPath);
				return res;
			}
			InodeTree.INodeDir<T> curInode = root;
			int i;
			// ignore first slash
			for (i = 1; i < path.Length - (resolveLastComponent ? 0 : 1); i++)
			{
				InodeTree.INode<T> nextInode = curInode.ResolveInternal(path[i]);
				if (nextInode == null)
				{
					StringBuilder failedAt = new StringBuilder(path[0]);
					for (int j = 1; j <= i; ++j)
					{
						failedAt.Append('/').Append(path[j]);
					}
					throw (new FileNotFoundException(failedAt.ToString()));
				}
				if (nextInode is InodeTree.INodeLink)
				{
					InodeTree.INodeLink<T> link = (InodeTree.INodeLink<T>)nextInode;
					Path remainingPath;
					if (i >= path.Length - 1)
					{
						remainingPath = SlashPath;
					}
					else
					{
						StringBuilder remainingPathStr = new StringBuilder("/" + path[i + 1]);
						for (int j = i + 2; j < path.Length; ++j)
						{
							remainingPathStr.Append('/').Append(path[j]);
						}
						remainingPath = new Path(remainingPathStr.ToString());
					}
					InodeTree.ResolveResult<T> res = new InodeTree.ResolveResult<T>(InodeTree.ResultKind
						.isExternalDir, link.targetFileSystem, nextInode.fullPath, remainingPath);
					return res;
				}
				else
				{
					if (nextInode is InodeTree.INodeDir)
					{
						curInode = (InodeTree.INodeDir<T>)nextInode;
					}
				}
			}
			// We have resolved to an internal dir in mount table.
			Path remainingPath_1;
			if (resolveLastComponent)
			{
				remainingPath_1 = SlashPath;
			}
			else
			{
				// note we have taken care of when path is "/" above
				// for internal dirs rem-path does not start with / since the lookup
				// that follows will do a children.get(remaningPath) and will have to
				// strip-out the initial /
				StringBuilder remainingPathStr = new StringBuilder("/" + path[i]);
				for (int j = i + 1; j < path.Length; ++j)
				{
					remainingPathStr.Append('/').Append(path[j]);
				}
				remainingPath_1 = new Path(remainingPathStr.ToString());
			}
			InodeTree.ResolveResult<T> res_1 = new InodeTree.ResolveResult<T>(InodeTree.ResultKind
				.isInternalDir, curInode.InodeDirFs, curInode.fullPath, remainingPath_1);
			return res_1;
		}

		internal virtual IList<InodeTree.MountPoint<T>> GetMountPoints()
		{
			return mountPoints;
		}

		/// <returns>
		/// home dir value from mount table; null if no config value
		/// was found.
		/// </returns>
		internal virtual string GetHomeDirPrefixValue()
		{
			return homedirPrefix;
		}
	}
}
