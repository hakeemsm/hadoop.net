using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>InodeTree implements a mount-table as a tree of inodes.</summary>
	/// <remarks>
	/// InodeTree implements a mount-table as a tree of inodes.
	/// It is used to implement ViewFs and ViewFileSystem.
	/// In order to use it the caller must subclass it and implement
	/// the abstract methods
	/// <see cref="InodeTree{T}.getTargetFileSystem(INodeDir{T})"/>
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

		internal static readonly org.apache.hadoop.fs.Path SlashPath = new org.apache.hadoop.fs.Path
			("/");

		internal readonly org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> root;

		internal readonly string homedirPrefix;

		internal System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
			<T>> mountPoints = new System.Collections.Generic.List<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
			<T>>();

		internal class MountPoint<T>
		{
			internal string src;

			internal org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<T> target;

			internal MountPoint(string srcPath, org.apache.hadoop.fs.viewfs.InodeTree.INodeLink
				<T> mountLink)
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
		internal static string[] breakIntoPathComponents(string path)
		{
			return path == null ? null : path.split(org.apache.hadoop.fs.Path.SEPARATOR);
		}

		/// <summary>Internal class for inode tree</summary>
		/// <?/>
		internal abstract class INode<T>
		{
			internal readonly string fullPath;

			public INode(string pathToNode, org.apache.hadoop.security.UserGroupInformation aUgi
				)
			{
				// the full path to the root
				fullPath = pathToNode;
			}
		}

		/// <summary>Internal class to represent an internal dir of the mount table</summary>
		/// <?/>
		internal class INodeDir<T> : org.apache.hadoop.fs.viewfs.InodeTree.INode<T>
		{
			internal readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.fs.viewfs.InodeTree.INode
				<T>> children = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.fs.viewfs.InodeTree.INode
				<T>>();

			internal T InodeDirFs = null;

			internal bool isRoot = false;

			internal INodeDir(string pathToNode, org.apache.hadoop.security.UserGroupInformation
				 aUgi)
				: base(pathToNode, aUgi)
			{
			}

			// file system of this internal directory of mountT
			/// <exception cref="java.io.FileNotFoundException"/>
			internal virtual org.apache.hadoop.fs.viewfs.InodeTree.INode<T> resolve(string pathComponent
				)
			{
				org.apache.hadoop.fs.viewfs.InodeTree.INode<T> result = resolveInternal(pathComponent
					);
				if (result == null)
				{
					throw new java.io.FileNotFoundException();
				}
				return result;
			}

			/// <exception cref="java.io.FileNotFoundException"/>
			internal virtual org.apache.hadoop.fs.viewfs.InodeTree.INode<T> resolveInternal(string
				 pathComponent)
			{
				return children[pathComponent];
			}

			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			internal virtual org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> addDir(string 
				pathComponent, org.apache.hadoop.security.UserGroupInformation aUgi)
			{
				if (children.Contains(pathComponent))
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException();
				}
				org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> newDir = new org.apache.hadoop.fs.viewfs.InodeTree.INodeDir
					<T>(fullPath + (isRoot ? string.Empty : "/") + pathComponent, aUgi);
				children[pathComponent] = newDir;
				return newDir;
			}

			/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
			internal virtual void addLink(string pathComponent, org.apache.hadoop.fs.viewfs.InodeTree.INodeLink
				<T> link)
			{
				if (children.Contains(pathComponent))
				{
					throw new org.apache.hadoop.fs.FileAlreadyExistsException();
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
		internal class INodeLink<T> : org.apache.hadoop.fs.viewfs.InodeTree.INode<T>
		{
			internal readonly bool isMergeLink;

			internal readonly java.net.URI[] targetDirLinkList;

			internal readonly T targetFileSystem;

			/// <summary>Construct a mergeLink</summary>
			internal INodeLink(string pathToNode, org.apache.hadoop.security.UserGroupInformation
				 aUgi, T targetMergeFs, java.net.URI[] aTargetDirLinkList)
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
			internal INodeLink(string pathToNode, org.apache.hadoop.security.UserGroupInformation
				 aUgi, T targetFs, java.net.URI aTargetDirLink)
				: base(pathToNode, aUgi)
			{
				targetFileSystem = targetFs;
				targetDirLinkList = new java.net.URI[1];
				targetDirLinkList[0] = aTargetDirLink;
				isMergeLink = false;
			}

			/// <summary>
			/// Get the target of the link
			/// If a merge link then it returned as "," separated URI list.
			/// </summary>
			internal virtual org.apache.hadoop.fs.Path getTargetLink()
			{
				// is merge link - use "," as separator between the merged URIs
				//String result = targetDirLinkList[0].toString();
				java.lang.StringBuilder result = new java.lang.StringBuilder(targetDirLinkList[0]
					.ToString());
				for (int i = 1; i < targetDirLinkList.Length; ++i)
				{
					result.Append(',').Append(targetDirLinkList[i].ToString());
				}
				return new org.apache.hadoop.fs.Path(result.ToString());
			}
		}

		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		private void createLink(string src, string target, bool isLinkMerge, org.apache.hadoop.security.UserGroupInformation
			 aUgi)
		{
			// Validate that src is valid absolute path
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(src);
			if (!srcPath.isAbsoluteAndSchemeAuthorityNull())
			{
				throw new System.IO.IOException("ViewFs:Non absolute mount name in config:" + src
					);
			}
			string[] srcPaths = breakIntoPathComponents(src);
			org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> curInode = root;
			int i;
			// Ignore first initial slash, process all except last component
			for (i = 1; i < srcPaths.Length - 1; i++)
			{
				string iPath = srcPaths[i];
				org.apache.hadoop.fs.viewfs.InodeTree.INode<T> nextInode = curInode.resolveInternal
					(iPath);
				if (nextInode == null)
				{
					org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> newDir = curInode.addDir(iPath, 
						aUgi);
					newDir.InodeDirFs = getTargetFileSystem(newDir);
					nextInode = newDir;
				}
				if (nextInode is org.apache.hadoop.fs.viewfs.InodeTree.INodeLink)
				{
					// Error - expected a dir but got a link
					throw new org.apache.hadoop.fs.FileAlreadyExistsException("Path " + nextInode.fullPath
						 + " already exists as link");
				}
				else
				{
					System.Diagnostics.Debug.Assert((nextInode is org.apache.hadoop.fs.viewfs.InodeTree.INodeDir
						));
					curInode = (org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T>)nextInode;
				}
			}
			// Now process the last component
			// Add the link in 2 cases: does not exist or a link exists
			string iPath_1 = srcPaths[i];
			// last component
			if (curInode.resolveInternal(iPath_1) != null)
			{
				//  directory/link already exists
				java.lang.StringBuilder strB = new java.lang.StringBuilder(srcPaths[0]);
				for (int j = 1; j <= i; ++j)
				{
					strB.Append('/').Append(srcPaths[j]);
				}
				throw new org.apache.hadoop.fs.FileAlreadyExistsException("Path " + strB + " already exists as dir; cannot create link here"
					);
			}
			org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<T> newLink;
			string fullPath = curInode.fullPath + (curInode == root ? string.Empty : "/") + iPath_1;
			if (isLinkMerge)
			{
				// Target is list of URIs
				string[] targetsList = org.apache.hadoop.util.StringUtils.getStrings(target);
				java.net.URI[] targetsListURI = new java.net.URI[targetsList.Length];
				int k = 0;
				foreach (string itarget in targetsList)
				{
					targetsListURI[k++] = new java.net.URI(itarget);
				}
				newLink = new org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<T>(fullPath, aUgi, 
					getTargetFileSystem(targetsListURI), targetsListURI);
			}
			else
			{
				newLink = new org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<T>(fullPath, aUgi, 
					getTargetFileSystem(new java.net.URI(target)), new java.net.URI(target));
			}
			curInode.addLink(iPath_1, newLink);
			mountPoints.add(new org.apache.hadoop.fs.viewfs.InodeTree.MountPoint<T>(src, newLink
				));
		}

		/// <summary>
		/// The user of this class must subclass and implement the following
		/// 3 abstract methods.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		protected internal abstract T getTargetFileSystem(java.net.URI uri);

		/// <exception cref="java.net.URISyntaxException"/>
		protected internal abstract T getTargetFileSystem(org.apache.hadoop.fs.viewfs.InodeTree.INodeDir
			<T> dir);

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		protected internal abstract T getTargetFileSystem(java.net.URI[] mergeFsURIList);

		/// <summary>Create Inode Tree from the specified mount-table specified in Config</summary>
		/// <param name="config">
		/// - the mount table keys are prefixed with
		/// FsConstants.CONFIG_VIEWFS_PREFIX
		/// </param>
		/// <param name="viewName">- the name of the mount table - if null use defaultMT name
		/// 	</param>
		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="org.apache.hadoop.fs.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal InodeTree(org.apache.hadoop.conf.Configuration config, string 
			viewName)
		{
			string vName = viewName;
			if (vName == null)
			{
				vName = org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
			}
			homedirPrefix = org.apache.hadoop.fs.viewfs.ConfigUtil.getHomeDirValue(config, vName
				);
			root = new org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T>("/", org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser());
			root.InodeDirFs = getTargetFileSystem(root);
			root.isRoot = true;
			string mtPrefix = org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_PREFIX + "."
				 + vName + ".";
			string linkPrefix = org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_LINK + ".";
			string linkMergePrefix = org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_LINK_MERGE
				 + ".";
			bool gotMountTableEntry = false;
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			foreach (System.Collections.Generic.KeyValuePair<string, string> si in config)
			{
				string key = si.Key;
				if (key.StartsWith(mtPrefix))
				{
					gotMountTableEntry = true;
					bool isMergeLink = false;
					string src = Sharpen.Runtime.substring(key, mtPrefix.Length);
					if (src.StartsWith(linkPrefix))
					{
						src = Sharpen.Runtime.substring(src, linkPrefix.Length);
					}
					else
					{
						if (src.StartsWith(linkMergePrefix))
						{
							// A merge link
							isMergeLink = true;
							src = Sharpen.Runtime.substring(src, linkMergePrefix.Length);
						}
						else
						{
							if (src.StartsWith(org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_HOMEDIR))
							{
								// ignore - we set home dir from config
								continue;
							}
							else
							{
								throw new System.IO.IOException("ViewFs: Cannot initialize: Invalid entry in Mount table in config: "
									 + src);
							}
						}
					}
					string target = si.Value;
					// link or merge link
					createLink(src, target, isMergeLink, ugi);
				}
			}
			if (!gotMountTableEntry)
			{
				throw new System.IO.IOException("ViewFs: Cannot initialize: Empty Mount table in config for "
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
			internal readonly org.apache.hadoop.fs.viewfs.InodeTree.ResultKind kind;

			internal readonly T targetFileSystem;

			internal readonly string resolvedPath;

			internal readonly org.apache.hadoop.fs.Path remainingPath;

			internal ResolveResult(org.apache.hadoop.fs.viewfs.InodeTree.ResultKind k, T targetFs
				, string resolveP, org.apache.hadoop.fs.Path remainingP)
			{
				// to resolve in the target FileSystem
				kind = k;
				targetFileSystem = targetFs;
				resolvedPath = resolveP;
				remainingPath = remainingP;
			}

			// isInternalDir of path resolution completed within the mount table 
			internal virtual bool isInternalDir()
			{
				return (kind == org.apache.hadoop.fs.viewfs.InodeTree.ResultKind.isInternalDir);
			}
		}

		/// <summary>Resolve the pathname p relative to root InodeDir</summary>
		/// <param name="p">- inout path</param>
		/// <param name="resolveLastComponent"></param>
		/// <returns>ResolveResult which allows further resolution of the remaining path</returns>
		/// <exception cref="java.io.FileNotFoundException"/>
		internal virtual org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<T> resolve(string
			 p, bool resolveLastComponent)
		{
			// TO DO: - more efficient to not split the path, but simply compare
			string[] path = breakIntoPathComponents(p);
			if (path.Length <= 1)
			{
				// special case for when path is "/"
				org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<T> res = new org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
					<T>(org.apache.hadoop.fs.viewfs.InodeTree.ResultKind.isInternalDir, root.InodeDirFs
					, root.fullPath, SlashPath);
				return res;
			}
			org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T> curInode = root;
			int i;
			// ignore first slash
			for (i = 1; i < path.Length - (resolveLastComponent ? 0 : 1); i++)
			{
				org.apache.hadoop.fs.viewfs.InodeTree.INode<T> nextInode = curInode.resolveInternal
					(path[i]);
				if (nextInode == null)
				{
					java.lang.StringBuilder failedAt = new java.lang.StringBuilder(path[0]);
					for (int j = 1; j <= i; ++j)
					{
						failedAt.Append('/').Append(path[j]);
					}
					throw (new java.io.FileNotFoundException(failedAt.ToString()));
				}
				if (nextInode is org.apache.hadoop.fs.viewfs.InodeTree.INodeLink)
				{
					org.apache.hadoop.fs.viewfs.InodeTree.INodeLink<T> link = (org.apache.hadoop.fs.viewfs.InodeTree.INodeLink
						<T>)nextInode;
					org.apache.hadoop.fs.Path remainingPath;
					if (i >= path.Length - 1)
					{
						remainingPath = SlashPath;
					}
					else
					{
						java.lang.StringBuilder remainingPathStr = new java.lang.StringBuilder("/" + path
							[i + 1]);
						for (int j = i + 2; j < path.Length; ++j)
						{
							remainingPathStr.Append('/').Append(path[j]);
						}
						remainingPath = new org.apache.hadoop.fs.Path(remainingPathStr.ToString());
					}
					org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<T> res = new org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
						<T>(org.apache.hadoop.fs.viewfs.InodeTree.ResultKind.isExternalDir, link.targetFileSystem
						, nextInode.fullPath, remainingPath);
					return res;
				}
				else
				{
					if (nextInode is org.apache.hadoop.fs.viewfs.InodeTree.INodeDir)
					{
						curInode = (org.apache.hadoop.fs.viewfs.InodeTree.INodeDir<T>)nextInode;
					}
				}
			}
			// We have resolved to an internal dir in mount table.
			org.apache.hadoop.fs.Path remainingPath_1;
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
				java.lang.StringBuilder remainingPathStr = new java.lang.StringBuilder("/" + path
					[i]);
				for (int j = i + 1; j < path.Length; ++j)
				{
					remainingPathStr.Append('/').Append(path[j]);
				}
				remainingPath_1 = new org.apache.hadoop.fs.Path(remainingPathStr.ToString());
			}
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<T> res_1 = new org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult
				<T>(org.apache.hadoop.fs.viewfs.InodeTree.ResultKind.isInternalDir, curInode.InodeDirFs
				, curInode.fullPath, remainingPath_1);
			return res_1;
		}

		internal virtual System.Collections.Generic.IList<org.apache.hadoop.fs.viewfs.InodeTree.MountPoint
			<T>> getMountPoints()
		{
			return mountPoints;
		}

		/// <returns>
		/// home dir value from mount table; null if no config value
		/// was found.
		/// </returns>
		internal virtual string getHomeDirPrefixValue()
		{
			return homedirPrefix;
		}
	}
}
