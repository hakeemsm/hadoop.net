using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Contains INodes information resolved from a given path.</summary>
	public class INodesInPath
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath
			));

		/// <returns>
		/// true if path component is
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.DotSnapshotDir"/>
		/// </returns>
		private static bool IsDotSnapshotDir(byte[] pathComponent)
		{
			return pathComponent != null && Arrays.Equals(HdfsConstants.DotSnapshotDirBytes, 
				pathComponent);
		}

		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath FromINode(INode
			 inode)
		{
			int depth = 0;
			int index;
			INode tmp = inode;
			while (tmp != null)
			{
				depth++;
				tmp = tmp.GetParent();
			}
			byte[][] path = new byte[depth][];
			INode[] inodes = new INode[depth];
			tmp = inode;
			index = depth;
			while (tmp != null)
			{
				index--;
				path[index] = tmp.GetKey();
				inodes[index] = tmp;
				tmp = tmp.GetParent();
			}
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(inodes, path);
		}

		/// <summary>Given some components, create a path name.</summary>
		/// <param name="components">The path components</param>
		/// <param name="start">index</param>
		/// <param name="end">index</param>
		/// <returns>concatenated path</returns>
		private static string ConstructPath(byte[][] components, int start, int end)
		{
			StringBuilder buf = new StringBuilder();
			for (int i = start; i < end; i++)
			{
				buf.Append(DFSUtil.Bytes2String(components[i]));
				if (i < end - 1)
				{
					buf.Append(Path.Separator);
				}
			}
			return buf.ToString();
		}

		/// <summary>Retrieve existing INodes from a path.</summary>
		/// <remarks>
		/// Retrieve existing INodes from a path. For non-snapshot path,
		/// the number of INodes is equal to the number of path components. For
		/// snapshot path (e.g., /foo/.snapshot/s1/bar), the number of INodes is
		/// (number_of_path_components - 1).
		/// An UnresolvedPathException is always thrown when an intermediate path
		/// component refers to a symbolic link. If the final path component refers
		/// to a symbolic link then an UnresolvedPathException is only thrown if
		/// resolveLink is true.
		/// <p>
		/// Example: <br />
		/// Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
		/// following path components: ["","c1","c2","c3"]
		/// <p>
		/// <code>getExistingPathINodes(["","c1","c2"])</code> should fill
		/// the array with [rootINode,c1,c2], <br />
		/// <code>getExistingPathINodes(["","c1","c2","c3"])</code> should
		/// fill the array with [rootINode,c1,c2,null]
		/// </remarks>
		/// <param name="startingDir">the starting directory</param>
		/// <param name="components">array of path component name</param>
		/// <param name="resolveLink">
		/// indicates whether UnresolvedLinkException should
		/// be thrown when the path refers to a symbolic link.
		/// </param>
		/// <returns>the specified number of existing INodes in the path</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath Resolve(INodeDirectory
			 startingDir, byte[][] components, bool resolveLink)
		{
			Preconditions.CheckArgument(startingDir.CompareTo(components[0]) == 0);
			INode curNode = startingDir;
			int count = 0;
			int inodeNum = 0;
			INode[] inodes = new INode[components.Length];
			bool isSnapshot = false;
			int snapshotId = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
			while (count < components.Length && curNode != null)
			{
				bool lastComp = (count == components.Length - 1);
				inodes[inodeNum++] = curNode;
				bool isRef = curNode.IsReference();
				bool isDir = curNode.IsDirectory();
				INodeDirectory dir = isDir ? curNode.AsDirectory() : null;
				if (!isRef && isDir && dir.IsWithSnapshot())
				{
					//if the path is a non-snapshot path, update the latest snapshot.
					if (!isSnapshot && ShouldUpdateLatestId(dir.GetDirectoryWithSnapshotFeature().GetLastSnapshotId
						(), snapshotId))
					{
						snapshotId = dir.GetDirectoryWithSnapshotFeature().GetLastSnapshotId();
					}
				}
				else
				{
					if (isRef && isDir && !lastComp)
					{
						// If the curNode is a reference node, need to check its dstSnapshot:
						// 1. if the existing snapshot is no later than the dstSnapshot (which
						// is the latest snapshot in dst before the rename), the changes 
						// should be recorded in previous snapshots (belonging to src).
						// 2. however, if the ref node is already the last component, we still 
						// need to know the latest snapshot among the ref node's ancestors, 
						// in case of processing a deletion operation. Thus we do not overwrite
						// the latest snapshot if lastComp is true. In case of the operation is
						// a modification operation, we do a similar check in corresponding 
						// recordModification method.
						if (!isSnapshot)
						{
							int dstSnapshotId = curNode.AsReference().GetDstSnapshotId();
							if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
								 || (dstSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
								 && dstSnapshotId >= snapshotId))
							{
								// no snapshot in dst tree of rename
								// the above scenario
								int lastSnapshot = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
								DirectoryWithSnapshotFeature sf;
								if (curNode.IsDirectory() && (sf = curNode.AsDirectory().GetDirectoryWithSnapshotFeature
									()) != null)
								{
									lastSnapshot = sf.GetLastSnapshotId();
								}
								snapshotId = lastSnapshot;
							}
						}
					}
				}
				if (curNode.IsSymlink() && (!lastComp || resolveLink))
				{
					string path = ConstructPath(components, 0, components.Length);
					string preceding = ConstructPath(components, 0, count);
					string remainder = ConstructPath(components, count + 1, components.Length);
					string link = DFSUtil.Bytes2String(components[count]);
					string target = curNode.AsSymlink().GetSymlinkString();
					if (Log.IsDebugEnabled())
					{
						Log.Debug("UnresolvedPathException " + " path: " + path + " preceding: " + preceding
							 + " count: " + count + " link: " + link + " target: " + target + " remainder: "
							 + remainder);
					}
					throw new UnresolvedPathException(path, preceding, remainder, target);
				}
				if (lastComp || !isDir)
				{
					break;
				}
				byte[] childName = components[count + 1];
				// check if the next byte[] in components is for ".snapshot"
				if (IsDotSnapshotDir(childName) && dir.IsSnapshottable())
				{
					// skip the ".snapshot" in components
					count++;
					isSnapshot = true;
					// check if ".snapshot" is the last element of components
					if (count == components.Length - 1)
					{
						break;
					}
					// Resolve snapshot root
					Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = dir.GetSnapshot(components
						[count + 1]);
					if (s == null)
					{
						curNode = null;
					}
					else
					{
						// snapshot not found
						curNode = s.GetRoot();
						snapshotId = s.GetId();
					}
				}
				else
				{
					// normal case, and also for resolving file/dir under snapshot root
					curNode = dir.GetChild(childName, isSnapshot ? snapshotId : Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
				}
				count++;
			}
			if (isSnapshot && !IsDotSnapshotDir(components[components.Length - 1]))
			{
				// for snapshot path shrink the inode array. however, for path ending with
				// .snapshot, still keep last the null inode in the array
				INode[] newNodes = new INode[components.Length - 1];
				System.Array.Copy(inodes, 0, newNodes, 0, newNodes.Length);
				inodes = newNodes;
			}
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(inodes, components
				, isSnapshot, snapshotId);
		}

		private static bool ShouldUpdateLatestId(int sid, int snapshotId)
		{
			return snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 || (sid != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 && Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator
				.Compare(snapshotId, sid) < 0);
		}

		/// <summary>Replace an inode of the given INodesInPath in the given position.</summary>
		/// <remarks>
		/// Replace an inode of the given INodesInPath in the given position. We do a
		/// deep copy of the INode array.
		/// </remarks>
		/// <param name="pos">the position of the replacement</param>
		/// <param name="inode">the new inode</param>
		/// <returns>a new INodesInPath instance</returns>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath Replace(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath
			 iip, int pos, INode inode)
		{
			Preconditions.CheckArgument(iip.Length() > 0 && pos > 0 && pos < iip.Length());
			// no for root
			if (iip.GetINode(pos) == null)
			{
				Preconditions.CheckState(iip.GetINode(pos - 1) != null);
			}
			INode[] inodes = new INode[iip.inodes.Length];
			System.Array.Copy(iip.inodes, 0, inodes, 0, inodes.Length);
			inodes[pos] = inode;
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(inodes, iip.path, 
				iip.isSnapshot, iip.snapshotId);
		}

		/// <summary>Extend a given INodesInPath with a child INode.</summary>
		/// <remarks>
		/// Extend a given INodesInPath with a child INode. The child INode will be
		/// appended to the end of the new INodesInPath.
		/// </remarks>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath Append(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath
			 iip, INode child, byte[] childName)
		{
			Preconditions.CheckArgument(!iip.isSnapshot && iip.Length() > 0);
			Preconditions.CheckArgument(iip.GetLastINode() != null && iip.GetLastINode().IsDirectory
				());
			INode[] inodes = new INode[iip.Length() + 1];
			System.Array.Copy(iip.inodes, 0, inodes, 0, inodes.Length - 1);
			inodes[inodes.Length - 1] = child;
			byte[][] path = new byte[iip.path.Length + 1][];
			System.Array.Copy(iip.path, 0, path, 0, path.Length - 1);
			path[path.Length - 1] = childName;
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(inodes, path, false
				, iip.snapshotId);
		}

		private readonly byte[][] path;

		/// <summary>Array with the specified number of INodes resolved for a given path.</summary>
		private readonly INode[] inodes;

		/// <summary>true if this path corresponds to a snapshot</summary>
		private readonly bool isSnapshot;

		/// <summary>
		/// For snapshot paths, it is the id of the snapshot; or
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// if the snapshot does not exist. For
		/// non-snapshot paths, it is the id of the latest snapshot found in the path;
		/// or
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// if no snapshot is found.
		/// </summary>
		private readonly int snapshotId;

		private INodesInPath(INode[] inodes, byte[][] path, bool isSnapshot, int snapshotId
			)
		{
			Preconditions.CheckArgument(inodes != null && path != null);
			this.inodes = inodes;
			this.path = path;
			this.isSnapshot = isSnapshot;
			this.snapshotId = snapshotId;
		}

		private INodesInPath(INode[] inodes, byte[][] path)
			: this(inodes, path, false, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId)
		{
		}

		/// <summary>For non-snapshot paths, return the latest snapshot id found in the path.
		/// 	</summary>
		public virtual int GetLatestSnapshotId()
		{
			Preconditions.CheckState(!isSnapshot);
			return snapshotId;
		}

		/// <summary>For snapshot paths, return the id of the snapshot specified in the path.
		/// 	</summary>
		/// <remarks>
		/// For snapshot paths, return the id of the snapshot specified in the path.
		/// For non-snapshot paths, return
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// .
		/// </remarks>
		public virtual int GetPathSnapshotId()
		{
			return isSnapshot ? snapshotId : Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId;
		}

		/// <returns>
		/// the i-th inode if i &gt;= 0;
		/// otherwise, i &lt; 0, return the (length + i)-th inode.
		/// </returns>
		public virtual INode GetINode(int i)
		{
			if (inodes == null || inodes.Length == 0)
			{
				throw new NoSuchElementException("inodes is null or empty");
			}
			int index = i >= 0 ? i : inodes.Length + i;
			if (index < inodes.Length && index >= 0)
			{
				return inodes[index];
			}
			else
			{
				throw new NoSuchElementException("inodes.length == " + inodes.Length);
			}
		}

		/// <returns>the last inode.</returns>
		public virtual INode GetLastINode()
		{
			return GetINode(-1);
		}

		internal virtual byte[] GetLastLocalName()
		{
			return path[path.Length - 1];
		}

		public virtual byte[][] GetPathComponents()
		{
			return path;
		}

		/// <returns>the full path in string form</returns>
		public virtual string GetPath()
		{
			return DFSUtil.ByteArray2PathString(path);
		}

		public virtual string GetParentPath()
		{
			return GetPath(path.Length - 1);
		}

		public virtual string GetPath(int pos)
		{
			return DFSUtil.ByteArray2PathString(path, 0, pos);
		}

		/// <param name="offset">start endpoint (inclusive)</param>
		/// <param name="length">number of path components</param>
		/// <returns>sub-list of the path</returns>
		public virtual IList<string> GetPath(int offset, int length)
		{
			Preconditions.CheckArgument(offset >= 0 && length >= 0 && offset + length <= path
				.Length);
			ImmutableList.Builder<string> components = ImmutableList.Builder();
			for (int i = offset; i < offset + length; i++)
			{
				components.Add(DFSUtil.Bytes2String(path[i]));
			}
			return ((ImmutableList<string>)components.Build());
		}

		public virtual int Length()
		{
			return inodes.Length;
		}

		public virtual IList<INode> GetReadOnlyINodes()
		{
			return Sharpen.Collections.UnmodifiableList(Arrays.AsList(inodes));
		}

		public virtual INode[] GetINodesArray()
		{
			INode[] retArr = new INode[inodes.Length];
			System.Array.Copy(inodes, 0, retArr, 0, inodes.Length);
			return retArr;
		}

		/// <param name="length">
		/// number of ancestral INodes in the returned INodesInPath
		/// instance
		/// </param>
		/// <returns>
		/// the INodesInPath instance containing ancestral INodes. Note that
		/// this method only handles non-snapshot paths.
		/// </returns>
		private Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath GetAncestorINodesInPath
			(int length)
		{
			Preconditions.CheckArgument(length >= 0 && length < inodes.Length);
			Preconditions.CheckState(!IsSnapshot());
			INode[] anodes = new INode[length];
			byte[][] apath = new byte[length][];
			System.Array.Copy(this.inodes, 0, anodes, 0, length);
			System.Array.Copy(this.path, 0, apath, 0, length);
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(anodes, apath, false
				, snapshotId);
		}

		/// <returns>
		/// an INodesInPath instance containing all the INodes in the parent
		/// path. We do a deep copy here.
		/// </returns>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath GetParentINodesInPath
			()
		{
			return inodes.Length > 1 ? GetAncestorINodesInPath(inodes.Length - 1) : null;
		}

		/// <returns>
		/// a new INodesInPath instance that only contains exisitng INodes.
		/// Note that this method only handles non-snapshot paths.
		/// </returns>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath GetExistingINodes
			()
		{
			Preconditions.CheckState(!IsSnapshot());
			int i = 0;
			for (; i < inodes.Length; i++)
			{
				if (inodes[i] == null)
				{
					break;
				}
			}
			INode[] existing = new INode[i];
			byte[][] existingPath = new byte[i][];
			System.Array.Copy(inodes, 0, existing, 0, i);
			System.Array.Copy(path, 0, existingPath, 0, i);
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodesInPath(existing, existingPath
				, false, snapshotId);
		}

		/// <returns>isSnapshot true for a snapshot path</returns>
		internal virtual bool IsSnapshot()
		{
			return this.isSnapshot;
		}

		private static string ToString(INode inode)
		{
			return inode == null ? null : inode.GetLocalName();
		}

		public override string ToString()
		{
			return ToString(true);
		}

		private string ToString(bool vaildateObject)
		{
			if (vaildateObject)
			{
				Validate();
			}
			StringBuilder b = new StringBuilder(GetType().Name).Append(": path = ").Append(DFSUtil
				.ByteArray2PathString(path)).Append("\n  inodes = ");
			if (inodes == null)
			{
				b.Append("null");
			}
			else
			{
				if (inodes.Length == 0)
				{
					b.Append("[]");
				}
				else
				{
					b.Append("[").Append(ToString(inodes[0]));
					for (int i = 1; i < inodes.Length; i++)
					{
						b.Append(", ").Append(ToString(inodes[i]));
					}
					b.Append("], length=").Append(inodes.Length);
				}
			}
			b.Append("\n  isSnapshot        = ").Append(isSnapshot).Append("\n  snapshotId        = "
				).Append(snapshotId);
			return b.ToString();
		}

		internal virtual void Validate()
		{
			// check parent up to snapshotRootIndex if this is a snapshot path
			int i = 0;
			if (inodes[i] != null)
			{
				for (i++; i < inodes.Length && inodes[i] != null; i++)
				{
					INodeDirectory parent_i = inodes[i].GetParent();
					INodeDirectory parent_i_1 = inodes[i - 1].GetParent();
					if (parent_i != inodes[i - 1] && (parent_i_1 == null || !parent_i_1.IsSnapshottable
						() || parent_i != parent_i_1))
					{
						throw new Exception("inodes[" + i + "].getParent() != inodes[" + (i - 1) + "]\n  inodes["
							 + i + "]=" + inodes[i].ToDetailString() + "\n  inodes[" + (i - 1) + "]=" + inodes
							[i - 1].ToDetailString() + "\n this=" + ToString(false));
					}
				}
			}
			if (i != inodes.Length)
			{
				throw new Exception("i = " + i + " != " + inodes.Length + ", this=" + ToString(false
					));
			}
		}
	}
}
