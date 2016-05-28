using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class FSImageFormatPBSnapshot
	{
		/// <summary>Loading snapshot related information from protobuf based FSImage</summary>
		public sealed class Loader
		{
			private readonly FSNamesystem fsn;

			private readonly FSDirectory fsDir;

			private readonly FSImageFormatProtobuf.Loader parent;

			private readonly IDictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				> snapshotMap;

			public Loader(FSNamesystem fsn, FSImageFormatProtobuf.Loader parent)
			{
				this.fsn = fsn;
				this.fsDir = fsn.GetFSDirectory();
				this.snapshotMap = new Dictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					>();
				this.parent = parent;
			}

			/// <summary>
			/// The sequence of the ref node in refList must be strictly the same with
			/// the sequence in fsimage
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public void LoadINodeReferenceSection(InputStream @in)
			{
				IList<INodeReference> refList = parent.GetLoaderContext().GetRefList();
				while (true)
				{
					FsImageProto.INodeReferenceSection.INodeReference e = FsImageProto.INodeReferenceSection.INodeReference
						.ParseDelimitedFrom(@in);
					if (e == null)
					{
						break;
					}
					INodeReference @ref = LoadINodeReference(e);
					refList.AddItem(@ref);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private INodeReference LoadINodeReference(FsImageProto.INodeReferenceSection.INodeReference
				 r)
			{
				long referredId = r.GetReferredId();
				INode referred = fsDir.GetInode(referredId);
				INodeReference.WithCount withCount = (INodeReference.WithCount)referred.GetParentReference
					();
				if (withCount == null)
				{
					withCount = new INodeReference.WithCount(null, referred);
				}
				INodeReference @ref;
				if (r.HasDstSnapshotId())
				{
					// DstReference
					@ref = new INodeReference.DstReference(null, withCount, r.GetDstSnapshotId());
				}
				else
				{
					@ref = new INodeReference.WithName(null, withCount, r.GetName().ToByteArray(), r.
						GetLastSnapshotId());
				}
				return @ref;
			}

			/// <summary>Load the snapshots section from fsimage.</summary>
			/// <remarks>
			/// Load the snapshots section from fsimage. Also add snapshottable feature
			/// to snapshottable directories.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public void LoadSnapshotSection(InputStream @in)
			{
				SnapshotManager sm = fsn.GetSnapshotManager();
				FsImageProto.SnapshotSection section = FsImageProto.SnapshotSection.ParseDelimitedFrom
					(@in);
				int snum = section.GetNumSnapshots();
				sm.SetNumSnapshots(snum);
				sm.SetSnapshotCounter(section.GetSnapshotCounter());
				foreach (long sdirId in section.GetSnapshottableDirList())
				{
					INodeDirectory dir = fsDir.GetInode(sdirId).AsDirectory();
					if (!dir.IsSnapshottable())
					{
						dir.AddSnapshottableFeature();
					}
					else
					{
						// dir is root, and admin set root to snapshottable before
						dir.SetSnapshotQuota(DirectorySnapshottableFeature.SnapshotLimit);
					}
					sm.AddSnapshottable(dir);
				}
				LoadSnapshots(@in, snum);
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadSnapshots(InputStream @in, int size)
			{
				for (int i = 0; i < size; i++)
				{
					FsImageProto.SnapshotSection.Snapshot pbs = FsImageProto.SnapshotSection.Snapshot
						.ParseDelimitedFrom(@in);
					INodeDirectory root = FSImageFormatPBINode.Loader.LoadINodeDirectory(pbs.GetRoot(
						), parent.GetLoaderContext());
					int sid = pbs.GetSnapshotId();
					INodeDirectory parent = fsDir.GetInode(root.GetId()).AsDirectory();
					Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						(sid, root, parent);
					// add the snapshot to parent, since we follow the sequence of
					// snapshotsByNames when saving, we do not need to sort when loading
					parent.GetDirectorySnapshottableFeature().AddSnapshot(snapshot);
					snapshotMap[sid] = snapshot;
				}
			}

			/// <summary>Load the snapshot diff section from fsimage.</summary>
			/// <exception cref="System.IO.IOException"/>
			public void LoadSnapshotDiffSection(InputStream @in)
			{
				IList<INodeReference> refList = parent.GetLoaderContext().GetRefList();
				while (true)
				{
					FsImageProto.SnapshotDiffSection.DiffEntry entry = FsImageProto.SnapshotDiffSection.DiffEntry
						.ParseDelimitedFrom(@in);
					if (entry == null)
					{
						break;
					}
					long inodeId = entry.GetInodeId();
					INode inode = fsDir.GetInode(inodeId);
					FsImageProto.SnapshotDiffSection.DiffEntry.Type type = entry.GetType();
					switch (type)
					{
						case FsImageProto.SnapshotDiffSection.DiffEntry.Type.Filediff:
						{
							LoadFileDiffList(@in, inode.AsFile(), entry.GetNumOfDiff());
							break;
						}

						case FsImageProto.SnapshotDiffSection.DiffEntry.Type.Directorydiff:
						{
							LoadDirectoryDiffList(@in, inode.AsDirectory(), entry.GetNumOfDiff(), refList);
							break;
						}
					}
				}
			}

			/// <summary>Load FileDiff list for a file with snapshot feature</summary>
			/// <exception cref="System.IO.IOException"/>
			private void LoadFileDiffList(InputStream @in, INodeFile file, int size)
			{
				FileDiffList diffs = new FileDiffList();
				FSImageFormatProtobuf.LoaderContext state = parent.GetLoaderContext();
				for (int i = 0; i < size; i++)
				{
					FsImageProto.SnapshotDiffSection.FileDiff pbf = FsImageProto.SnapshotDiffSection.FileDiff
						.ParseDelimitedFrom(@in);
					INodeFileAttributes copy = null;
					if (pbf.HasSnapshotCopy())
					{
						FsImageProto.INodeSection.INodeFile fileInPb = pbf.GetSnapshotCopy();
						PermissionStatus permission = FSImageFormatPBINode.Loader.LoadPermission(fileInPb
							.GetPermission(), state.GetStringTable());
						AclFeature acl = null;
						if (fileInPb.HasAcl())
						{
							int[] entries = AclEntryStatusFormat.ToInt(FSImageFormatPBINode.Loader.LoadAclEntries
								(fileInPb.GetAcl(), state.GetStringTable()));
							acl = new AclFeature(entries);
						}
						XAttrFeature xAttrs = null;
						if (fileInPb.HasXAttrs())
						{
							xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.LoadXAttrs(fileInPb.GetXAttrs
								(), state.GetStringTable()));
						}
						copy = new INodeFileAttributes.SnapshotCopy(pbf.GetName().ToByteArray(), permission
							, acl, fileInPb.GetModificationTime(), fileInPb.GetAccessTime(), (short)fileInPb
							.GetReplication(), fileInPb.GetPreferredBlockSize(), unchecked((byte)fileInPb.GetStoragePolicyID
							()), xAttrs);
					}
					FileDiff diff = new FileDiff(pbf.GetSnapshotId(), copy, null, pbf.GetFileSize());
					IList<HdfsProtos.BlockProto> bpl = pbf.GetBlocksList();
					BlockInfoContiguous[] blocks = new BlockInfoContiguous[bpl.Count];
					for (int j = 0; j < e; ++j)
					{
						Block blk = PBHelper.Convert(bpl[j]);
						BlockInfoContiguous storedBlock = fsn.GetBlockManager().GetStoredBlock(blk);
						if (storedBlock == null)
						{
							storedBlock = fsn.GetBlockManager().AddBlockCollection(new BlockInfoContiguous(blk
								, copy.GetFileReplication()), file);
						}
						blocks[j] = storedBlock;
					}
					if (blocks.Length > 0)
					{
						diff.SetBlocks(blocks);
					}
					diffs.AddFirst(diff);
				}
				file.AddSnapshotFeature(diffs);
			}

			/// <summary>Load the created list in a DirectoryDiff</summary>
			/// <exception cref="System.IO.IOException"/>
			private IList<INode> LoadCreatedList(InputStream @in, INodeDirectory dir, int size
				)
			{
				IList<INode> clist = new AList<INode>(size);
				for (long c = 0; c < size; c++)
				{
					FsImageProto.SnapshotDiffSection.CreatedListEntry entry = FsImageProto.SnapshotDiffSection.CreatedListEntry
						.ParseDelimitedFrom(@in);
					INode created = SnapshotFSImageFormat.LoadCreated(entry.GetName().ToByteArray(), 
						dir);
					clist.AddItem(created);
				}
				return clist;
			}

			private void AddToDeletedList(INode dnode, INodeDirectory parent)
			{
				dnode.SetParent(parent);
				if (dnode.IsFile())
				{
					FSImageFormatPBINode.Loader.UpdateBlocksMap(dnode.AsFile(), fsn.GetBlockManager()
						);
				}
			}

			/// <summary>Load the deleted list in a DirectoryDiff</summary>
			/// <exception cref="System.IO.IOException"/>
			private IList<INode> LoadDeletedList(IList<INodeReference> refList, InputStream @in
				, INodeDirectory dir, IList<long> deletedNodes, IList<int> deletedRefNodes)
			{
				IList<INode> dlist = new AList<INode>(deletedRefNodes.Count + deletedNodes.Count);
				// load non-reference inodes
				foreach (long deletedId in deletedNodes)
				{
					INode deleted = fsDir.GetInode(deletedId);
					dlist.AddItem(deleted);
					AddToDeletedList(deleted, dir);
				}
				// load reference nodes in the deleted list
				foreach (int refId in deletedRefNodes)
				{
					INodeReference deletedRef = refList[refId];
					dlist.AddItem(deletedRef);
					AddToDeletedList(deletedRef, dir);
				}
				dlist.Sort(new _IComparer_303());
				return dlist;
			}

			private sealed class _IComparer_303 : IComparer<INode>
			{
				public _IComparer_303()
				{
				}

				public int Compare(INode n1, INode n2)
				{
					return n1.CompareTo(n2.GetLocalNameBytes());
				}
			}

			/// <summary>Load DirectoryDiff list for a directory with snapshot feature</summary>
			/// <exception cref="System.IO.IOException"/>
			private void LoadDirectoryDiffList(InputStream @in, INodeDirectory dir, int size, 
				IList<INodeReference> refList)
			{
				if (!dir.IsWithSnapshot())
				{
					dir.AddSnapshotFeature(null);
				}
				DirectoryWithSnapshotFeature.DirectoryDiffList diffs = dir.GetDiffs();
				FSImageFormatProtobuf.LoaderContext state = parent.GetLoaderContext();
				for (int i = 0; i < size; i++)
				{
					// load a directory diff
					FsImageProto.SnapshotDiffSection.DirectoryDiff diffInPb = FsImageProto.SnapshotDiffSection.DirectoryDiff
						.ParseDelimitedFrom(@in);
					int snapshotId = diffInPb.GetSnapshotId();
					Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = snapshotMap[snapshotId
						];
					int childrenSize = diffInPb.GetChildrenSize();
					bool useRoot = diffInPb.GetIsSnapshotRoot();
					INodeDirectoryAttributes copy = null;
					if (useRoot)
					{
						copy = snapshot.GetRoot();
					}
					else
					{
						if (diffInPb.HasSnapshotCopy())
						{
							FsImageProto.INodeSection.INodeDirectory dirCopyInPb = diffInPb.GetSnapshotCopy();
							byte[] name = diffInPb.GetName().ToByteArray();
							PermissionStatus permission = FSImageFormatPBINode.Loader.LoadPermission(dirCopyInPb
								.GetPermission(), state.GetStringTable());
							AclFeature acl = null;
							if (dirCopyInPb.HasAcl())
							{
								int[] entries = AclEntryStatusFormat.ToInt(FSImageFormatPBINode.Loader.LoadAclEntries
									(dirCopyInPb.GetAcl(), state.GetStringTable()));
								acl = new AclFeature(entries);
							}
							XAttrFeature xAttrs = null;
							if (dirCopyInPb.HasXAttrs())
							{
								xAttrs = new XAttrFeature(FSImageFormatPBINode.Loader.LoadXAttrs(dirCopyInPb.GetXAttrs
									(), state.GetStringTable()));
							}
							long modTime = dirCopyInPb.GetModificationTime();
							bool noQuota = dirCopyInPb.GetNsQuota() == -1 && dirCopyInPb.GetDsQuota() == -1 &&
								 (!dirCopyInPb.HasTypeQuotas());
							if (noQuota)
							{
								copy = new INodeDirectoryAttributes.SnapshotCopy(name, permission, acl, modTime, 
									xAttrs);
							}
							else
							{
								EnumCounters<StorageType> typeQuotas = null;
								if (dirCopyInPb.HasTypeQuotas())
								{
									ImmutableList<QuotaByStorageTypeEntry> qes = FSImageFormatPBINode.Loader.LoadQuotaByStorageTypeEntries
										(dirCopyInPb.GetTypeQuotas());
									typeQuotas = new EnumCounters<StorageType>(typeof(StorageType), HdfsConstants.QuotaReset
										);
									foreach (QuotaByStorageTypeEntry qe in qes)
									{
										if (qe.GetQuota() >= 0 && qe.GetStorageType() != null && qe.GetStorageType().SupportTypeQuota
											())
										{
											typeQuotas.Set(qe.GetStorageType(), qe.GetQuota());
										}
									}
								}
								copy = new INodeDirectoryAttributes.CopyWithQuota(name, permission, acl, modTime, 
									dirCopyInPb.GetNsQuota(), dirCopyInPb.GetDsQuota(), typeQuotas, xAttrs);
							}
						}
					}
					// load created list
					IList<INode> clist = LoadCreatedList(@in, dir, diffInPb.GetCreatedListSize());
					// load deleted list
					IList<INode> dlist = LoadDeletedList(refList, @in, dir, diffInPb.GetDeletedINodeList
						(), diffInPb.GetDeletedINodeRefList());
					// create the directory diff
					DirectoryWithSnapshotFeature.DirectoryDiff diff = new DirectoryWithSnapshotFeature.DirectoryDiff
						(snapshotId, copy, null, childrenSize, clist, dlist, useRoot);
					diffs.AddFirst(diff);
				}
			}
		}

		/// <summary>Saving snapshot related information to protobuf based FSImage</summary>
		public sealed class Saver
		{
			private readonly FSNamesystem fsn;

			private readonly FsImageProto.FileSummary.Builder headers;

			private readonly FSImageFormatProtobuf.Saver parent;

			private readonly SaveNamespaceContext context;

			public Saver(FSImageFormatProtobuf.Saver parent, FsImageProto.FileSummary.Builder
				 headers, SaveNamespaceContext context, FSNamesystem fsn)
			{
				this.parent = parent;
				this.headers = headers;
				this.context = context;
				this.fsn = fsn;
			}

			/// <summary>save all the snapshottable directories and snapshots to fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			public void SerializeSnapshotSection(OutputStream @out)
			{
				SnapshotManager sm = fsn.GetSnapshotManager();
				FsImageProto.SnapshotSection.Builder b = FsImageProto.SnapshotSection.NewBuilder(
					).SetSnapshotCounter(sm.GetSnapshotCounter()).SetNumSnapshots(sm.GetNumSnapshots
					());
				INodeDirectory[] snapshottables = sm.GetSnapshottableDirs();
				foreach (INodeDirectory sdir in snapshottables)
				{
					b.AddSnapshottableDir(sdir.GetId());
				}
				((FsImageProto.SnapshotSection)b.Build()).WriteDelimitedTo(@out);
				int i = 0;
				foreach (INodeDirectory sdir_1 in snapshottables)
				{
					foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in sdir_1.GetDirectorySnapshottableFeature
						().GetSnapshotList())
					{
						Snapshot.Root sroot = s.GetRoot();
						FsImageProto.SnapshotSection.Snapshot.Builder sb = FsImageProto.SnapshotSection.Snapshot
							.NewBuilder().SetSnapshotId(s.GetId());
						FsImageProto.INodeSection.INodeDirectory.Builder db = FSImageFormatPBINode.Saver.BuildINodeDirectory
							(sroot, parent.GetSaverContext());
						FsImageProto.INodeSection.INode r = ((FsImageProto.INodeSection.INode)FsImageProto.INodeSection.INode
							.NewBuilder().SetId(sroot.GetId()).SetType(FsImageProto.INodeSection.INode.Type.
							Directory).SetName(ByteString.CopyFrom(sroot.GetLocalNameBytes())).SetDirectory(
							db).Build());
						((FsImageProto.SnapshotSection.Snapshot)sb.SetRoot(r).Build()).WriteDelimitedTo(@out
							);
						i++;
						if (i % FSImageFormatProtobuf.Saver.CheckCancelInterval == 0)
						{
							context.CheckCancelled();
						}
					}
				}
				Preconditions.CheckState(i == sm.GetNumSnapshots());
				parent.CommitSection(headers, FSImageFormatProtobuf.SectionName.Snapshot);
			}

			/// <summary>This can only be called after serializing both INode_Dir and SnapshotDiff
			/// 	</summary>
			/// <exception cref="System.IO.IOException"/>
			public void SerializeINodeReferenceSection(OutputStream @out)
			{
				IList<INodeReference> refList = parent.GetSaverContext().GetRefList();
				foreach (INodeReference @ref in refList)
				{
					FsImageProto.INodeReferenceSection.INodeReference.Builder rb = BuildINodeReference
						(@ref);
					((FsImageProto.INodeReferenceSection.INodeReference)rb.Build()).WriteDelimitedTo(
						@out);
				}
				parent.CommitSection(headers, FSImageFormatProtobuf.SectionName.InodeReference);
			}

			/// <exception cref="System.IO.IOException"/>
			private FsImageProto.INodeReferenceSection.INodeReference.Builder BuildINodeReference
				(INodeReference @ref)
			{
				FsImageProto.INodeReferenceSection.INodeReference.Builder rb = FsImageProto.INodeReferenceSection.INodeReference
					.NewBuilder().SetReferredId(@ref.GetId());
				if (@ref is INodeReference.WithName)
				{
					rb.SetLastSnapshotId(((INodeReference.WithName)@ref).GetLastSnapshotId()).SetName
						(ByteString.CopyFrom(@ref.GetLocalNameBytes()));
				}
				else
				{
					if (@ref is INodeReference.DstReference)
					{
						rb.SetDstSnapshotId(@ref.GetDstSnapshotId());
					}
				}
				return rb;
			}

			/// <summary>save all the snapshot diff to fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			public void SerializeSnapshotDiffSection(OutputStream @out)
			{
				INodeMap inodesMap = fsn.GetFSDirectory().GetINodeMap();
				IList<INodeReference> refList = parent.GetSaverContext().GetRefList();
				int i = 0;
				IEnumerator<INodeWithAdditionalFields> iter = inodesMap.GetMapIterator();
				while (iter.HasNext())
				{
					INodeWithAdditionalFields inode = iter.Next();
					if (inode.IsFile())
					{
						SerializeFileDiffList(inode.AsFile(), @out);
					}
					else
					{
						if (inode.IsDirectory())
						{
							SerializeDirDiffList(inode.AsDirectory(), refList, @out);
						}
					}
					++i;
					if (i % FSImageFormatProtobuf.Saver.CheckCancelInterval == 0)
					{
						context.CheckCancelled();
					}
				}
				parent.CommitSection(headers, FSImageFormatProtobuf.SectionName.SnapshotDiff);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SerializeFileDiffList(INodeFile file, OutputStream @out)
			{
				FileWithSnapshotFeature sf = file.GetFileWithSnapshotFeature();
				if (sf != null)
				{
					IList<FileDiff> diffList = sf.GetDiffs().AsList();
					FsImageProto.SnapshotDiffSection.DiffEntry entry = ((FsImageProto.SnapshotDiffSection.DiffEntry
						)FsImageProto.SnapshotDiffSection.DiffEntry.NewBuilder().SetInodeId(file.GetId()
						).SetType(FsImageProto.SnapshotDiffSection.DiffEntry.Type.Filediff).SetNumOfDiff
						(diffList.Count).Build());
					entry.WriteDelimitedTo(@out);
					for (int i = diffList.Count - 1; i >= 0; i--)
					{
						FileDiff diff = diffList[i];
						FsImageProto.SnapshotDiffSection.FileDiff.Builder fb = FsImageProto.SnapshotDiffSection.FileDiff
							.NewBuilder().SetSnapshotId(diff.GetSnapshotId()).SetFileSize(diff.GetFileSize()
							);
						if (diff.GetBlocks() != null)
						{
							foreach (Block block in diff.GetBlocks())
							{
								fb.AddBlocks(PBHelper.Convert(block));
							}
						}
						INodeFileAttributes copy = diff.snapshotINode;
						if (copy != null)
						{
							fb.SetName(ByteString.CopyFrom(copy.GetLocalNameBytes())).SetSnapshotCopy(FSImageFormatPBINode.Saver.BuildINodeFile
								(copy, parent.GetSaverContext()));
						}
						((FsImageProto.SnapshotDiffSection.FileDiff)fb.Build()).WriteDelimitedTo(@out);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveCreatedList(IList<INode> created, OutputStream @out)
			{
				// local names of the created list member
				foreach (INode c in created)
				{
					((FsImageProto.SnapshotDiffSection.CreatedListEntry)FsImageProto.SnapshotDiffSection.CreatedListEntry
						.NewBuilder().SetName(ByteString.CopyFrom(c.GetLocalNameBytes())).Build()).WriteDelimitedTo
						(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void SerializeDirDiffList(INodeDirectory dir, IList<INodeReference> refList
				, OutputStream @out)
			{
				DirectoryWithSnapshotFeature sf = dir.GetDirectoryWithSnapshotFeature();
				if (sf != null)
				{
					IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = sf.GetDiffs().AsList
						();
					FsImageProto.SnapshotDiffSection.DiffEntry entry = ((FsImageProto.SnapshotDiffSection.DiffEntry
						)FsImageProto.SnapshotDiffSection.DiffEntry.NewBuilder().SetInodeId(dir.GetId())
						.SetType(FsImageProto.SnapshotDiffSection.DiffEntry.Type.Directorydiff).SetNumOfDiff
						(diffList.Count).Build());
					entry.WriteDelimitedTo(@out);
					for (int i = diffList.Count - 1; i >= 0; i--)
					{
						// reverse order!
						DirectoryWithSnapshotFeature.DirectoryDiff diff = diffList[i];
						FsImageProto.SnapshotDiffSection.DirectoryDiff.Builder db = FsImageProto.SnapshotDiffSection.DirectoryDiff
							.NewBuilder().SetSnapshotId(diff.GetSnapshotId()).SetChildrenSize(diff.GetChildrenSize
							()).SetIsSnapshotRoot(diff.IsSnapshotRoot());
						INodeDirectoryAttributes copy = diff.snapshotINode;
						if (!diff.IsSnapshotRoot() && copy != null)
						{
							db.SetName(ByteString.CopyFrom(copy.GetLocalNameBytes())).SetSnapshotCopy(FSImageFormatPBINode.Saver.BuildINodeDirectory
								(copy, parent.GetSaverContext()));
						}
						// process created list and deleted list
						IList<INode> created = diff.GetChildrenDiff().GetList(Diff.ListType.Created);
						db.SetCreatedListSize(created.Count);
						IList<INode> deleted = diff.GetChildrenDiff().GetList(Diff.ListType.Deleted);
						foreach (INode d in deleted)
						{
							if (d.IsReference())
							{
								refList.AddItem(d.AsReference());
								db.AddDeletedINodeRef(refList.Count - 1);
							}
							else
							{
								db.AddDeletedINode(d.GetId());
							}
						}
						((FsImageProto.SnapshotDiffSection.DirectoryDiff)db.Build()).WriteDelimitedTo(@out
							);
						SaveCreatedList(created, @out);
					}
				}
			}
		}

		private FSImageFormatPBSnapshot()
		{
		}
	}
}
