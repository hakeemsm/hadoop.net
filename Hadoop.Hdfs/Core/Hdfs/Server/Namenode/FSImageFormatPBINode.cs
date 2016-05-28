using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public sealed class FSImageFormatPBINode
	{
		private const long UserGroupStridMask = (1 << 24) - 1;

		private const int UserStridOffset = 40;

		private const int GroupStridOffset = 16;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageFormatPBINode
			));

		private const int AclEntryNameMask = (1 << 24) - 1;

		private const int AclEntryNameOffset = 6;

		private const int AclEntryTypeOffset = 3;

		private const int AclEntryScopeOffset = 5;

		private const int AclEntryPermMask = 7;

		private const int AclEntryTypeMask = 3;

		private const int AclEntryScopeMask = 1;

		private static readonly FsAction[] FsactionValues = FsAction.Values();

		private static readonly AclEntryScope[] AclEntryScopeValues = AclEntryScope.Values
			();

		private static readonly AclEntryType[] AclEntryTypeValues = AclEntryType.Values();

		private const int XattrNamespaceMask = 3;

		private const int XattrNamespaceOffset = 30;

		private const int XattrNameMask = (1 << 24) - 1;

		private const int XattrNameOffset = 6;

		private const int XattrNamespaceExtOffset = 5;

		private const int XattrNamespaceExtMask = 1;

		private static readonly XAttr.NameSpace[] XattrNamespaceValues = XAttr.NameSpace.
			Values();

		public sealed class Loader
		{
			/* See the comments in fsimage.proto for an explanation of the following. */
			public static PermissionStatus LoadPermission(long id, string[] stringTable)
			{
				short perm = (short)(id & ((1 << GroupStridOffset) - 1));
				int gsid = (int)((id >> GroupStridOffset) & UserGroupStridMask);
				int usid = (int)((id >> UserStridOffset) & UserGroupStridMask);
				return new PermissionStatus(stringTable[usid], stringTable[gsid], new FsPermission
					(perm));
			}

			public static ImmutableList<AclEntry> LoadAclEntries(FsImageProto.INodeSection.AclFeatureProto
				 proto, string[] stringTable)
			{
				ImmutableList.Builder<AclEntry> b = ImmutableList.Builder();
				foreach (int v in proto.GetEntriesList())
				{
					int p = v & AclEntryPermMask;
					int t = (v >> AclEntryTypeOffset) & AclEntryTypeMask;
					int s = (v >> AclEntryScopeOffset) & AclEntryScopeMask;
					int nid = (v >> AclEntryNameOffset) & AclEntryNameMask;
					string name = stringTable[nid];
					b.Add(new AclEntry.Builder().SetName(name).SetPermission(FsactionValues[p]).SetScope
						(AclEntryScopeValues[s]).SetType(AclEntryTypeValues[t]).Build());
				}
				return ((ImmutableList<AclEntry>)b.Build());
			}

			public static ImmutableList<XAttr> LoadXAttrs(FsImageProto.INodeSection.XAttrFeatureProto
				 proto, string[] stringTable)
			{
				ImmutableList.Builder<XAttr> b = ImmutableList.Builder();
				foreach (FsImageProto.INodeSection.XAttrCompactProto xAttrCompactProto in proto.GetXAttrsList
					())
				{
					int v = xAttrCompactProto.GetName();
					int nid = (v >> XattrNameOffset) & XattrNameMask;
					int ns = (v >> XattrNamespaceOffset) & XattrNamespaceMask;
					ns |= ((v >> XattrNamespaceExtOffset) & XattrNamespaceExtMask) << 2;
					string name = stringTable[nid];
					byte[] value = null;
					if (xAttrCompactProto.GetValue() != null)
					{
						value = xAttrCompactProto.GetValue().ToByteArray();
					}
					b.Add(new XAttr.Builder().SetNameSpace(XattrNamespaceValues[ns]).SetName(name).SetValue
						(value).Build());
				}
				return ((ImmutableList<XAttr>)b.Build());
			}

			public static ImmutableList<QuotaByStorageTypeEntry> LoadQuotaByStorageTypeEntries
				(FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto proto)
			{
				ImmutableList.Builder<QuotaByStorageTypeEntry> b = ImmutableList.Builder();
				foreach (FsImageProto.INodeSection.QuotaByStorageTypeEntryProto quotaEntry in proto
					.GetQuotasList())
				{
					StorageType type = PBHelper.ConvertStorageType(quotaEntry.GetStorageType());
					long quota = quotaEntry.GetQuota();
					b.Add(new QuotaByStorageTypeEntry.Builder().SetStorageType(type).SetQuota(quota).
						Build());
				}
				return ((ImmutableList<QuotaByStorageTypeEntry>)b.Build());
			}

			public static INodeDirectory LoadINodeDirectory(FsImageProto.INodeSection.INode n
				, FSImageFormatProtobuf.LoaderContext state)
			{
				System.Diagnostics.Debug.Assert(n.GetType() == FsImageProto.INodeSection.INode.Type
					.Directory);
				FsImageProto.INodeSection.INodeDirectory d = n.GetDirectory();
				PermissionStatus permissions = LoadPermission(d.GetPermission(), state.GetStringTable
					());
				INodeDirectory dir = new INodeDirectory(n.GetId(), n.GetName().ToByteArray(), permissions
					, d.GetModificationTime());
				long nsQuota = d.GetNsQuota();
				long dsQuota = d.GetDsQuota();
				if (nsQuota >= 0 || dsQuota >= 0)
				{
					dir.AddDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().NameSpaceQuota
						(nsQuota).StorageSpaceQuota(dsQuota).Build());
				}
				EnumCounters<StorageType> typeQuotas = null;
				if (d.HasTypeQuotas())
				{
					ImmutableList<QuotaByStorageTypeEntry> qes = LoadQuotaByStorageTypeEntries(d.GetTypeQuotas
						());
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
					if (typeQuotas.AnyGreaterOrEqual(0))
					{
						DirectoryWithQuotaFeature q = dir.GetDirectoryWithQuotaFeature();
						if (q == null)
						{
							dir.AddDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().TypeQuotas
								(typeQuotas).Build());
						}
						else
						{
							q.SetQuota(typeQuotas);
						}
					}
				}
				if (d.HasAcl())
				{
					int[] entries = AclEntryStatusFormat.ToInt(LoadAclEntries(d.GetAcl(), state.GetStringTable
						()));
					dir.AddAclFeature(new AclFeature(entries));
				}
				if (d.HasXAttrs())
				{
					dir.AddXAttrFeature(new XAttrFeature(LoadXAttrs(d.GetXAttrs(), state.GetStringTable
						())));
				}
				return dir;
			}

			public static void UpdateBlocksMap(INodeFile file, BlockManager bm)
			{
				// Add file->block mapping
				BlockInfoContiguous[] blocks = file.GetBlocks();
				if (blocks != null)
				{
					for (int i = 0; i < blocks.Length; i++)
					{
						file.SetBlock(i, bm.AddBlockCollection(blocks[i], file));
					}
				}
			}

			private readonly FSDirectory dir;

			private readonly FSNamesystem fsn;

			private readonly FSImageFormatProtobuf.Loader parent;

			internal Loader(FSNamesystem fsn, FSImageFormatProtobuf.Loader parent)
			{
				this.fsn = fsn;
				this.dir = fsn.dir;
				this.parent = parent;
			}

			/// <exception cref="System.IO.IOException"/>
			internal void LoadINodeDirectorySection(InputStream @in)
			{
				IList<INodeReference> refList = parent.GetLoaderContext().GetRefList();
				while (true)
				{
					FsImageProto.INodeDirectorySection.DirEntry e = FsImageProto.INodeDirectorySection.DirEntry
						.ParseDelimitedFrom(@in);
					// note that in is a LimitedInputStream
					if (e == null)
					{
						break;
					}
					INodeDirectory p = dir.GetInode(e.GetParent()).AsDirectory();
					foreach (long id in e.GetChildrenList())
					{
						INode child = dir.GetInode(id);
						AddToParent(p, child);
					}
					foreach (int refId in e.GetRefChildrenList())
					{
						INodeReference @ref = refList[refId];
						AddToParent(p, @ref);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal void LoadINodeSection(InputStream @in)
			{
				FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
				fsn.dir.ResetLastInodeId(s.GetLastInodeId());
				Log.Info("Loading " + s.GetNumInodes() + " INodes.");
				for (int i = 0; i < s.GetNumInodes(); ++i)
				{
					FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.ParseDelimitedFrom
						(@in);
					if (p.GetId() == INodeId.RootInodeId)
					{
						LoadRootINode(p);
					}
					else
					{
						INode n = LoadINode(p);
						dir.AddToInodeMap(n);
					}
				}
			}

			/// <summary>Load the under-construction files section, and update the lease map</summary>
			/// <exception cref="System.IO.IOException"/>
			internal void LoadFilesUnderConstructionSection(InputStream @in)
			{
				while (true)
				{
					FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry entry = FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry
						.ParseDelimitedFrom(@in);
					if (entry == null)
					{
						break;
					}
					// update the lease manager
					INodeFile file = dir.GetInode(entry.GetInodeId()).AsFile();
					FileUnderConstructionFeature uc = file.GetFileUnderConstructionFeature();
					Preconditions.CheckState(uc != null);
					// file must be under-construction
					fsn.leaseManager.AddLease(uc.GetClientName(), entry.GetFullPath());
				}
			}

			private void AddToParent(INodeDirectory parent, INode child)
			{
				if (parent == dir.rootDir && FSDirectory.IsReservedName(child))
				{
					throw new HadoopIllegalArgumentException("File name \"" + child.GetLocalName() + 
						"\" is reserved. Please " + " change the name of the existing file or directory to another "
						 + "name before upgrading to this release.");
				}
				// NOTE: This does not update space counts for parents
				if (!parent.AddChild(child))
				{
					return;
				}
				dir.CacheName(child);
				if (child.IsFile())
				{
					UpdateBlocksMap(child.AsFile(), fsn.GetBlockManager());
				}
			}

			private INode LoadINode(FsImageProto.INodeSection.INode n)
			{
				switch (n.GetType())
				{
					case FsImageProto.INodeSection.INode.Type.File:
					{
						return LoadINodeFile(n);
					}

					case FsImageProto.INodeSection.INode.Type.Directory:
					{
						return LoadINodeDirectory(n, parent.GetLoaderContext());
					}

					case FsImageProto.INodeSection.INode.Type.Symlink:
					{
						return LoadINodeSymlink(n);
					}

					default:
					{
						break;
					}
				}
				return null;
			}

			private INodeFile LoadINodeFile(FsImageProto.INodeSection.INode n)
			{
				System.Diagnostics.Debug.Assert(n.GetType() == FsImageProto.INodeSection.INode.Type
					.File);
				FsImageProto.INodeSection.INodeFile f = n.GetFile();
				IList<HdfsProtos.BlockProto> bp = f.GetBlocksList();
				short replication = (short)f.GetReplication();
				FSImageFormatProtobuf.LoaderContext state = parent.GetLoaderContext();
				BlockInfoContiguous[] blocks = new BlockInfoContiguous[bp.Count];
				for (int i = 0; i < e; ++i)
				{
					blocks[i] = new BlockInfoContiguous(PBHelper.Convert(bp[i]), replication);
				}
				PermissionStatus permissions = LoadPermission(f.GetPermission(), parent.GetLoaderContext
					().GetStringTable());
				INodeFile file = new INodeFile(n.GetId(), n.GetName().ToByteArray(), permissions, 
					f.GetModificationTime(), f.GetAccessTime(), blocks, replication, f.GetPreferredBlockSize
					(), unchecked((byte)f.GetStoragePolicyID()));
				if (f.HasAcl())
				{
					int[] entries = AclEntryStatusFormat.ToInt(LoadAclEntries(f.GetAcl(), state.GetStringTable
						()));
					file.AddAclFeature(new AclFeature(entries));
				}
				if (f.HasXAttrs())
				{
					file.AddXAttrFeature(new XAttrFeature(LoadXAttrs(f.GetXAttrs(), state.GetStringTable
						())));
				}
				// under-construction information
				if (f.HasFileUC())
				{
					FsImageProto.INodeSection.FileUnderConstructionFeature uc = f.GetFileUC();
					file.ToUnderConstruction(uc.GetClientName(), uc.GetClientMachine());
					if (blocks.Length > 0)
					{
						BlockInfoContiguous lastBlk = file.GetLastBlock();
						// replace the last block of file
						file.SetBlock(file.NumBlocks() - 1, new BlockInfoContiguousUnderConstruction(lastBlk
							, replication));
					}
				}
				return file;
			}

			private INodeSymlink LoadINodeSymlink(FsImageProto.INodeSection.INode n)
			{
				System.Diagnostics.Debug.Assert(n.GetType() == FsImageProto.INodeSection.INode.Type
					.Symlink);
				FsImageProto.INodeSection.INodeSymlink s = n.GetSymlink();
				PermissionStatus permissions = LoadPermission(s.GetPermission(), parent.GetLoaderContext
					().GetStringTable());
				INodeSymlink sym = new INodeSymlink(n.GetId(), n.GetName().ToByteArray(), permissions
					, s.GetModificationTime(), s.GetAccessTime(), s.GetTarget().ToStringUtf8());
				return sym;
			}

			private void LoadRootINode(FsImageProto.INodeSection.INode p)
			{
				INodeDirectory root = LoadINodeDirectory(p, parent.GetLoaderContext());
				QuotaCounts q = root.GetQuotaCounts();
				long nsQuota = q.GetNameSpace();
				long dsQuota = q.GetStorageSpace();
				if (nsQuota != -1 || dsQuota != -1)
				{
					dir.rootDir.GetDirectoryWithQuotaFeature().SetQuota(nsQuota, dsQuota);
				}
				EnumCounters<StorageType> typeQuotas = q.GetTypeSpaces();
				if (typeQuotas.AnyGreaterOrEqual(0))
				{
					dir.rootDir.GetDirectoryWithQuotaFeature().SetQuota(typeQuotas);
				}
				dir.rootDir.CloneModificationTime(root);
				dir.rootDir.ClonePermissionStatus(root);
				AclFeature af = root.GetFeature(typeof(AclFeature));
				if (af != null)
				{
					dir.rootDir.AddAclFeature(af);
				}
				// root dir supports having extended attributes according to POSIX
				XAttrFeature f = root.GetXAttrFeature();
				if (f != null)
				{
					dir.rootDir.AddXAttrFeature(f);
				}
				dir.AddRootDirToEncryptionZone(f);
			}
		}

		public sealed class Saver
		{
			private static long BuildPermissionStatus(INodeAttributes n, FSImageFormatProtobuf.SaverContext.DeduplicationMap
				<string> stringMap)
			{
				long userId = stringMap.GetId(n.GetUserName());
				long groupId = stringMap.GetId(n.GetGroupName());
				return ((userId & UserGroupStridMask) << UserStridOffset) | ((groupId & UserGroupStridMask
					) << GroupStridOffset) | n.GetFsPermissionShort();
			}

			private static FsImageProto.INodeSection.AclFeatureProto.Builder BuildAclEntries(
				AclFeature f, FSImageFormatProtobuf.SaverContext.DeduplicationMap<string> map)
			{
				FsImageProto.INodeSection.AclFeatureProto.Builder b = FsImageProto.INodeSection.AclFeatureProto
					.NewBuilder();
				for (int pos = 0; pos < f.GetEntriesSize(); pos++)
				{
					e = f.GetEntryAt(pos);
					int nameId = map.GetId(AclEntryStatusFormat.GetName(e));
					int v = ((nameId & AclEntryNameMask) << AclEntryNameOffset) | ((int)(AclEntryStatusFormat
						.GetType(e)) << AclEntryTypeOffset) | ((int)(AclEntryStatusFormat.GetScope(e)) <<
						 AclEntryScopeOffset) | ((int)(AclEntryStatusFormat.GetPermission(e)));
					b.AddEntries(v);
				}
				return b;
			}

			private static FsImageProto.INodeSection.XAttrFeatureProto.Builder BuildXAttrs(XAttrFeature
				 f, FSImageFormatProtobuf.SaverContext.DeduplicationMap<string> stringMap)
			{
				FsImageProto.INodeSection.XAttrFeatureProto.Builder b = FsImageProto.INodeSection.XAttrFeatureProto
					.NewBuilder();
				foreach (XAttr a in f.GetXAttrs())
				{
					FsImageProto.INodeSection.XAttrCompactProto.Builder xAttrCompactBuilder = FsImageProto.INodeSection.XAttrCompactProto
						.NewBuilder();
					int nsOrd = (int)(a.GetNameSpace());
					Preconditions.CheckArgument(nsOrd < 8, "Too many namespaces.");
					int v = ((nsOrd & XattrNamespaceMask) << XattrNamespaceOffset) | ((stringMap.GetId
						(a.GetName()) & XattrNameMask) << XattrNameOffset);
					v |= (((nsOrd >> 2) & XattrNamespaceExtMask) << XattrNamespaceExtOffset);
					xAttrCompactBuilder.SetName(v);
					if (a.GetValue() != null)
					{
						xAttrCompactBuilder.SetValue(PBHelper.GetByteString(a.GetValue()));
					}
					b.AddXAttrs(((FsImageProto.INodeSection.XAttrCompactProto)xAttrCompactBuilder.Build
						()));
				}
				return b;
			}

			private static FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto.Builder BuildQuotaByStorageTypeEntries
				(QuotaCounts q)
			{
				FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto.Builder b = FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto
					.NewBuilder();
				foreach (StorageType t in StorageType.GetTypesSupportingQuota())
				{
					if (q.GetTypeSpace(t) >= 0)
					{
						FsImageProto.INodeSection.QuotaByStorageTypeEntryProto.Builder eb = FsImageProto.INodeSection.QuotaByStorageTypeEntryProto
							.NewBuilder().SetStorageType(PBHelper.ConvertStorageType(t)).SetQuota(q.GetTypeSpace
							(t));
						b.AddQuotas(eb);
					}
				}
				return b;
			}

			public static FsImageProto.INodeSection.INodeFile.Builder BuildINodeFile(INodeFileAttributes
				 file, FSImageFormatProtobuf.SaverContext state)
			{
				FsImageProto.INodeSection.INodeFile.Builder b = FsImageProto.INodeSection.INodeFile
					.NewBuilder().SetAccessTime(file.GetAccessTime()).SetModificationTime(file.GetModificationTime
					()).SetPermission(BuildPermissionStatus(file, state.GetStringMap())).SetPreferredBlockSize
					(file.GetPreferredBlockSize()).SetReplication(file.GetFileReplication()).SetStoragePolicyID
					(file.GetLocalStoragePolicyID());
				AclFeature f = file.GetAclFeature();
				if (f != null)
				{
					b.SetAcl(BuildAclEntries(f, state.GetStringMap()));
				}
				XAttrFeature xAttrFeature = file.GetXAttrFeature();
				if (xAttrFeature != null)
				{
					b.SetXAttrs(BuildXAttrs(xAttrFeature, state.GetStringMap()));
				}
				return b;
			}

			public static FsImageProto.INodeSection.INodeDirectory.Builder BuildINodeDirectory
				(INodeDirectoryAttributes dir, FSImageFormatProtobuf.SaverContext state)
			{
				QuotaCounts quota = dir.GetQuotaCounts();
				FsImageProto.INodeSection.INodeDirectory.Builder b = FsImageProto.INodeSection.INodeDirectory
					.NewBuilder().SetModificationTime(dir.GetModificationTime()).SetNsQuota(quota.GetNameSpace
					()).SetDsQuota(quota.GetStorageSpace()).SetPermission(BuildPermissionStatus(dir, 
					state.GetStringMap()));
				if (quota.GetTypeSpaces().AnyGreaterOrEqual(0))
				{
					b.SetTypeQuotas(BuildQuotaByStorageTypeEntries(quota));
				}
				AclFeature f = dir.GetAclFeature();
				if (f != null)
				{
					b.SetAcl(BuildAclEntries(f, state.GetStringMap()));
				}
				XAttrFeature xAttrFeature = dir.GetXAttrFeature();
				if (xAttrFeature != null)
				{
					b.SetXAttrs(BuildXAttrs(xAttrFeature, state.GetStringMap()));
				}
				return b;
			}

			private readonly FSNamesystem fsn;

			private readonly FsImageProto.FileSummary.Builder summary;

			private readonly SaveNamespaceContext context;

			private readonly FSImageFormatProtobuf.Saver parent;

			internal Saver(FSImageFormatProtobuf.Saver parent, FsImageProto.FileSummary.Builder
				 summary)
			{
				this.parent = parent;
				this.summary = summary;
				this.context = parent.GetContext();
				this.fsn = context.GetSourceNamesystem();
			}

			/// <exception cref="System.IO.IOException"/>
			internal void SerializeINodeDirectorySection(OutputStream @out)
			{
				IEnumerator<INodeWithAdditionalFields> iter = fsn.GetFSDirectory().GetINodeMap().
					GetMapIterator();
				AList<INodeReference> refList = parent.GetSaverContext().GetRefList();
				int i = 0;
				while (iter.HasNext())
				{
					INodeWithAdditionalFields n = iter.Next();
					if (!n.IsDirectory())
					{
						continue;
					}
					ReadOnlyList<INode> children = n.AsDirectory().GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					if (children.Size() > 0)
					{
						FsImageProto.INodeDirectorySection.DirEntry.Builder b = FsImageProto.INodeDirectorySection.DirEntry
							.NewBuilder().SetParent(n.GetId());
						foreach (INode inode in children)
						{
							if (!inode.IsReference())
							{
								b.AddChildren(inode.GetId());
							}
							else
							{
								refList.AddItem(inode.AsReference());
								b.AddRefChildren(refList.Count - 1);
							}
						}
						FsImageProto.INodeDirectorySection.DirEntry e = ((FsImageProto.INodeDirectorySection.DirEntry
							)b.Build());
						e.WriteDelimitedTo(@out);
					}
					++i;
					if (i % FSImageFormatProtobuf.Saver.CheckCancelInterval == 0)
					{
						context.CheckCancelled();
					}
				}
				parent.CommitSection(summary, FSImageFormatProtobuf.SectionName.InodeDir);
			}

			/// <exception cref="System.IO.IOException"/>
			internal void SerializeINodeSection(OutputStream @out)
			{
				INodeMap inodesMap = fsn.dir.GetINodeMap();
				FsImageProto.INodeSection.Builder b = FsImageProto.INodeSection.NewBuilder().SetLastInodeId
					(fsn.dir.GetLastInodeId()).SetNumInodes(inodesMap.Size());
				FsImageProto.INodeSection s = ((FsImageProto.INodeSection)b.Build());
				s.WriteDelimitedTo(@out);
				int i = 0;
				IEnumerator<INodeWithAdditionalFields> iter = inodesMap.GetMapIterator();
				while (iter.HasNext())
				{
					INodeWithAdditionalFields n = iter.Next();
					Save(@out, n);
					++i;
					if (i % FSImageFormatProtobuf.Saver.CheckCancelInterval == 0)
					{
						context.CheckCancelled();
					}
				}
				parent.CommitSection(summary, FSImageFormatProtobuf.SectionName.Inode);
			}

			/// <exception cref="System.IO.IOException"/>
			internal void SerializeFilesUCSection(OutputStream @out)
			{
				IDictionary<string, INodeFile> ucMap = fsn.GetFilesUnderConstruction();
				foreach (KeyValuePair<string, INodeFile> entry in ucMap)
				{
					string path = entry.Key;
					INodeFile file = entry.Value;
					FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry.Builder b = 
						FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry.NewBuilder
						().SetInodeId(file.GetId()).SetFullPath(path);
					FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry e = ((FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry
						)b.Build());
					e.WriteDelimitedTo(@out);
				}
				parent.CommitSection(summary, FSImageFormatProtobuf.SectionName.FilesUnderconstruction
					);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Save(OutputStream @out, INode n)
			{
				if (n.IsDirectory())
				{
					Save(@out, n.AsDirectory());
				}
				else
				{
					if (n.IsFile())
					{
						Save(@out, n.AsFile());
					}
					else
					{
						if (n.IsSymlink())
						{
							Save(@out, n.AsSymlink());
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void Save(OutputStream @out, INodeDirectory n)
			{
				FsImageProto.INodeSection.INodeDirectory.Builder b = BuildINodeDirectory(n, parent
					.GetSaverContext());
				FsImageProto.INodeSection.INode r = ((FsImageProto.INodeSection.INode)BuildINodeCommon
					(n).SetType(FsImageProto.INodeSection.INode.Type.Directory).SetDirectory(b).Build
					());
				r.WriteDelimitedTo(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Save(OutputStream @out, INodeFile n)
			{
				FsImageProto.INodeSection.INodeFile.Builder b = BuildINodeFile(n, parent.GetSaverContext
					());
				if (n.GetBlocks() != null)
				{
					foreach (Block block in n.GetBlocks())
					{
						b.AddBlocks(PBHelper.Convert(block));
					}
				}
				FileUnderConstructionFeature uc = n.GetFileUnderConstructionFeature();
				if (uc != null)
				{
					FsImageProto.INodeSection.FileUnderConstructionFeature f = ((FsImageProto.INodeSection.FileUnderConstructionFeature
						)FsImageProto.INodeSection.FileUnderConstructionFeature.NewBuilder().SetClientName
						(uc.GetClientName()).SetClientMachine(uc.GetClientMachine()).Build());
					b.SetFileUC(f);
				}
				FsImageProto.INodeSection.INode r = ((FsImageProto.INodeSection.INode)BuildINodeCommon
					(n).SetType(FsImageProto.INodeSection.INode.Type.File).SetFile(b).Build());
				r.WriteDelimitedTo(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Save(OutputStream @out, INodeSymlink n)
			{
				FSImageFormatProtobuf.SaverContext state = parent.GetSaverContext();
				FsImageProto.INodeSection.INodeSymlink.Builder b = FsImageProto.INodeSection.INodeSymlink
					.NewBuilder().SetPermission(BuildPermissionStatus(n, state.GetStringMap())).SetTarget
					(ByteString.CopyFrom(n.GetSymlink())).SetModificationTime(n.GetModificationTime(
					)).SetAccessTime(n.GetAccessTime());
				FsImageProto.INodeSection.INode r = ((FsImageProto.INodeSection.INode)BuildINodeCommon
					(n).SetType(FsImageProto.INodeSection.INode.Type.Symlink).SetSymlink(b).Build());
				r.WriteDelimitedTo(@out);
			}

			private FsImageProto.INodeSection.INode.Builder BuildINodeCommon(INode n)
			{
				return FsImageProto.INodeSection.INode.NewBuilder().SetId(n.GetId()).SetName(ByteString
					.CopyFrom(n.GetLocalNameBytes()));
			}
		}

		private FSImageFormatPBINode()
		{
		}
	}
}
