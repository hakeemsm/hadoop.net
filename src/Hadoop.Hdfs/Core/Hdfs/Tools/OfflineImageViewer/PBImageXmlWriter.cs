using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// PBImageXmlWriter walks over an fsimage structure and writes out
	/// an equivalent XML document that contains the fsimage's components.
	/// </summary>
	public sealed class PBImageXmlWriter
	{
		private readonly Configuration conf;

		private readonly TextWriter @out;

		private string[] stringTable;

		public PBImageXmlWriter(Configuration conf, TextWriter @out)
		{
			this.conf = conf;
			this.@out = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public void Visit(RandomAccessFile file)
		{
			if (!FSImageUtil.CheckFileFormat(file))
			{
				throw new IOException("Unrecognized FSImage");
			}
			FsImageProto.FileSummary summary = FSImageUtil.LoadSummary(file);
			using (FileInputStream fin = new FileInputStream(file.GetFD()))
			{
				@out.Write("<?xml version=\"1.0\"?>\n<fsimage>");
				AList<FsImageProto.FileSummary.Section> sections = Lists.NewArrayList(summary.GetSectionsList
					());
				sections.Sort(new _IComparer_83());
				foreach (FsImageProto.FileSummary.Section s in sections)
				{
					fin.GetChannel().Position(s.GetOffset());
					InputStream @is = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec
						(), new BufferedInputStream(new LimitInputStream(fin, s.GetLength())));
					switch (FSImageFormatProtobuf.SectionName.FromString(s.GetName()))
					{
						case FSImageFormatProtobuf.SectionName.NsInfo:
						{
							DumpNameSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.StringTable:
						{
							LoadStringTable(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.Inode:
						{
							DumpINodeSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeReference:
						{
							DumpINodeReferenceSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeDir:
						{
							DumpINodeDirectorySection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.FilesUnderconstruction:
						{
							DumpFileUnderConstructionSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.Snapshot:
						{
							DumpSnapshotSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.SnapshotDiff:
						{
							DumpSnapshotDiffSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.SecretManager:
						{
							DumpSecretManagerSection(@is);
							break;
						}

						case FSImageFormatProtobuf.SectionName.CacheManager:
						{
							DumpCacheManagerSection(@is);
							break;
						}

						default:
						{
							break;
						}
					}
				}
				@out.Write("</fsimage>\n");
			}
		}

		private sealed class _IComparer_83 : IComparer<FsImageProto.FileSummary.Section>
		{
			public _IComparer_83()
			{
			}

			public int Compare(FsImageProto.FileSummary.Section s1, FsImageProto.FileSummary.Section
				 s2)
			{
				FSImageFormatProtobuf.SectionName n1 = FSImageFormatProtobuf.SectionName.FromString
					(s1.GetName());
				FSImageFormatProtobuf.SectionName n2 = FSImageFormatProtobuf.SectionName.FromString
					(s2.GetName());
				if (n1 == null)
				{
					return n2 == null ? 0 : -1;
				}
				else
				{
					if (n2 == null)
					{
						return -1;
					}
					else
					{
						return (int)(n1) - (int)(n2);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpCacheManagerSection(InputStream @is)
		{
			@out.Write("<CacheManagerSection>");
			FsImageProto.CacheManagerSection s = FsImageProto.CacheManagerSection.ParseDelimitedFrom
				(@is);
			O("nextDirectiveId", s.GetNextDirectiveId());
			for (int i = 0; i < s.GetNumPools(); ++i)
			{
				ClientNamenodeProtocolProtos.CachePoolInfoProto p = ClientNamenodeProtocolProtos.CachePoolInfoProto
					.ParseDelimitedFrom(@is);
				@out.Write("<pool>");
				O("poolName", p.GetPoolName()).O("ownerName", p.GetOwnerName()).O("groupName", p.
					GetGroupName()).O("mode", p.GetMode()).O("limit", p.GetLimit()).O("maxRelativeExpiry"
					, p.GetMaxRelativeExpiry());
				@out.Write("</pool>\n");
			}
			for (int i_1 = 0; i_1 < s.GetNumDirectives(); ++i_1)
			{
				ClientNamenodeProtocolProtos.CacheDirectiveInfoProto p = ClientNamenodeProtocolProtos.CacheDirectiveInfoProto
					.ParseDelimitedFrom(@is);
				@out.Write("<directive>");
				O("id", p.GetId()).O("path", p.GetPath()).O("replication", p.GetReplication()).O(
					"pool", p.GetPool());
				@out.Write("<expiration>");
				ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto e = p.GetExpiration
					();
				O("millis", e.GetMillis()).O("relatilve", e.GetIsRelative());
				@out.Write("</expiration>\n");
				@out.Write("</directive>\n");
			}
			@out.Write("</CacheManagerSection>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpFileUnderConstructionSection(InputStream @in)
		{
			@out.Write("<FileUnderConstructionSection>");
			while (true)
			{
				FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry e = FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry
					.ParseDelimitedFrom(@in);
				if (e == null)
				{
					break;
				}
				@out.Write("<inode>");
				O("id", e.GetInodeId()).O("path", e.GetFullPath());
				@out.Write("</inode>\n");
			}
			@out.Write("</FileUnderConstructionSection>\n");
		}

		private void DumpINodeDirectory(FsImageProto.INodeSection.INodeDirectory d)
		{
			O("mtime", d.GetModificationTime()).O("permission", DumpPermission(d.GetPermission
				()));
			if (d.HasDsQuota() && d.HasNsQuota())
			{
				O("nsquota", d.GetNsQuota()).O("dsquota", d.GetDsQuota());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpINodeDirectorySection(InputStream @in)
		{
			@out.Write("<INodeDirectorySection>");
			while (true)
			{
				FsImageProto.INodeDirectorySection.DirEntry e = FsImageProto.INodeDirectorySection.DirEntry
					.ParseDelimitedFrom(@in);
				// note that in is a LimitedInputStream
				if (e == null)
				{
					break;
				}
				@out.Write("<directory>");
				O("parent", e.GetParent());
				foreach (long id in e.GetChildrenList())
				{
					O("inode", id);
				}
				foreach (int refId in e.GetRefChildrenList())
				{
					O("inodereference-index", refId);
				}
				@out.Write("</directory>\n");
			}
			@out.Write("</INodeDirectorySection>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpINodeReferenceSection(InputStream @in)
		{
			@out.Write("<INodeReferenceSection>");
			while (true)
			{
				FsImageProto.INodeReferenceSection.INodeReference e = FsImageProto.INodeReferenceSection.INodeReference
					.ParseDelimitedFrom(@in);
				if (e == null)
				{
					break;
				}
				DumpINodeReference(e);
			}
			@out.Write("</INodeReferenceSection>");
		}

		private void DumpINodeReference(FsImageProto.INodeReferenceSection.INodeReference
			 r)
		{
			@out.Write("<ref>");
			O("referredId", r.GetReferredId()).O("name", r.GetName().ToStringUtf8()).O("dstSnapshotId"
				, r.GetDstSnapshotId()).O("lastSnapshotId", r.GetLastSnapshotId());
			@out.Write("</ref>\n");
		}

		private void DumpINodeFile(FsImageProto.INodeSection.INodeFile f)
		{
			O("replication", f.GetReplication()).O("mtime", f.GetModificationTime()).O("atime"
				, f.GetAccessTime()).O("perferredBlockSize", f.GetPreferredBlockSize()).O("permission"
				, DumpPermission(f.GetPermission()));
			if (f.GetBlocksCount() > 0)
			{
				@out.Write("<blocks>");
				foreach (HdfsProtos.BlockProto b in f.GetBlocksList())
				{
					@out.Write("<block>");
					O("id", b.GetBlockId()).O("genstamp", b.GetGenStamp()).O("numBytes", b.GetNumBytes
						());
					@out.Write("</block>\n");
				}
				@out.Write("</blocks>\n");
			}
			if (f.HasFileUC())
			{
				FsImageProto.INodeSection.FileUnderConstructionFeature u = f.GetFileUC();
				@out.Write("<file-under-construction>");
				O("clientName", u.GetClientName()).O("clientMachine", u.GetClientMachine());
				@out.Write("</file-under-construction>\n");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpINodeSection(InputStream @in)
		{
			FsImageProto.INodeSection s = FsImageProto.INodeSection.ParseDelimitedFrom(@in);
			@out.Write("<INodeSection>");
			O("lastInodeId", s.GetLastInodeId());
			for (int i = 0; i < s.GetNumInodes(); ++i)
			{
				FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.ParseDelimitedFrom
					(@in);
				@out.Write("<inode>");
				O("id", p.GetId()).O("type", p.GetType()).O("name", p.GetName().ToStringUtf8());
				if (p.HasFile())
				{
					DumpINodeFile(p.GetFile());
				}
				else
				{
					if (p.HasDirectory())
					{
						DumpINodeDirectory(p.GetDirectory());
					}
					else
					{
						if (p.HasSymlink())
						{
							DumpINodeSymlink(p.GetSymlink());
						}
					}
				}
				@out.Write("</inode>\n");
			}
			@out.Write("</INodeSection>\n");
		}

		private void DumpINodeSymlink(FsImageProto.INodeSection.INodeSymlink s)
		{
			O("permission", DumpPermission(s.GetPermission())).O("target", s.GetTarget().ToStringUtf8
				()).O("mtime", s.GetModificationTime()).O("atime", s.GetAccessTime());
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpNameSection(InputStream @in)
		{
			FsImageProto.NameSystemSection s = FsImageProto.NameSystemSection.ParseDelimitedFrom
				(@in);
			@out.Write("<NameSection>\n");
			O("genstampV1", s.GetGenstampV1()).O("genstampV2", s.GetGenstampV2()).O("genstampV1Limit"
				, s.GetGenstampV1Limit()).O("lastAllocatedBlockId", s.GetLastAllocatedBlockId())
				.O("txid", s.GetTransactionId());
			@out.Write("</NameSection>\n");
		}

		private string DumpPermission(long permission)
		{
			return FSImageFormatPBINode.Loader.LoadPermission(permission, stringTable).ToString
				();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpSecretManagerSection(InputStream @is)
		{
			@out.Write("<SecretManagerSection>");
			FsImageProto.SecretManagerSection s = FsImageProto.SecretManagerSection.ParseDelimitedFrom
				(@is);
			O("currentId", s.GetCurrentId()).O("tokenSequenceNumber", s.GetTokenSequenceNumber
				());
			@out.Write("</SecretManagerSection>");
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpSnapshotDiffSection(InputStream @in)
		{
			@out.Write("<SnapshotDiffSection>");
			while (true)
			{
				FsImageProto.SnapshotDiffSection.DiffEntry e = FsImageProto.SnapshotDiffSection.DiffEntry
					.ParseDelimitedFrom(@in);
				if (e == null)
				{
					break;
				}
				@out.Write("<diff>");
				O("inodeid", e.GetInodeId());
				switch (e.GetType())
				{
					case FsImageProto.SnapshotDiffSection.DiffEntry.Type.Filediff:
					{
						for (int i = 0; i < e.GetNumOfDiff(); ++i)
						{
							@out.Write("<filediff>");
							FsImageProto.SnapshotDiffSection.FileDiff f = FsImageProto.SnapshotDiffSection.FileDiff
								.ParseDelimitedFrom(@in);
							O("snapshotId", f.GetSnapshotId()).O("size", f.GetFileSize()).O("name", f.GetName
								().ToStringUtf8());
							@out.Write("</filediff>\n");
						}
						break;
					}

					case FsImageProto.SnapshotDiffSection.DiffEntry.Type.Directorydiff:
					{
						for (int i = 0; i < e.GetNumOfDiff(); ++i)
						{
							@out.Write("<dirdiff>");
							FsImageProto.SnapshotDiffSection.DirectoryDiff d = FsImageProto.SnapshotDiffSection.DirectoryDiff
								.ParseDelimitedFrom(@in);
							O("snapshotId", d.GetSnapshotId()).O("isSnapshotroot", d.GetIsSnapshotRoot()).O("childrenSize"
								, d.GetChildrenSize()).O("name", d.GetName().ToStringUtf8());
							for (int j = 0; j < d.GetCreatedListSize(); ++j)
							{
								FsImageProto.SnapshotDiffSection.CreatedListEntry ce = FsImageProto.SnapshotDiffSection.CreatedListEntry
									.ParseDelimitedFrom(@in);
								@out.Write("<created>");
								O("name", ce.GetName().ToStringUtf8());
								@out.Write("</created>\n");
							}
							foreach (long did in d.GetDeletedINodeList())
							{
								@out.Write("<deleted>");
								O("inode", did);
								@out.Write("</deleted>\n");
							}
							foreach (int dRefid in d.GetDeletedINodeRefList())
							{
								@out.Write("<deleted>");
								O("inodereference-index", dRefid);
								@out.Write("</deleted>\n");
							}
							@out.Write("</dirdiff>\n");
						}
						break;
					}

					default:
					{
						break;
					}
				}
				@out.Write("</diff>");
			}
			@out.Write("</SnapshotDiffSection>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void DumpSnapshotSection(InputStream @in)
		{
			@out.Write("<SnapshotSection>");
			FsImageProto.SnapshotSection s = FsImageProto.SnapshotSection.ParseDelimitedFrom(
				@in);
			O("snapshotCounter", s.GetSnapshotCounter());
			if (s.GetSnapshottableDirCount() > 0)
			{
				@out.Write("<snapshottableDir>");
				foreach (long id in s.GetSnapshottableDirList())
				{
					O("dir", id);
				}
				@out.Write("</snapshottableDir>\n");
			}
			for (int i = 0; i < s.GetNumSnapshots(); ++i)
			{
				FsImageProto.SnapshotSection.Snapshot pbs = FsImageProto.SnapshotSection.Snapshot
					.ParseDelimitedFrom(@in);
				O("snapshot", pbs.GetSnapshotId());
			}
			@out.Write("</SnapshotSection>\n");
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadStringTable(InputStream @in)
		{
			FsImageProto.StringTableSection s = FsImageProto.StringTableSection.ParseDelimitedFrom
				(@in);
			stringTable = new string[s.GetNumEntry() + 1];
			for (int i = 0; i < s.GetNumEntry(); ++i)
			{
				FsImageProto.StringTableSection.Entry e = FsImageProto.StringTableSection.Entry.ParseDelimitedFrom
					(@in);
				stringTable[e.GetId()] = e.GetStr();
			}
		}

		private Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.PBImageXmlWriter O(string
			 e, object v)
		{
			@out.Write("<" + e + ">" + XMLUtils.MangleXmlString(v.ToString(), true) + "</" + 
				e + ">");
			return this;
		}
	}
}
