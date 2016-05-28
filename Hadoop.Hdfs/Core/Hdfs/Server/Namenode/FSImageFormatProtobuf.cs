using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Utility class to read / write fsimage in protobuf format.</summary>
	public sealed class FSImageFormatProtobuf
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageFormatProtobuf
			));

		public sealed class LoaderContext
		{
			private string[] stringTable;

			private readonly AList<INodeReference> refList = Lists.NewArrayList();

			public string[] GetStringTable()
			{
				return stringTable;
			}

			public AList<INodeReference> GetRefList()
			{
				return refList;
			}
		}

		public sealed class SaverContext
		{
			public class DeduplicationMap<E>
			{
				private readonly IDictionary<E, int> map = Maps.NewHashMap();

				private DeduplicationMap()
				{
				}

				internal static FSImageFormatProtobuf.SaverContext.DeduplicationMap<T> NewMap<T>(
					)
				{
					return new FSImageFormatProtobuf.SaverContext.DeduplicationMap<T>();
				}

				internal virtual int GetId(E value)
				{
					if (value == null)
					{
						return 0;
					}
					int v = map[value];
					if (v == null)
					{
						int nv = map.Count + 1;
						map[value] = nv;
						return nv;
					}
					return v;
				}

				internal virtual int Size()
				{
					return map.Count;
				}

				internal virtual ICollection<KeyValuePair<E, int>> EntrySet()
				{
					return map;
				}
			}

			private readonly AList<INodeReference> refList = Lists.NewArrayList();

			private readonly FSImageFormatProtobuf.SaverContext.DeduplicationMap<string> stringMap
				 = FSImageFormatProtobuf.SaverContext.DeduplicationMap.NewMap();

			public FSImageFormatProtobuf.SaverContext.DeduplicationMap<string> GetStringMap()
			{
				return stringMap;
			}

			public AList<INodeReference> GetRefList()
			{
				return refList;
			}
		}

		public sealed class Loader : FSImageFormat.AbstractLoader
		{
			internal const int MinimumFileLength = 8;

			private readonly Configuration conf;

			private readonly FSNamesystem fsn;

			private readonly FSImageFormatProtobuf.LoaderContext ctx;

			/// <summary>The MD5 sum of the loaded file</summary>
			private MD5Hash imgDigest;

			/// <summary>The transaction ID of the last edit represented by the loaded file</summary>
			private long imgTxId;

			/// <summary>
			/// Whether the image's layout version must be the same with
			/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.NamenodeLayoutVersion"/>
			/// . This is only set to true
			/// when we're doing (rollingUpgrade rollback).
			/// </summary>
			private readonly bool requireSameLayoutVersion;

			internal Loader(Configuration conf, FSNamesystem fsn, bool requireSameLayoutVersion
				)
			{
				this.conf = conf;
				this.fsn = fsn;
				this.ctx = new FSImageFormatProtobuf.LoaderContext();
				this.requireSameLayoutVersion = requireSameLayoutVersion;
			}

			public MD5Hash GetLoadedImageMd5()
			{
				return imgDigest;
			}

			public long GetLoadedImageTxId()
			{
				return imgTxId;
			}

			public FSImageFormatProtobuf.LoaderContext GetLoaderContext()
			{
				return ctx;
			}

			/// <exception cref="System.IO.IOException"/>
			internal void Load(FilePath file)
			{
				long start = Time.MonotonicNow();
				imgDigest = MD5FileUtils.ComputeMd5ForFile(file);
				RandomAccessFile raFile = new RandomAccessFile(file, "r");
				FileInputStream fin = new FileInputStream(file);
				try
				{
					LoadInternal(raFile, fin);
					long end = Time.MonotonicNow();
					Log.Info("Loaded FSImage in " + (end - start) / 1000 + " seconds.");
				}
				finally
				{
					fin.Close();
					raFile.Close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadInternal(RandomAccessFile raFile, FileInputStream fin)
			{
				if (!FSImageUtil.CheckFileFormat(raFile))
				{
					throw new IOException("Unrecognized file format");
				}
				FsImageProto.FileSummary summary = FSImageUtil.LoadSummary(raFile);
				if (requireSameLayoutVersion && summary.GetLayoutVersion() != HdfsConstants.NamenodeLayoutVersion)
				{
					throw new IOException("Image version " + summary.GetLayoutVersion() + " is not equal to the software version "
						 + HdfsConstants.NamenodeLayoutVersion);
				}
				FileChannel channel = fin.GetChannel();
				FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(fsn, this
					);
				FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader
					(fsn, this);
				AList<FsImageProto.FileSummary.Section> sections = Lists.NewArrayList(summary.GetSectionsList
					());
				sections.Sort(new _IComparer_210());
				StartupProgress prog = NameNode.GetStartupProgress();
				Step currentStep = null;
				foreach (FsImageProto.FileSummary.Section s in sections)
				{
					channel.Position(s.GetOffset());
					InputStream @in = new BufferedInputStream(new LimitInputStream(fin, s.GetLength()
						));
					@in = FSImageUtil.WrapInputStreamForCompression(conf, summary.GetCodec(), @in);
					string n = s.GetName();
					switch (FSImageFormatProtobuf.SectionName.FromString(n))
					{
						case FSImageFormatProtobuf.SectionName.NsInfo:
						{
							LoadNameSystemSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.StringTable:
						{
							LoadStringTableSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.Inode:
						{
							currentStep = new Step(StepType.Inodes);
							prog.BeginStep(Phase.LoadingFsimage, currentStep);
							inodeLoader.LoadINodeSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeReference:
						{
							snapshotLoader.LoadINodeReferenceSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.InodeDir:
						{
							inodeLoader.LoadINodeDirectorySection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.FilesUnderconstruction:
						{
							inodeLoader.LoadFilesUnderConstructionSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.Snapshot:
						{
							snapshotLoader.LoadSnapshotSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.SnapshotDiff:
						{
							snapshotLoader.LoadSnapshotDiffSection(@in);
							break;
						}

						case FSImageFormatProtobuf.SectionName.SecretManager:
						{
							prog.EndStep(Phase.LoadingFsimage, currentStep);
							Step step = new Step(StepType.DelegationTokens);
							prog.BeginStep(Phase.LoadingFsimage, step);
							LoadSecretManagerSection(@in);
							prog.EndStep(Phase.LoadingFsimage, step);
							break;
						}

						case FSImageFormatProtobuf.SectionName.CacheManager:
						{
							Step step = new Step(StepType.CachePools);
							prog.BeginStep(Phase.LoadingFsimage, step);
							LoadCacheManagerSection(@in);
							prog.EndStep(Phase.LoadingFsimage, step);
							break;
						}

						default:
						{
							Log.Warn("Unrecognized section " + n);
							break;
						}
					}
				}
			}

			private sealed class _IComparer_210 : IComparer<FsImageProto.FileSummary.Section>
			{
				public _IComparer_210()
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
			private void LoadNameSystemSection(InputStream @in)
			{
				FsImageProto.NameSystemSection s = FsImageProto.NameSystemSection.ParseDelimitedFrom
					(@in);
				BlockIdManager blockIdManager = fsn.GetBlockIdManager();
				blockIdManager.SetGenerationStampV1(s.GetGenstampV1());
				blockIdManager.SetGenerationStampV2(s.GetGenstampV2());
				blockIdManager.SetGenerationStampV1Limit(s.GetGenstampV1Limit());
				blockIdManager.SetLastAllocatedBlockId(s.GetLastAllocatedBlockId());
				imgTxId = s.GetTransactionId();
				if (s.HasRollingUpgradeStartTime() && fsn.GetFSImage().HasRollbackFSImage())
				{
					// we set the rollingUpgradeInfo only when we make sure we have the
					// rollback image
					fsn.SetRollingUpgradeInfo(true, s.GetRollingUpgradeStartTime());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadStringTableSection(InputStream @in)
			{
				FsImageProto.StringTableSection s = FsImageProto.StringTableSection.ParseDelimitedFrom
					(@in);
				ctx.stringTable = new string[s.GetNumEntry() + 1];
				for (int i = 0; i < s.GetNumEntry(); ++i)
				{
					FsImageProto.StringTableSection.Entry e = FsImageProto.StringTableSection.Entry.ParseDelimitedFrom
						(@in);
					ctx.stringTable[e.GetId()] = e.GetStr();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadSecretManagerSection(InputStream @in)
			{
				FsImageProto.SecretManagerSection s = FsImageProto.SecretManagerSection.ParseDelimitedFrom
					(@in);
				int numKeys = s.GetNumKeys();
				int numTokens = s.GetNumTokens();
				AList<FsImageProto.SecretManagerSection.DelegationKey> keys = Lists.NewArrayListWithCapacity
					(numKeys);
				AList<FsImageProto.SecretManagerSection.PersistToken> tokens = Lists.NewArrayListWithCapacity
					(numTokens);
				for (int i = 0; i < numKeys; ++i)
				{
					keys.AddItem(FsImageProto.SecretManagerSection.DelegationKey.ParseDelimitedFrom(@in
						));
				}
				for (int i_1 = 0; i_1 < numTokens; ++i_1)
				{
					tokens.AddItem(FsImageProto.SecretManagerSection.PersistToken.ParseDelimitedFrom(
						@in));
				}
				fsn.LoadSecretManagerState(s, keys, tokens);
			}

			/// <exception cref="System.IO.IOException"/>
			private void LoadCacheManagerSection(InputStream @in)
			{
				FsImageProto.CacheManagerSection s = FsImageProto.CacheManagerSection.ParseDelimitedFrom
					(@in);
				AList<ClientNamenodeProtocolProtos.CachePoolInfoProto> pools = Lists.NewArrayListWithCapacity
					(s.GetNumPools());
				AList<ClientNamenodeProtocolProtos.CacheDirectiveInfoProto> directives = Lists.NewArrayListWithCapacity
					(s.GetNumDirectives());
				for (int i = 0; i < s.GetNumPools(); ++i)
				{
					pools.AddItem(ClientNamenodeProtocolProtos.CachePoolInfoProto.ParseDelimitedFrom(
						@in));
				}
				for (int i_1 = 0; i_1 < s.GetNumDirectives(); ++i_1)
				{
					directives.AddItem(ClientNamenodeProtocolProtos.CacheDirectiveInfoProto.ParseDelimitedFrom
						(@in));
				}
				fsn.GetCacheManager().LoadState(new CacheManager.PersistState(s, pools, directives
					));
			}
		}

		public sealed class Saver
		{
			public const int CheckCancelInterval = 4096;

			private readonly SaveNamespaceContext context;

			private readonly FSImageFormatProtobuf.SaverContext saverContext;

			private long currentOffset = FSImageUtil.MagicHeader.Length;

			private MD5Hash savedDigest;

			private FileChannel fileChannel;

			private OutputStream sectionOutputStream;

			private CompressionCodec codec;

			private OutputStream underlyingOutputStream;

			internal Saver(SaveNamespaceContext context)
			{
				// OutputStream for the section data
				this.context = context;
				this.saverContext = new FSImageFormatProtobuf.SaverContext();
			}

			public MD5Hash GetSavedDigest()
			{
				return savedDigest;
			}

			public SaveNamespaceContext GetContext()
			{
				return context;
			}

			public FSImageFormatProtobuf.SaverContext GetSaverContext()
			{
				return saverContext;
			}

			/// <exception cref="System.IO.IOException"/>
			public void CommitSection(FsImageProto.FileSummary.Builder summary, FSImageFormatProtobuf.SectionName
				 name)
			{
				long oldOffset = currentOffset;
				FlushSectionOutputStream();
				if (codec != null)
				{
					sectionOutputStream = codec.CreateOutputStream(underlyingOutputStream);
				}
				else
				{
					sectionOutputStream = underlyingOutputStream;
				}
				long length = fileChannel.Position() - oldOffset;
				summary.AddSections(FsImageProto.FileSummary.Section.NewBuilder().SetName(name.name
					).SetLength(length).SetOffset(currentOffset));
				currentOffset += length;
			}

			/// <exception cref="System.IO.IOException"/>
			private void FlushSectionOutputStream()
			{
				if (codec != null)
				{
					((CompressorStream)sectionOutputStream).Finish();
				}
				sectionOutputStream.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			internal void Save(FilePath file, FSImageCompression compression)
			{
				FileOutputStream fout = new FileOutputStream(file);
				fileChannel = fout.GetChannel();
				try
				{
					SaveInternal(fout, compression, file.GetAbsolutePath());
				}
				finally
				{
					fout.Close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private static void SaveFileSummary(OutputStream @out, FsImageProto.FileSummary summary
				)
			{
				summary.WriteDelimitedTo(@out);
				int length = GetOndiskTrunkSize(summary);
				byte[] lengthBytes = new byte[4];
				ByteBuffer.Wrap(lengthBytes).AsIntBuffer().Put(length);
				@out.Write(lengthBytes);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveInodes(FsImageProto.FileSummary.Builder summary)
			{
				FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this, summary);
				saver.SerializeINodeSection(sectionOutputStream);
				saver.SerializeINodeDirectorySection(sectionOutputStream);
				saver.SerializeFilesUCSection(sectionOutputStream);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveSnapshots(FsImageProto.FileSummary.Builder summary)
			{
				FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(this
					, summary, context, context.GetSourceNamesystem());
				snapshotSaver.SerializeSnapshotSection(sectionOutputStream);
				snapshotSaver.SerializeSnapshotDiffSection(sectionOutputStream);
				snapshotSaver.SerializeINodeReferenceSection(sectionOutputStream);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveInternal(FileOutputStream fout, FSImageCompression compression, 
				string filePath)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				MessageDigest digester = MD5Hash.GetDigester();
				underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(fout), digester
					);
				underlyingOutputStream.Write(FSImageUtil.MagicHeader);
				fileChannel = fout.GetChannel();
				FsImageProto.FileSummary.Builder b = FsImageProto.FileSummary.NewBuilder().SetOndiskVersion
					(FSImageUtil.FileVersion).SetLayoutVersion(NameNodeLayoutVersion.CurrentLayoutVersion
					);
				codec = compression.GetImageCodec();
				if (codec != null)
				{
					b.SetCodec(codec.GetType().GetCanonicalName());
					sectionOutputStream = codec.CreateOutputStream(underlyingOutputStream);
				}
				else
				{
					sectionOutputStream = underlyingOutputStream;
				}
				SaveNameSystemSection(b);
				// Check for cancellation right after serializing the name system section.
				// Some unit tests, such as TestSaveNamespace#testCancelSaveNameSpace
				// depends on this behavior.
				context.CheckCancelled();
				Step step = new Step(StepType.Inodes, filePath);
				prog.BeginStep(Phase.SavingCheckpoint, step);
				SaveInodes(b);
				SaveSnapshots(b);
				prog.EndStep(Phase.SavingCheckpoint, step);
				step = new Step(StepType.DelegationTokens, filePath);
				prog.BeginStep(Phase.SavingCheckpoint, step);
				SaveSecretManagerSection(b);
				prog.EndStep(Phase.SavingCheckpoint, step);
				step = new Step(StepType.CachePools, filePath);
				prog.BeginStep(Phase.SavingCheckpoint, step);
				SaveCacheManagerSection(b);
				prog.EndStep(Phase.SavingCheckpoint, step);
				SaveStringTableSection(b);
				// We use the underlyingOutputStream to write the header. Therefore flush
				// the buffered stream (which is potentially compressed) first.
				FlushSectionOutputStream();
				FsImageProto.FileSummary summary = ((FsImageProto.FileSummary)b.Build());
				SaveFileSummary(underlyingOutputStream, summary);
				underlyingOutputStream.Close();
				savedDigest = new MD5Hash(digester.Digest());
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveSecretManagerSection(FsImageProto.FileSummary.Builder summary)
			{
				FSNamesystem fsn = context.GetSourceNamesystem();
				DelegationTokenSecretManager.SecretManagerState state = fsn.SaveSecretManagerState
					();
				state.section.WriteDelimitedTo(sectionOutputStream);
				foreach (FsImageProto.SecretManagerSection.DelegationKey k in state.keys)
				{
					k.WriteDelimitedTo(sectionOutputStream);
				}
				foreach (FsImageProto.SecretManagerSection.PersistToken t in state.tokens)
				{
					t.WriteDelimitedTo(sectionOutputStream);
				}
				CommitSection(summary, FSImageFormatProtobuf.SectionName.SecretManager);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveCacheManagerSection(FsImageProto.FileSummary.Builder summary)
			{
				FSNamesystem fsn = context.GetSourceNamesystem();
				CacheManager.PersistState state = fsn.GetCacheManager().SaveState();
				state.section.WriteDelimitedTo(sectionOutputStream);
				foreach (ClientNamenodeProtocolProtos.CachePoolInfoProto p in state.pools)
				{
					p.WriteDelimitedTo(sectionOutputStream);
				}
				foreach (ClientNamenodeProtocolProtos.CacheDirectiveInfoProto p_1 in state.directives)
				{
					p_1.WriteDelimitedTo(sectionOutputStream);
				}
				CommitSection(summary, FSImageFormatProtobuf.SectionName.CacheManager);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveNameSystemSection(FsImageProto.FileSummary.Builder summary)
			{
				FSNamesystem fsn = context.GetSourceNamesystem();
				OutputStream @out = sectionOutputStream;
				BlockIdManager blockIdManager = fsn.GetBlockIdManager();
				FsImageProto.NameSystemSection.Builder b = FsImageProto.NameSystemSection.NewBuilder
					().SetGenstampV1(blockIdManager.GetGenerationStampV1()).SetGenstampV1Limit(blockIdManager
					.GetGenerationStampV1Limit()).SetGenstampV2(blockIdManager.GetGenerationStampV2(
					)).SetLastAllocatedBlockId(blockIdManager.GetLastAllocatedBlockId()).SetTransactionId
					(context.GetTxId());
				// We use the non-locked version of getNamespaceInfo here since
				// the coordinating thread of saveNamespace already has read-locked
				// the namespace for us. If we attempt to take another readlock
				// from the actual saver thread, there's a potential of a
				// fairness-related deadlock. See the comments on HDFS-2223.
				b.SetNamespaceId(fsn.UnprotectedGetNamespaceInfo().GetNamespaceID());
				if (fsn.IsRollingUpgrade())
				{
					b.SetRollingUpgradeStartTime(fsn.GetRollingUpgradeInfo().GetStartTime());
				}
				FsImageProto.NameSystemSection s = ((FsImageProto.NameSystemSection)b.Build());
				s.WriteDelimitedTo(@out);
				CommitSection(summary, FSImageFormatProtobuf.SectionName.NsInfo);
			}

			/// <exception cref="System.IO.IOException"/>
			private void SaveStringTableSection(FsImageProto.FileSummary.Builder summary)
			{
				OutputStream @out = sectionOutputStream;
				FsImageProto.StringTableSection.Builder b = FsImageProto.StringTableSection.NewBuilder
					().SetNumEntry(saverContext.stringMap.Size());
				((FsImageProto.StringTableSection)b.Build()).WriteDelimitedTo(@out);
				foreach (KeyValuePair<string, int> e in saverContext.stringMap.EntrySet())
				{
					FsImageProto.StringTableSection.Entry.Builder eb = FsImageProto.StringTableSection.Entry
						.NewBuilder().SetId(e.Value).SetStr(e.Key);
					((FsImageProto.StringTableSection.Entry)eb.Build()).WriteDelimitedTo(@out);
				}
				CommitSection(summary, FSImageFormatProtobuf.SectionName.StringTable);
			}
		}

		/// <summary>Supported section name.</summary>
		/// <remarks>
		/// Supported section name. The order of the enum determines the order of
		/// loading.
		/// </remarks>
		[System.Serializable]
		public sealed class SectionName
		{
			public static readonly FSImageFormatProtobuf.SectionName NsInfo = new FSImageFormatProtobuf.SectionName
				("NS_INFO");

			public static readonly FSImageFormatProtobuf.SectionName StringTable = new FSImageFormatProtobuf.SectionName
				("STRING_TABLE");

			public static readonly FSImageFormatProtobuf.SectionName ExtendedAcl = new FSImageFormatProtobuf.SectionName
				("EXTENDED_ACL");

			public static readonly FSImageFormatProtobuf.SectionName Inode = new FSImageFormatProtobuf.SectionName
				("INODE");

			public static readonly FSImageFormatProtobuf.SectionName InodeReference = new FSImageFormatProtobuf.SectionName
				("INODE_REFERENCE");

			public static readonly FSImageFormatProtobuf.SectionName Snapshot = new FSImageFormatProtobuf.SectionName
				("SNAPSHOT");

			public static readonly FSImageFormatProtobuf.SectionName InodeDir = new FSImageFormatProtobuf.SectionName
				("INODE_DIR");

			public static readonly FSImageFormatProtobuf.SectionName FilesUnderconstruction = 
				new FSImageFormatProtobuf.SectionName("FILES_UNDERCONSTRUCTION");

			public static readonly FSImageFormatProtobuf.SectionName SnapshotDiff = new FSImageFormatProtobuf.SectionName
				("SNAPSHOT_DIFF");

			public static readonly FSImageFormatProtobuf.SectionName SecretManager = new FSImageFormatProtobuf.SectionName
				("SECRET_MANAGER");

			public static readonly FSImageFormatProtobuf.SectionName CacheManager = new FSImageFormatProtobuf.SectionName
				("CACHE_MANAGER");

			private static readonly FSImageFormatProtobuf.SectionName[] values = FSImageFormatProtobuf.SectionName
				.Values();

			public static FSImageFormatProtobuf.SectionName FromString(string name)
			{
				foreach (FSImageFormatProtobuf.SectionName n in FSImageFormatProtobuf.SectionName
					.values)
				{
					if (n.name.Equals(name))
					{
						return n;
					}
				}
				return null;
			}

			private readonly string name;

			private SectionName(string name)
			{
				this.name = name;
			}
		}

		private static int GetOndiskTrunkSize(GeneratedMessage s)
		{
			return CodedOutputStream.ComputeRawVarint32Size(s.GetSerializedSize()) + s.GetSerializedSize
				();
		}

		private FSImageFormatProtobuf()
		{
		}
	}
}
