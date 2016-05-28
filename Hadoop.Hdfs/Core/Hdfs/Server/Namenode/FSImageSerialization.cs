using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Static utility functions for serializing various pieces of data in the correct
	/// format for the FSImage file.
	/// </summary>
	/// <remarks>
	/// Static utility functions for serializing various pieces of data in the correct
	/// format for the FSImage file.
	/// Some members are currently public for the benefit of the Offline Image Viewer
	/// which is located outside of this package. These members should be made
	/// package-protected when the OIV is refactored.
	/// </remarks>
	public class FSImageSerialization
	{
		private FSImageSerialization()
		{
		}

		private sealed class _ThreadLocal_76 : ThreadLocal<FSImageSerialization.TLData>
		{
			public _ThreadLocal_76()
			{
			}

			// Static-only class
			protected override FSImageSerialization.TLData InitialValue()
			{
				return new FSImageSerialization.TLData();
			}
		}

		/// <summary>In order to reduce allocation, we reuse some static objects.</summary>
		/// <remarks>
		/// In order to reduce allocation, we reuse some static objects. However, the methods
		/// in this class should be thread-safe since image-saving is multithreaded, so
		/// we need to keep the static objects in a thread-local.
		/// </remarks>
		private static readonly ThreadLocal<FSImageSerialization.TLData> TlData = new _ThreadLocal_76
			();

		/// <summary>Simple container "struct" for threadlocal data.</summary>
		private sealed class TLData
		{
			internal readonly DeprecatedUTF8 UStr = new DeprecatedUTF8();

			internal readonly ShortWritable UShort = new ShortWritable();

			internal readonly IntWritable UInt = new IntWritable();

			internal readonly LongWritable ULong = new LongWritable();

			internal readonly FsPermission FilePerm = new FsPermission((short)0);

			internal readonly BooleanWritable UBoolean = new BooleanWritable();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WritePermissionStatus(INodeAttributes inode, DataOutput @out)
		{
			FsPermission p = TlData.Get().FilePerm;
			p.FromShort(inode.GetFsPermissionShort());
			PermissionStatus.Write(@out, inode.GetUserName(), inode.GetGroupName(), p);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteBlocks(Block[] blocks, DataOutput @out)
		{
			if (blocks == null)
			{
				@out.WriteInt(0);
			}
			else
			{
				@out.WriteInt(blocks.Length);
				foreach (Block blk in blocks)
				{
					blk.Write(@out);
				}
			}
		}

		// Helper function that reads in an INodeUnderConstruction
		// from the input stream
		//
		/// <exception cref="System.IO.IOException"/>
		internal static INodeFile ReadINodeUnderConstruction(DataInput @in, FSNamesystem 
			fsNamesys, int imgVersion)
		{
			byte[] name = ReadBytes(@in);
			long inodeId = NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, imgVersion
				) ? @in.ReadLong() : fsNamesys.dir.AllocateNewInodeId();
			short blockReplication = @in.ReadShort();
			long modificationTime = @in.ReadLong();
			long preferredBlockSize = @in.ReadLong();
			int numBlocks = @in.ReadInt();
			BlockInfoContiguous[] blocks = new BlockInfoContiguous[numBlocks];
			Block blk = new Block();
			int i = 0;
			for (; i < numBlocks - 1; i++)
			{
				blk.ReadFields(@in);
				blocks[i] = new BlockInfoContiguous(blk, blockReplication);
			}
			// last block is UNDER_CONSTRUCTION
			if (numBlocks > 0)
			{
				blk.ReadFields(@in);
				blocks[i] = new BlockInfoContiguousUnderConstruction(blk, blockReplication, HdfsServerConstants.BlockUCState
					.UnderConstruction, null);
			}
			PermissionStatus perm = PermissionStatus.Read(@in);
			string clientName = ReadString(@in);
			string clientMachine = ReadString(@in);
			// We previously stored locations for the last block, now we
			// just record that there are none
			int numLocs = @in.ReadInt();
			System.Diagnostics.Debug.Assert(numLocs == 0, "Unexpected block locations");
			// Images in the pre-protobuf format will not have the lazyPersist flag,
			// so it is safe to pass false always.
			INodeFile file = new INodeFile(inodeId, name, perm, modificationTime, modificationTime
				, blocks, blockReplication, preferredBlockSize, unchecked((byte)0));
			file.ToUnderConstruction(clientName, clientMachine);
			return file;
		}

		// Helper function that writes an INodeUnderConstruction
		// into the output stream
		//
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteINodeUnderConstruction(DataOutputStream @out, INodeFile
			 cons, string path)
		{
			WriteString(path, @out);
			@out.WriteLong(cons.GetId());
			@out.WriteShort(cons.GetFileReplication());
			@out.WriteLong(cons.GetModificationTime());
			@out.WriteLong(cons.GetPreferredBlockSize());
			WriteBlocks(cons.GetBlocks(), @out);
			cons.GetPermissionStatus().Write(@out);
			FileUnderConstructionFeature uc = cons.GetFileUnderConstructionFeature();
			WriteString(uc.GetClientName(), @out);
			WriteString(uc.GetClientMachine(), @out);
			@out.WriteInt(0);
		}

		//  do not store locations of last block
		/// <summary>
		/// Serialize a
		/// <see cref="INodeFile"/>
		/// node
		/// </summary>
		/// <param name="node">The node to write</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutputStream"/>
		/// where the fields are written
		/// </param>
		/// <param name="writeBlock">Whether to write block information</param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteINodeFile(INodeFile file, DataOutput @out, bool writeUnderConstruction
			)
		{
			WriteLocalName(file, @out);
			@out.WriteLong(file.GetId());
			@out.WriteShort(file.GetFileReplication());
			@out.WriteLong(file.GetModificationTime());
			@out.WriteLong(file.GetAccessTime());
			@out.WriteLong(file.GetPreferredBlockSize());
			WriteBlocks(file.GetBlocks(), @out);
			SnapshotFSImageFormat.SaveFileDiffList(file, @out);
			if (writeUnderConstruction)
			{
				if (file.IsUnderConstruction())
				{
					@out.WriteBoolean(true);
					FileUnderConstructionFeature uc = file.GetFileUnderConstructionFeature();
					WriteString(uc.GetClientName(), @out);
					WriteString(uc.GetClientMachine(), @out);
				}
				else
				{
					@out.WriteBoolean(false);
				}
			}
			WritePermissionStatus(file, @out);
		}

		/// <summary>
		/// Serialize an
		/// <see cref="INodeFileAttributes"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteINodeFileAttributes(INodeFileAttributes file, DataOutput 
			@out)
		{
			WriteLocalName(file, @out);
			WritePermissionStatus(file, @out);
			@out.WriteLong(file.GetModificationTime());
			@out.WriteLong(file.GetAccessTime());
			@out.WriteShort(file.GetFileReplication());
			@out.WriteLong(file.GetPreferredBlockSize());
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteQuota(QuotaCounts quota, DataOutput @out)
		{
			@out.WriteLong(quota.GetNameSpace());
			@out.WriteLong(quota.GetStorageSpace());
		}

		/// <summary>
		/// Serialize a
		/// <see cref="INodeDirectory"/>
		/// </summary>
		/// <param name="node">The node to write</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutput"/>
		/// where the fields are written
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteINodeDirectory(INodeDirectory node, DataOutput @out)
		{
			WriteLocalName(node, @out);
			@out.WriteLong(node.GetId());
			@out.WriteShort(0);
			// replication
			@out.WriteLong(node.GetModificationTime());
			@out.WriteLong(0);
			// access time
			@out.WriteLong(0);
			// preferred block size
			@out.WriteInt(-1);
			// # of blocks
			WriteQuota(node.GetQuotaCounts(), @out);
			if (node.IsSnapshottable())
			{
				@out.WriteBoolean(true);
			}
			else
			{
				@out.WriteBoolean(false);
				@out.WriteBoolean(node.IsWithSnapshot());
			}
			WritePermissionStatus(node, @out);
		}

		/// <summary>
		/// Serialize a
		/// <see cref="INodeDirectory"/>
		/// </summary>
		/// <param name="a">The node to write</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutput"/>
		/// where the fields are written
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteINodeDirectoryAttributes(INodeDirectoryAttributes a, DataOutput
			 @out)
		{
			WriteLocalName(a, @out);
			WritePermissionStatus(a, @out);
			@out.WriteLong(a.GetModificationTime());
			WriteQuota(a.GetQuotaCounts(), @out);
		}

		/// <summary>
		/// Serialize a
		/// <see cref="INodeSymlink"/>
		/// node
		/// </summary>
		/// <param name="node">The node to write</param>
		/// <param name="out">
		/// The
		/// <see cref="System.IO.DataOutput"/>
		/// where the fields are written
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteINodeSymlink(INodeSymlink node, DataOutput @out)
		{
			WriteLocalName(node, @out);
			@out.WriteLong(node.GetId());
			@out.WriteShort(0);
			// replication
			@out.WriteLong(0);
			// modification time
			@out.WriteLong(0);
			// access time
			@out.WriteLong(0);
			// preferred block size
			@out.WriteInt(-2);
			// # of blocks
			Text.WriteString(@out, node.GetSymlinkString());
			WritePermissionStatus(node, @out);
		}

		/// <summary>
		/// Serialize a
		/// <see cref="INodeReference"/>
		/// node
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void WriteINodeReference(INodeReference @ref, DataOutput @out, bool
			 writeUnderConstruction, SnapshotFSImageFormat.ReferenceMap referenceMap)
		{
			WriteLocalName(@ref, @out);
			@out.WriteLong(@ref.GetId());
			@out.WriteShort(0);
			// replication
			@out.WriteLong(0);
			// modification time
			@out.WriteLong(0);
			// access time
			@out.WriteLong(0);
			// preferred block size
			@out.WriteInt(-3);
			// # of blocks
			bool isWithName = @ref is INodeReference.WithName;
			@out.WriteBoolean(isWithName);
			if (!isWithName)
			{
				Preconditions.CheckState(@ref is INodeReference.DstReference);
				// dst snapshot id
				@out.WriteInt(((INodeReference.DstReference)@ref).GetDstSnapshotId());
			}
			else
			{
				@out.WriteInt(((INodeReference.WithName)@ref).GetLastSnapshotId());
			}
			INodeReference.WithCount withCount = (INodeReference.WithCount)@ref.GetReferredINode
				();
			referenceMap.WriteINodeReferenceWithCount(withCount, @out, writeUnderConstruction
				);
		}

		/// <summary>Save one inode's attributes to the image.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void SaveINode2Image(INode node, DataOutput @out, bool writeUnderConstruction
			, SnapshotFSImageFormat.ReferenceMap referenceMap)
		{
			if (node.IsReference())
			{
				WriteINodeReference(node.AsReference(), @out, writeUnderConstruction, referenceMap
					);
			}
			else
			{
				if (node.IsDirectory())
				{
					WriteINodeDirectory(node.AsDirectory(), @out);
				}
				else
				{
					if (node.IsSymlink())
					{
						WriteINodeSymlink(node.AsSymlink(), @out);
					}
					else
					{
						if (node.IsFile())
						{
							WriteINodeFile(node.AsFile(), @out, writeUnderConstruction);
						}
					}
				}
			}
		}

		// This should be reverted to package private once the ImageLoader
		// code is moved into this package. This method should not be called
		// by other code.
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(DataInput @in)
		{
			DeprecatedUTF8 ustr = TlData.Get().UStr;
			ustr.ReadFields(@in);
			return ustr.ToStringChecked();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string ReadString_EmptyAsNull(DataInput @in)
		{
			string s = ReadString(@in);
			return s.IsEmpty() ? null : s;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteString(string str, DataOutput @out)
		{
			DeprecatedUTF8 ustr = TlData.Get().UStr;
			ustr.Set(str);
			ustr.Write(@out);
		}

		/// <summary>read the long value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static long ReadLong(DataInput @in)
		{
			LongWritable uLong = TlData.Get().ULong;
			uLong.ReadFields(@in);
			return uLong.Get();
		}

		/// <summary>write the long value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteLong(long value, DataOutputStream @out)
		{
			LongWritable uLong = TlData.Get().ULong;
			uLong.Set(value);
			uLong.Write(@out);
		}

		/// <summary>read the boolean value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static bool ReadBoolean(DataInput @in)
		{
			BooleanWritable uBoolean = TlData.Get().UBoolean;
			uBoolean.ReadFields(@in);
			return uBoolean.Get();
		}

		/// <summary>write the boolean value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteBoolean(bool value, DataOutputStream @out)
		{
			BooleanWritable uBoolean = TlData.Get().UBoolean;
			uBoolean.Set(value);
			uBoolean.Write(@out);
		}

		/// <summary>write the byte value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteByte(byte value, DataOutputStream @out)
		{
			@out.Write(value);
		}

		/// <summary>read the int value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static int ReadInt(DataInput @in)
		{
			IntWritable uInt = TlData.Get().UInt;
			uInt.ReadFields(@in);
			return uInt.Get();
		}

		/// <summary>write the int value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteInt(int value, DataOutputStream @out)
		{
			IntWritable uInt = TlData.Get().UInt;
			uInt.Set(value);
			uInt.Write(@out);
		}

		/// <summary>read short value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static short ReadShort(DataInput @in)
		{
			ShortWritable uShort = TlData.Get().UShort;
			uShort.ReadFields(@in);
			return uShort.Get();
		}

		/// <summary>write short value</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteShort(short value, DataOutputStream @out)
		{
			ShortWritable uShort = TlData.Get().UShort;
			uShort.Set(value);
			uShort.Write(@out);
		}

		// Same comments apply for this method as for readString()
		/// <exception cref="System.IO.IOException"/>
		public static byte[] ReadBytes(DataInput @in)
		{
			DeprecatedUTF8 ustr = TlData.Get().UStr;
			ustr.ReadFields(@in);
			int len = ustr.GetLength();
			byte[] bytes = new byte[len];
			System.Array.Copy(ustr.GetBytes(), 0, bytes, 0, len);
			return bytes;
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte ReadByte(DataInput @in)
		{
			return @in.ReadByte();
		}

		/// <summary>
		/// Reading the path from the image and converting it to byte[][] directly
		/// this saves us an array copy and conversions to and from String
		/// </summary>
		/// <param name="in">input to read from</param>
		/// <returns>
		/// the array each element of which is a byte[] representation
		/// of a path component
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static byte[][] ReadPathComponents(DataInput @in)
		{
			DeprecatedUTF8 ustr = TlData.Get().UStr;
			ustr.ReadFields(@in);
			return DFSUtil.Bytes2byteArray(ustr.GetBytes(), ustr.GetLength(), unchecked((byte
				)Path.SeparatorChar));
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte[] ReadLocalName(DataInput @in)
		{
			byte[] createdNodeName = new byte[@in.ReadShort()];
			@in.ReadFully(createdNodeName);
			return createdNodeName;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteLocalName(INodeAttributes inode, DataOutput @out)
		{
			byte[] name = inode.GetLocalNameBytes();
			WriteBytes(name, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteBytes(byte[] data, DataOutput @out)
		{
			@out.WriteShort(data.Length);
			@out.Write(data);
		}

		/// <summary>Write an array of blocks as compactly as possible.</summary>
		/// <remarks>
		/// Write an array of blocks as compactly as possible. This uses
		/// delta-encoding for the generation stamp and size, following
		/// the principle that genstamp increases relatively slowly,
		/// and size is equal for all but the last block of a file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteCompactBlockArray(Block[] blocks, DataOutputStream @out)
		{
			WritableUtils.WriteVInt(@out, blocks.Length);
			Block prev = null;
			foreach (Block b in blocks)
			{
				long szDelta = b.GetNumBytes() - (prev != null ? prev.GetNumBytes() : 0);
				long gsDelta = b.GetGenerationStamp() - (prev != null ? prev.GetGenerationStamp()
					 : 0);
				@out.WriteLong(b.GetBlockId());
				// blockid is random
				WritableUtils.WriteVLong(@out, szDelta);
				WritableUtils.WriteVLong(@out, gsDelta);
				prev = b;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static Block[] ReadCompactBlockArray(DataInput @in, int logVersion)
		{
			int num = WritableUtils.ReadVInt(@in);
			if (num < 0)
			{
				throw new IOException("Invalid block array length: " + num);
			}
			Block prev = null;
			Block[] ret = new Block[num];
			for (int i = 0; i < num; i++)
			{
				long id = @in.ReadLong();
				long sz = WritableUtils.ReadVLong(@in) + ((prev != null) ? prev.GetNumBytes() : 0
					);
				long gs = WritableUtils.ReadVLong(@in) + ((prev != null) ? prev.GetGenerationStamp
					() : 0);
				ret[i] = new Block(id, sz, gs);
				prev = ret[i];
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteCacheDirectiveInfo(DataOutputStream @out, CacheDirectiveInfo
			 directive)
		{
			WriteLong(directive.GetId(), @out);
			int flags = ((directive.GetPath() != null) ? unchecked((int)(0x1)) : 0) | ((directive
				.GetReplication() != null) ? unchecked((int)(0x2)) : 0) | ((directive.GetPool() 
				!= null) ? unchecked((int)(0x4)) : 0) | ((directive.GetExpiration() != null) ? unchecked(
				(int)(0x8)) : 0);
			@out.WriteInt(flags);
			if (directive.GetPath() != null)
			{
				WriteString(directive.GetPath().ToUri().GetPath(), @out);
			}
			if (directive.GetReplication() != null)
			{
				WriteShort(directive.GetReplication(), @out);
			}
			if (directive.GetPool() != null)
			{
				WriteString(directive.GetPool(), @out);
			}
			if (directive.GetExpiration() != null)
			{
				WriteLong(directive.GetExpiration().GetMillis(), @out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static CacheDirectiveInfo ReadCacheDirectiveInfo(DataInput @in)
		{
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
			builder.SetId(ReadLong(@in));
			int flags = @in.ReadInt();
			if ((flags & unchecked((int)(0x1))) != 0)
			{
				builder.SetPath(new Path(ReadString(@in)));
			}
			if ((flags & unchecked((int)(0x2))) != 0)
			{
				builder.SetReplication(ReadShort(@in));
			}
			if ((flags & unchecked((int)(0x4))) != 0)
			{
				builder.SetPool(ReadString(@in));
			}
			if ((flags & unchecked((int)(0x8))) != 0)
			{
				builder.SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute(ReadLong(@in)));
			}
			if ((flags & ~unchecked((int)(0xF))) != 0)
			{
				throw new IOException("unknown flags set in " + "ModifyCacheDirectiveInfoOp: " + 
					flags);
			}
			return builder.Build();
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static CacheDirectiveInfo ReadCacheDirectiveInfo(XMLUtils.Stanza st)
		{
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
			builder.SetId(long.Parse(st.GetValue("ID")));
			string path = st.GetValueOrNull("PATH");
			if (path != null)
			{
				builder.SetPath(new Path(path));
			}
			string replicationString = st.GetValueOrNull("REPLICATION");
			if (replicationString != null)
			{
				builder.SetReplication(short.ParseShort(replicationString));
			}
			string pool = st.GetValueOrNull("POOL");
			if (pool != null)
			{
				builder.SetPool(pool);
			}
			string expiryTime = st.GetValueOrNull("EXPIRATION");
			if (expiryTime != null)
			{
				builder.SetExpiration(CacheDirectiveInfo.Expiration.NewAbsolute(long.Parse(expiryTime
					)));
			}
			return builder.Build();
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void WriteCacheDirectiveInfo(ContentHandler contentHandler, CacheDirectiveInfo
			 directive)
		{
			XMLUtils.AddSaxString(contentHandler, "ID", System.Convert.ToString(directive.GetId
				()));
			if (directive.GetPath() != null)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", directive.GetPath().ToUri().GetPath
					());
			}
			if (directive.GetReplication() != null)
			{
				XMLUtils.AddSaxString(contentHandler, "REPLICATION", short.ToString(directive.GetReplication
					()));
			}
			if (directive.GetPool() != null)
			{
				XMLUtils.AddSaxString(contentHandler, "POOL", directive.GetPool());
			}
			if (directive.GetExpiration() != null)
			{
				XMLUtils.AddSaxString(contentHandler, "EXPIRATION", string.Empty + directive.GetExpiration
					().GetMillis());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteCachePoolInfo(DataOutputStream @out, CachePoolInfo info)
		{
			WriteString(info.GetPoolName(), @out);
			string ownerName = info.GetOwnerName();
			string groupName = info.GetGroupName();
			long limit = info.GetLimit();
			FsPermission mode = info.GetMode();
			long maxRelativeExpiry = info.GetMaxRelativeExpiryMs();
			bool hasOwner;
			bool hasGroup;
			bool hasMode;
			bool hasLimit;
			bool hasMaxRelativeExpiry;
			hasOwner = ownerName != null;
			hasGroup = groupName != null;
			hasMode = mode != null;
			hasLimit = limit != null;
			hasMaxRelativeExpiry = maxRelativeExpiry != null;
			int flags = (hasOwner ? unchecked((int)(0x1)) : 0) | (hasGroup ? unchecked((int)(
				0x2)) : 0) | (hasMode ? unchecked((int)(0x4)) : 0) | (hasLimit ? unchecked((int)
				(0x8)) : 0) | (hasMaxRelativeExpiry ? unchecked((int)(0x10)) : 0);
			WriteInt(flags, @out);
			if (hasOwner)
			{
				WriteString(ownerName, @out);
			}
			if (hasGroup)
			{
				WriteString(groupName, @out);
			}
			if (hasMode)
			{
				mode.Write(@out);
			}
			if (hasLimit)
			{
				WriteLong(limit, @out);
			}
			if (hasMaxRelativeExpiry)
			{
				WriteLong(maxRelativeExpiry, @out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static CachePoolInfo ReadCachePoolInfo(DataInput @in)
		{
			string poolName = ReadString(@in);
			CachePoolInfo info = new CachePoolInfo(poolName);
			int flags = ReadInt(@in);
			if ((flags & unchecked((int)(0x1))) != 0)
			{
				info.SetOwnerName(ReadString(@in));
			}
			if ((flags & unchecked((int)(0x2))) != 0)
			{
				info.SetGroupName(ReadString(@in));
			}
			if ((flags & unchecked((int)(0x4))) != 0)
			{
				info.SetMode(FsPermission.Read(@in));
			}
			if ((flags & unchecked((int)(0x8))) != 0)
			{
				info.SetLimit(ReadLong(@in));
			}
			if ((flags & unchecked((int)(0x10))) != 0)
			{
				info.SetMaxRelativeExpiryMs(ReadLong(@in));
			}
			if ((flags & ~unchecked((int)(0x1F))) != 0)
			{
				throw new IOException("Unknown flag in CachePoolInfo: " + flags);
			}
			return info;
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void WriteCachePoolInfo(ContentHandler contentHandler, CachePoolInfo
			 info)
		{
			XMLUtils.AddSaxString(contentHandler, "POOLNAME", info.GetPoolName());
			string ownerName = info.GetOwnerName();
			string groupName = info.GetGroupName();
			long limit = info.GetLimit();
			FsPermission mode = info.GetMode();
			long maxRelativeExpiry = info.GetMaxRelativeExpiryMs();
			if (ownerName != null)
			{
				XMLUtils.AddSaxString(contentHandler, "OWNERNAME", ownerName);
			}
			if (groupName != null)
			{
				XMLUtils.AddSaxString(contentHandler, "GROUPNAME", groupName);
			}
			if (mode != null)
			{
				FSEditLogOp.FsPermissionToXml(contentHandler, mode);
			}
			if (limit != null)
			{
				XMLUtils.AddSaxString(contentHandler, "LIMIT", System.Convert.ToString(limit));
			}
			if (maxRelativeExpiry != null)
			{
				XMLUtils.AddSaxString(contentHandler, "MAXRELATIVEEXPIRY", System.Convert.ToString
					(maxRelativeExpiry));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static CachePoolInfo ReadCachePoolInfo(XMLUtils.Stanza st)
		{
			string poolName = st.GetValue("POOLNAME");
			CachePoolInfo info = new CachePoolInfo(poolName);
			if (st.HasChildren("OWNERNAME"))
			{
				info.SetOwnerName(st.GetValue("OWNERNAME"));
			}
			if (st.HasChildren("GROUPNAME"))
			{
				info.SetGroupName(st.GetValue("GROUPNAME"));
			}
			if (st.HasChildren("MODE"))
			{
				info.SetMode(FSEditLogOp.FsPermissionFromXml(st));
			}
			if (st.HasChildren("LIMIT"))
			{
				info.SetLimit(long.Parse(st.GetValue("LIMIT")));
			}
			if (st.HasChildren("MAXRELATIVEEXPIRY"))
			{
				info.SetMaxRelativeExpiryMs(long.Parse(st.GetValue("MAXRELATIVEEXPIRY")));
			}
			return info;
		}
	}
}
