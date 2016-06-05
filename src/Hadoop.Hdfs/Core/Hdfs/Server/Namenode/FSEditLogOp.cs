using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Codec;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Helper classes for reading the ops from an InputStream.</summary>
	/// <remarks>
	/// Helper classes for reading the ops from an InputStream.
	/// All ops derive from FSEditLogOp and are only
	/// instantiated from Reader#readOp()
	/// </remarks>
	public abstract class FSEditLogOp
	{
		public readonly FSEditLogOpCodes opCode;

		internal long txid;

		internal byte[] rpcClientId;

		internal int rpcCallId;

		internal void Reset()
		{
			txid = HdfsConstants.InvalidTxid;
			rpcClientId = RpcConstants.DummyClientId;
			rpcCallId = RpcConstants.InvalidCallId;
			ResetSubFields();
		}

		internal abstract void ResetSubFields();

		public sealed class OpInstanceCache
		{
			private readonly EnumMap<FSEditLogOpCodes, FSEditLogOp> inst = new EnumMap<FSEditLogOpCodes
				, FSEditLogOp>(typeof(FSEditLogOpCodes));

			public OpInstanceCache()
			{
				inst[FSEditLogOpCodes.OpAdd] = new FSEditLogOp.AddOp();
				inst[FSEditLogOpCodes.OpClose] = new FSEditLogOp.CloseOp();
				inst[FSEditLogOpCodes.OpSetReplication] = new FSEditLogOp.SetReplicationOp();
				inst[FSEditLogOpCodes.OpConcatDelete] = new FSEditLogOp.ConcatDeleteOp();
				inst[FSEditLogOpCodes.OpRenameOld] = new FSEditLogOp.RenameOldOp();
				inst[FSEditLogOpCodes.OpDelete] = new FSEditLogOp.DeleteOp();
				inst[FSEditLogOpCodes.OpMkdir] = new FSEditLogOp.MkdirOp();
				inst[FSEditLogOpCodes.OpSetGenstampV1] = new FSEditLogOp.SetGenstampV1Op();
				inst[FSEditLogOpCodes.OpSetPermissions] = new FSEditLogOp.SetPermissionsOp();
				inst[FSEditLogOpCodes.OpSetOwner] = new FSEditLogOp.SetOwnerOp();
				inst[FSEditLogOpCodes.OpSetNsQuota] = new FSEditLogOp.SetNSQuotaOp();
				inst[FSEditLogOpCodes.OpClearNsQuota] = new FSEditLogOp.ClearNSQuotaOp();
				inst[FSEditLogOpCodes.OpSetQuota] = new FSEditLogOp.SetQuotaOp();
				inst[FSEditLogOpCodes.OpTimes] = new FSEditLogOp.TimesOp();
				inst[FSEditLogOpCodes.OpSymlink] = new FSEditLogOp.SymlinkOp();
				inst[FSEditLogOpCodes.OpRename] = new FSEditLogOp.RenameOp();
				inst[FSEditLogOpCodes.OpReassignLease] = new FSEditLogOp.ReassignLeaseOp();
				inst[FSEditLogOpCodes.OpGetDelegationToken] = new FSEditLogOp.GetDelegationTokenOp
					();
				inst[FSEditLogOpCodes.OpRenewDelegationToken] = new FSEditLogOp.RenewDelegationTokenOp
					();
				inst[FSEditLogOpCodes.OpCancelDelegationToken] = new FSEditLogOp.CancelDelegationTokenOp
					();
				inst[FSEditLogOpCodes.OpUpdateMasterKey] = new FSEditLogOp.UpdateMasterKeyOp();
				inst[FSEditLogOpCodes.OpStartLogSegment] = new FSEditLogOp.LogSegmentOp(FSEditLogOpCodes
					.OpStartLogSegment);
				inst[FSEditLogOpCodes.OpEndLogSegment] = new FSEditLogOp.LogSegmentOp(FSEditLogOpCodes
					.OpEndLogSegment);
				inst[FSEditLogOpCodes.OpUpdateBlocks] = new FSEditLogOp.UpdateBlocksOp();
				inst[FSEditLogOpCodes.OpTruncate] = new FSEditLogOp.TruncateOp();
				inst[FSEditLogOpCodes.OpAllowSnapshot] = new FSEditLogOp.AllowSnapshotOp();
				inst[FSEditLogOpCodes.OpDisallowSnapshot] = new FSEditLogOp.DisallowSnapshotOp();
				inst[FSEditLogOpCodes.OpCreateSnapshot] = new FSEditLogOp.CreateSnapshotOp();
				inst[FSEditLogOpCodes.OpDeleteSnapshot] = new FSEditLogOp.DeleteSnapshotOp();
				inst[FSEditLogOpCodes.OpRenameSnapshot] = new FSEditLogOp.RenameSnapshotOp();
				inst[FSEditLogOpCodes.OpSetGenstampV2] = new FSEditLogOp.SetGenstampV2Op();
				inst[FSEditLogOpCodes.OpAllocateBlockId] = new FSEditLogOp.AllocateBlockIdOp();
				inst[FSEditLogOpCodes.OpAddBlock] = new FSEditLogOp.AddBlockOp();
				inst[FSEditLogOpCodes.OpAddCacheDirective] = new FSEditLogOp.AddCacheDirectiveInfoOp
					();
				inst[FSEditLogOpCodes.OpModifyCacheDirective] = new FSEditLogOp.ModifyCacheDirectiveInfoOp
					();
				inst[FSEditLogOpCodes.OpRemoveCacheDirective] = new FSEditLogOp.RemoveCacheDirectiveInfoOp
					();
				inst[FSEditLogOpCodes.OpAddCachePool] = new FSEditLogOp.AddCachePoolOp();
				inst[FSEditLogOpCodes.OpModifyCachePool] = new FSEditLogOp.ModifyCachePoolOp();
				inst[FSEditLogOpCodes.OpRemoveCachePool] = new FSEditLogOp.RemoveCachePoolOp();
				inst[FSEditLogOpCodes.OpSetAcl] = new FSEditLogOp.SetAclOp();
				inst[FSEditLogOpCodes.OpRollingUpgradeStart] = new FSEditLogOp.RollingUpgradeOp(FSEditLogOpCodes
					.OpRollingUpgradeStart, "start");
				inst[FSEditLogOpCodes.OpRollingUpgradeFinalize] = new FSEditLogOp.RollingUpgradeOp
					(FSEditLogOpCodes.OpRollingUpgradeFinalize, "finalize");
				inst[FSEditLogOpCodes.OpSetXattr] = new FSEditLogOp.SetXAttrOp();
				inst[FSEditLogOpCodes.OpRemoveXattr] = new FSEditLogOp.RemoveXAttrOp();
				inst[FSEditLogOpCodes.OpSetStoragePolicy] = new FSEditLogOp.SetStoragePolicyOp();
				inst[FSEditLogOpCodes.OpAppend] = new FSEditLogOp.AppendOp();
				inst[FSEditLogOpCodes.OpSetQuotaByStoragetype] = new FSEditLogOp.SetQuotaByStorageTypeOp
					();
			}

			public FSEditLogOp Get(FSEditLogOpCodes opcode)
			{
				return inst[opcode];
			}
		}

		private static ImmutableMap<string, FsAction> FsActionMap()
		{
			ImmutableMap.Builder<string, FsAction> b = ImmutableMap.Builder();
			foreach (FsAction v in FsAction.Values())
			{
				b.Put(v.Symbol, v);
			}
			return b.Build();
		}

		private static readonly ImmutableMap<string, FsAction> FsactionSymbolMap = FsActionMap
			();

		/// <summary>Constructor for an EditLog Op.</summary>
		/// <remarks>
		/// Constructor for an EditLog Op. EditLog ops cannot be constructed
		/// directly, but only through Reader#readOp.
		/// </remarks>
		[VisibleForTesting]
		protected internal FSEditLogOp(FSEditLogOpCodes opCode)
		{
			this.opCode = opCode;
			Reset();
		}

		public virtual long GetTransactionId()
		{
			Preconditions.CheckState(txid != HdfsConstants.InvalidTxid);
			return txid;
		}

		public virtual string GetTransactionIdStr()
		{
			return (txid == HdfsConstants.InvalidTxid) ? "(none)" : string.Empty + txid;
		}

		public virtual bool HasTransactionId()
		{
			return (txid != HdfsConstants.InvalidTxid);
		}

		public virtual void SetTransactionId(long txid)
		{
			this.txid = txid;
		}

		public virtual bool HasRpcIds()
		{
			return rpcClientId != RpcConstants.DummyClientId && rpcCallId != RpcConstants.InvalidCallId;
		}

		/// <summary>
		/// this has to be called after calling
		/// <see cref="HasRpcIds()"/>
		/// 
		/// </summary>
		public virtual byte[] GetClientId()
		{
			Preconditions.CheckState(rpcClientId != RpcConstants.DummyClientId);
			return rpcClientId;
		}

		public virtual void SetRpcClientId(byte[] clientId)
		{
			this.rpcClientId = clientId;
		}

		/// <summary>
		/// this has to be called after calling
		/// <see cref="HasRpcIds()"/>
		/// 
		/// </summary>
		public virtual int GetCallId()
		{
			Preconditions.CheckState(rpcCallId != RpcConstants.InvalidCallId);
			return rpcCallId;
		}

		public virtual void SetRpcCallId(int callId)
		{
			this.rpcCallId = callId;
		}

		/// <exception cref="System.IO.IOException"/>
		internal abstract void ReadFields(DataInputStream @in, int logVersion);

		/// <exception cref="System.IO.IOException"/>
		public abstract void WriteFields(DataOutputStream @out);

		internal interface BlockListUpdatingOp
		{
			Block[] GetBlocks();

			string GetPath();

			bool ShouldCompleteLastBlock();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteRpcIds(byte[] clientId, int callId, DataOutputStream @out
			)
		{
			FSImageSerialization.WriteBytes(clientId, @out);
			FSImageSerialization.WriteInt(callId, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadRpcIds(DataInputStream @in, int logVersion)
		{
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogSupportRetrycache
				, logVersion))
			{
				this.rpcClientId = FSImageSerialization.ReadBytes(@in);
				this.rpcCallId = FSImageSerialization.ReadInt(@in);
			}
		}

		internal virtual void ReadRpcIdsFromXml(XMLUtils.Stanza st)
		{
			this.rpcClientId = st.HasChildren("RPC_CLIENTID") ? ClientId.ToBytes(st.GetValue(
				"RPC_CLIENTID")) : RpcConstants.DummyClientId;
			this.rpcCallId = st.HasChildren("RPC_CALLID") ? System.Convert.ToInt32(st.GetValue
				("RPC_CALLID")) : RpcConstants.InvalidCallId;
		}

		private static void AppendRpcIdsToString(StringBuilder builder, byte[] clientId, 
			int callId)
		{
			builder.Append(", RpcClientId=");
			builder.Append(ClientId.ToString(clientId));
			builder.Append(", RpcCallId=");
			builder.Append(callId);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private static void AppendRpcIdsToXml(ContentHandler contentHandler, byte[] clientId
			, int callId)
		{
			XMLUtils.AddSaxString(contentHandler, "RPC_CLIENTID", ClientId.ToString(clientId)
				);
			XMLUtils.AddSaxString(contentHandler, "RPC_CALLID", Sharpen.Extensions.ToString(callId
				));
		}

		private sealed class AclEditLogUtil
		{
			private const int AclEditlogEntryHasNameOffset = 6;

			private const int AclEditlogEntryTypeOffset = 3;

			private const int AclEditlogEntryScopeOffset = 5;

			private const int AclEditlogPermMask = 7;

			private const int AclEditlogEntryTypeMask = 3;

			private const int AclEditlogEntryScopeMask = 1;

			private static readonly FsAction[] FsactionValues = FsAction.Values();

			private static readonly AclEntryScope[] AclEntryScopeValues = AclEntryScope.Values
				();

			private static readonly AclEntryType[] AclEntryTypeValues = AclEntryType.Values();

			/// <exception cref="System.IO.IOException"/>
			private static IList<AclEntry> Read(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.ExtendedAcl, logVersion
					))
				{
					return null;
				}
				int size = @in.ReadInt();
				if (size == 0)
				{
					return null;
				}
				IList<AclEntry> aclEntries = Lists.NewArrayListWithCapacity(size);
				for (int i = 0; i < size; ++i)
				{
					int v = @in.Read();
					int p = v & AclEditlogPermMask;
					int t = (v >> AclEditlogEntryTypeOffset) & AclEditlogEntryTypeMask;
					int s = (v >> AclEditlogEntryScopeOffset) & AclEditlogEntryScopeMask;
					bool hasName = ((v >> AclEditlogEntryHasNameOffset) & 1) == 1;
					string name = hasName ? FSImageSerialization.ReadString(@in) : null;
					aclEntries.AddItem(new AclEntry.Builder().SetName(name).SetPermission(FsactionValues
						[p]).SetScope(AclEntryScopeValues[s]).SetType(AclEntryTypeValues[t]).Build());
				}
				return aclEntries;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void Write(IList<AclEntry> aclEntries, DataOutputStream @out)
			{
				if (aclEntries == null)
				{
					@out.WriteInt(0);
					return;
				}
				@out.WriteInt(aclEntries.Count);
				foreach (AclEntry e in aclEntries)
				{
					bool hasName = e.GetName() != null;
					int v = ((int)(e.GetScope()) << AclEditlogEntryScopeOffset) | ((int)(e.GetType())
						 << AclEditlogEntryTypeOffset) | (int)(e.GetPermission());
					if (hasName)
					{
						v |= 1 << AclEditlogEntryHasNameOffset;
					}
					@out.Write(v);
					if (hasName)
					{
						FSImageSerialization.WriteString(e.GetName(), @out);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<XAttr> ReadXAttrsFromEditLog(DataInputStream @in, int logVersion
			)
		{
			if (!NameNodeLayoutVersion.Supports(NameNodeLayoutVersion.Feature.Xattrs, logVersion
				))
			{
				return null;
			}
			XAttrProtos.XAttrEditLogProto proto = XAttrProtos.XAttrEditLogProto.ParseDelimitedFrom
				(@in);
			return PBHelper.ConvertXAttrs(proto.GetXAttrsList());
		}

		internal abstract class AddCloseOp : FSEditLogOp, FSEditLogOp.BlockListUpdatingOp
		{
			internal int length;

			internal long inodeId;

			internal string path;

			internal short replication;

			internal long mtime;

			internal long atime;

			internal long blockSize;

			internal Block[] blocks;

			internal PermissionStatus permissions;

			internal IList<AclEntry> aclEntries;

			internal IList<XAttr> xAttrs;

			internal string clientName;

			internal string clientMachine;

			internal bool overwrite;

			internal byte storagePolicyId;

			private AddCloseOp(FSEditLogOpCodes opCode)
				: base(opCode)
			{
				storagePolicyId = BlockStoragePolicySuite.IdUnspecified;
				System.Diagnostics.Debug.Assert((opCode == FSEditLogOpCodes.OpAdd || opCode == FSEditLogOpCodes
					.OpClose || opCode == FSEditLogOpCodes.OpAppend));
			}

			internal override void ResetSubFields()
			{
				length = 0;
				inodeId = 0L;
				path = null;
				replication = 0;
				mtime = 0L;
				atime = 0L;
				blockSize = 0L;
				blocks = null;
				permissions = null;
				aclEntries = null;
				xAttrs = null;
				clientName = null;
				clientMachine = null;
				overwrite = false;
				storagePolicyId = 0;
			}

			internal virtual T SetInodeId<T>(long inodeId)
				where T : FSEditLogOp.AddCloseOp
			{
				this.inodeId = inodeId;
				return (T)this;
			}

			internal virtual T SetPath<T>(string path)
				where T : FSEditLogOp.AddCloseOp
			{
				this.path = path;
				return (T)this;
			}

			public virtual string GetPath()
			{
				return path;
			}

			internal virtual T SetReplication<T>(short replication)
				where T : FSEditLogOp.AddCloseOp
			{
				this.replication = replication;
				return (T)this;
			}

			internal virtual T SetModificationTime<T>(long mtime)
				where T : FSEditLogOp.AddCloseOp
			{
				this.mtime = mtime;
				return (T)this;
			}

			internal virtual T SetAccessTime<T>(long atime)
				where T : FSEditLogOp.AddCloseOp
			{
				this.atime = atime;
				return (T)this;
			}

			internal virtual T SetBlockSize<T>(long blockSize)
				where T : FSEditLogOp.AddCloseOp
			{
				this.blockSize = blockSize;
				return (T)this;
			}

			internal virtual T SetBlocks<T>(Block[] blocks)
				where T : FSEditLogOp.AddCloseOp
			{
				if (blocks.Length > MaxBlocks)
				{
					throw new RuntimeException("Can't have more than " + MaxBlocks + " in an AddCloseOp."
						);
				}
				this.blocks = blocks;
				return (T)this;
			}

			public virtual Block[] GetBlocks()
			{
				return blocks;
			}

			internal virtual T SetPermissionStatus<T>(PermissionStatus permissions)
				where T : FSEditLogOp.AddCloseOp
			{
				this.permissions = permissions;
				return (T)this;
			}

			internal virtual T SetAclEntries<T>(IList<AclEntry> aclEntries)
				where T : FSEditLogOp.AddCloseOp
			{
				this.aclEntries = aclEntries;
				return (T)this;
			}

			internal virtual T SetXAttrs<T>(IList<XAttr> xAttrs)
				where T : FSEditLogOp.AddCloseOp
			{
				this.xAttrs = xAttrs;
				return (T)this;
			}

			internal virtual T SetClientName<T>(string clientName)
				where T : FSEditLogOp.AddCloseOp
			{
				this.clientName = clientName;
				return (T)this;
			}

			internal virtual T SetClientMachine<T>(string clientMachine)
				where T : FSEditLogOp.AddCloseOp
			{
				this.clientMachine = clientMachine;
				return (T)this;
			}

			internal virtual T SetOverwrite<T>(bool overwrite)
				where T : FSEditLogOp.AddCloseOp
			{
				this.overwrite = overwrite;
				return (T)this;
			}

			internal virtual T SetStoragePolicyId<T>(byte storagePolicyId)
				where T : FSEditLogOp.AddCloseOp
			{
				this.storagePolicyId = storagePolicyId;
				return (T)this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(inodeId, @out);
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteShort(replication, @out);
				FSImageSerialization.WriteLong(mtime, @out);
				FSImageSerialization.WriteLong(atime, @out);
				FSImageSerialization.WriteLong(blockSize, @out);
				new ArrayWritable(typeof(Block), blocks).Write(@out);
				permissions.Write(@out);
				if (this.opCode == FSEditLogOpCodes.OpAdd)
				{
					FSEditLogOp.AclEditLogUtil.Write(aclEntries, @out);
					XAttrProtos.XAttrEditLogProto.Builder b = XAttrProtos.XAttrEditLogProto.NewBuilder
						();
					b.AddAllXAttrs(PBHelper.ConvertXAttrProto(xAttrs));
					((XAttrProtos.XAttrEditLogProto)b.Build()).WriteDelimitedTo(@out);
					FSImageSerialization.WriteString(clientName, @out);
					FSImageSerialization.WriteString(clientMachine, @out);
					FSImageSerialization.WriteBoolean(overwrite, @out);
					FSImageSerialization.WriteByte(storagePolicyId, @out);
					// write clientId and callId
					WriteRpcIds(rpcClientId, rpcCallId, @out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, logVersion))
				{
					this.inodeId = @in.ReadLong();
				}
				else
				{
					// The inodeId should be updated when this editLogOp is applied
					this.inodeId = INodeId.GrandfatherInodeId;
				}
				if ((-17 < logVersion && length != 4) || (logVersion <= -17 && length != 5 && !NameNodeLayoutVersion
					.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion)))
				{
					throw new IOException("Incorrect data format." + " logVersion is " + logVersion +
						 " but writables.length is " + length + ". ");
				}
				this.path = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.replication = FSImageSerialization.ReadShort(@in);
					this.mtime = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.replication = ReadShort(@in);
					this.mtime = ReadLong(@in);
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FileAccessTime, logVersion
					))
				{
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
						))
					{
						this.atime = FSImageSerialization.ReadLong(@in);
					}
					else
					{
						this.atime = ReadLong(@in);
					}
				}
				else
				{
					this.atime = 0;
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.blockSize = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.blockSize = ReadLong(@in);
				}
				this.blocks = ReadBlocks(@in, logVersion);
				this.permissions = PermissionStatus.Read(@in);
				if (this.opCode == FSEditLogOpCodes.OpAdd)
				{
					aclEntries = FSEditLogOp.AclEditLogUtil.Read(@in, logVersion);
					this.xAttrs = ReadXAttrsFromEditLog(@in, logVersion);
					this.clientName = FSImageSerialization.ReadString(@in);
					this.clientMachine = FSImageSerialization.ReadString(@in);
					if (NameNodeLayoutVersion.Supports(NameNodeLayoutVersion.Feature.CreateOverwrite, 
						logVersion))
					{
						this.overwrite = FSImageSerialization.ReadBoolean(@in);
					}
					else
					{
						this.overwrite = false;
					}
					if (NameNodeLayoutVersion.Supports(NameNodeLayoutVersion.Feature.BlockStoragePolicy
						, logVersion))
					{
						this.storagePolicyId = FSImageSerialization.ReadByte(@in);
					}
					else
					{
						this.storagePolicyId = BlockStoragePolicySuite.IdUnspecified;
					}
					// read clientId and callId
					ReadRpcIds(@in, logVersion);
				}
				else
				{
					this.clientName = string.Empty;
					this.clientMachine = string.Empty;
				}
			}

			public const int MaxBlocks = 1024 * 1024 * 64;

			/// <exception cref="System.IO.IOException"/>
			private static Block[] ReadBlocks(DataInputStream @in, int logVersion)
			{
				int numBlocks = @in.ReadInt();
				if (numBlocks < 0)
				{
					throw new IOException("invalid negative number of blocks");
				}
				else
				{
					if (numBlocks > MaxBlocks)
					{
						throw new IOException("invalid number of blocks: " + numBlocks + ".  The maximum number of blocks per file is "
							 + MaxBlocks);
					}
				}
				Block[] blocks = new Block[numBlocks];
				for (int i = 0; i < numBlocks; i++)
				{
					Block blk = new Block();
					blk.ReadFields(@in);
					blocks[i] = blk;
				}
				return blocks;
			}

			public virtual string StringifyMembers()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("[length=");
				builder.Append(length);
				builder.Append(", inodeId=");
				builder.Append(inodeId);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", replication=");
				builder.Append(replication);
				builder.Append(", mtime=");
				builder.Append(mtime);
				builder.Append(", atime=");
				builder.Append(atime);
				builder.Append(", blockSize=");
				builder.Append(blockSize);
				builder.Append(", blocks=");
				builder.Append(Arrays.ToString(blocks));
				builder.Append(", permissions=");
				builder.Append(permissions);
				builder.Append(", aclEntries=");
				builder.Append(aclEntries);
				builder.Append(", clientName=");
				builder.Append(clientName);
				builder.Append(", clientMachine=");
				builder.Append(clientMachine);
				builder.Append(", overwrite=");
				builder.Append(overwrite);
				if (this.opCode == FSEditLogOpCodes.OpAdd)
				{
					AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				}
				builder.Append(", storagePolicyId=");
				builder.Append(storagePolicyId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "INODEID", System.Convert.ToString(inodeId)
					);
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "REPLICATION", short.ValueOf(replication).ToString
					());
				XMLUtils.AddSaxString(contentHandler, "MTIME", System.Convert.ToString(mtime));
				XMLUtils.AddSaxString(contentHandler, "ATIME", System.Convert.ToString(atime));
				XMLUtils.AddSaxString(contentHandler, "BLOCKSIZE", System.Convert.ToString(blockSize
					));
				XMLUtils.AddSaxString(contentHandler, "CLIENT_NAME", clientName);
				XMLUtils.AddSaxString(contentHandler, "CLIENT_MACHINE", clientMachine);
				XMLUtils.AddSaxString(contentHandler, "OVERWRITE", bool.ToString(overwrite));
				foreach (Block b in blocks)
				{
					FSEditLogOp.BlockToXml(contentHandler, b);
				}
				FSEditLogOp.PermissionStatusToXml(contentHandler, permissions);
				if (this.opCode == FSEditLogOpCodes.OpAdd)
				{
					if (aclEntries != null)
					{
						AppendAclEntriesToXml(contentHandler, aclEntries);
					}
					AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.inodeId = long.Parse(st.GetValue("INODEID"));
				this.path = st.GetValue("PATH");
				this.replication = short.ValueOf(st.GetValue("REPLICATION"));
				this.mtime = long.Parse(st.GetValue("MTIME"));
				this.atime = long.Parse(st.GetValue("ATIME"));
				this.blockSize = long.Parse(st.GetValue("BLOCKSIZE"));
				this.clientName = st.GetValue("CLIENT_NAME");
				this.clientMachine = st.GetValue("CLIENT_MACHINE");
				this.overwrite = System.Boolean.Parse(st.GetValueOrNull("OVERWRITE"));
				if (st.HasChildren("BLOCK"))
				{
					IList<XMLUtils.Stanza> blocks = st.GetChildren("BLOCK");
					this.blocks = new Block[blocks.Count];
					for (int i = 0; i < blocks.Count; i++)
					{
						this.blocks[i] = FSEditLogOp.BlockFromXml(blocks[i]);
					}
				}
				else
				{
					this.blocks = new Block[0];
				}
				this.permissions = PermissionStatusFromXml(st);
				aclEntries = ReadAclEntriesFromXml(st);
				ReadRpcIdsFromXml(st);
			}

			public abstract bool ShouldCompleteLastBlock();
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, string, Org.Apache.Hadoop.IO.EnumSetWritable{E}, bool, short, long, Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[])
		/// 	"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Append(string, string, Org.Apache.Hadoop.IO.EnumSetWritable{E})
		/// 	"/>
		/// </summary>
		internal class AddOp : FSEditLogOp.AddCloseOp
		{
			private AddOp()
				: base(FSEditLogOpCodes.OpAdd)
			{
			}

			internal static FSEditLogOp.AddOp GetInstance(FSEditLogOp.OpInstanceCache cache)
			{
				return (FSEditLogOp.AddOp)cache.Get(FSEditLogOpCodes.OpAdd);
			}

			public override bool ShouldCompleteLastBlock()
			{
				return false;
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AddOp ");
				builder.Append(StringifyMembers());
				return builder.ToString();
			}
		}

		/// <summary>
		/// Although
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Append(string, string, Org.Apache.Hadoop.IO.EnumSetWritable{E})
		/// 	"/>
		/// may also log a close op, we do
		/// not need to record the rpc ids here since a successful appendFile op will
		/// finally log an AddOp.
		/// </summary>
		internal class CloseOp : FSEditLogOp.AddCloseOp
		{
			private CloseOp()
				: base(FSEditLogOpCodes.OpClose)
			{
			}

			internal static FSEditLogOp.CloseOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.CloseOp)cache.Get(FSEditLogOpCodes.OpClose);
			}

			public override bool ShouldCompleteLastBlock()
			{
				return true;
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("CloseOp ");
				builder.Append(StringifyMembers());
				return builder.ToString();
			}
		}

		internal class AppendOp : FSEditLogOp
		{
			internal string path;

			internal string clientName;

			internal string clientMachine;

			internal bool newBlock;

			private AppendOp()
				: base(FSEditLogOpCodes.OpAppend)
			{
			}

			internal static FSEditLogOp.AppendOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.AppendOp)cache.Get(FSEditLogOpCodes.OpAppend);
			}

			internal virtual FSEditLogOp.AppendOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.AppendOp SetClientName(string clientName)
			{
				this.clientName = clientName;
				return this;
			}

			internal virtual FSEditLogOp.AppendOp SetClientMachine(string clientMachine)
			{
				this.clientMachine = clientMachine;
				return this;
			}

			internal virtual FSEditLogOp.AppendOp SetNewBlock(bool newBlock)
			{
				this.newBlock = newBlock;
				return this;
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AppendOp ");
				builder.Append("[path=").Append(path);
				builder.Append(", clientName=").Append(clientName);
				builder.Append(", clientMachine=").Append(clientMachine);
				builder.Append(", newBlock=").Append(newBlock).Append("]");
				return builder.ToString();
			}

			internal override void ResetSubFields()
			{
				this.path = null;
				this.clientName = null;
				this.clientMachine = null;
				this.newBlock = false;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.path = FSImageSerialization.ReadString(@in);
				this.clientName = FSImageSerialization.ReadString(@in);
				this.clientMachine = FSImageSerialization.ReadString(@in);
				this.newBlock = FSImageSerialization.ReadBoolean(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteString(clientName, @out);
				FSImageSerialization.WriteString(clientMachine, @out);
				FSImageSerialization.WriteBoolean(newBlock, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "CLIENT_NAME", clientName);
				XMLUtils.AddSaxString(contentHandler, "CLIENT_MACHINE", clientMachine);
				XMLUtils.AddSaxString(contentHandler, "NEWBLOCK", bool.ToString(newBlock));
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.path = st.GetValue("PATH");
				this.clientName = st.GetValue("CLIENT_NAME");
				this.clientMachine = st.GetValue("CLIENT_MACHINE");
				this.newBlock = System.Boolean.Parse(st.GetValue("NEWBLOCK"));
				ReadRpcIdsFromXml(st);
			}
		}

		internal class AddBlockOp : FSEditLogOp
		{
			private string path;

			private Block penultimateBlock;

			private Block lastBlock;

			private AddBlockOp()
				: base(FSEditLogOpCodes.OpAddBlock)
			{
			}

			internal static FSEditLogOp.AddBlockOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.AddBlockOp)cache.Get(FSEditLogOpCodes.OpAddBlock);
			}

			internal override void ResetSubFields()
			{
				path = null;
				penultimateBlock = null;
				lastBlock = null;
			}

			internal virtual FSEditLogOp.AddBlockOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			public virtual string GetPath()
			{
				return path;
			}

			internal virtual FSEditLogOp.AddBlockOp SetPenultimateBlock(Block pBlock)
			{
				this.penultimateBlock = pBlock;
				return this;
			}

			internal virtual Block GetPenultimateBlock()
			{
				return penultimateBlock;
			}

			internal virtual FSEditLogOp.AddBlockOp SetLastBlock(Block lastBlock)
			{
				this.lastBlock = lastBlock;
				return this;
			}

			internal virtual Block GetLastBlock()
			{
				return lastBlock;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				int size = penultimateBlock != null ? 2 : 1;
				Block[] blocks = new Block[size];
				if (penultimateBlock != null)
				{
					blocks[0] = penultimateBlock;
				}
				blocks[size - 1] = lastBlock;
				FSImageSerialization.WriteCompactBlockArray(blocks, @out);
				// clientId and callId
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				path = FSImageSerialization.ReadString(@in);
				Block[] blocks = FSImageSerialization.ReadCompactBlockArray(@in, logVersion);
				Preconditions.CheckState(blocks.Length == 2 || blocks.Length == 1);
				penultimateBlock = blocks.Length == 1 ? null : blocks[0];
				lastBlock = blocks[blocks.Length - 1];
				ReadRpcIds(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("AddBlockOp [path=").Append(path).Append(", penultimateBlock=").Append(
					penultimateBlock == null ? "NULL" : penultimateBlock).Append(", lastBlock=").Append
					(lastBlock);
				AppendRpcIdsToString(sb, rpcClientId, rpcCallId);
				sb.Append("]");
				return sb.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				if (penultimateBlock != null)
				{
					FSEditLogOp.BlockToXml(contentHandler, penultimateBlock);
				}
				FSEditLogOp.BlockToXml(contentHandler, lastBlock);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.path = st.GetValue("PATH");
				IList<XMLUtils.Stanza> blocks = st.GetChildren("BLOCK");
				int size = blocks.Count;
				Preconditions.CheckState(size == 1 || size == 2);
				this.penultimateBlock = size == 2 ? FSEditLogOp.BlockFromXml(blocks[0]) : null;
				this.lastBlock = FSEditLogOp.BlockFromXml(blocks[size - 1]);
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.UpdatePipeline(string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID[], string[])
		/// 	"/>
		/// , but
		/// <literal>@Idempotent</literal>
		/// for some other ops.
		/// </summary>
		internal class UpdateBlocksOp : FSEditLogOp, FSEditLogOp.BlockListUpdatingOp
		{
			internal string path;

			internal Block[] blocks;

			private UpdateBlocksOp()
				: base(FSEditLogOpCodes.OpUpdateBlocks)
			{
			}

			internal static FSEditLogOp.UpdateBlocksOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.UpdateBlocksOp)cache.Get(FSEditLogOpCodes.OpUpdateBlocks);
			}

			internal override void ResetSubFields()
			{
				path = null;
				blocks = null;
			}

			internal virtual FSEditLogOp.UpdateBlocksOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			public virtual string GetPath()
			{
				return path;
			}

			internal virtual FSEditLogOp.UpdateBlocksOp SetBlocks(Block[] blocks)
			{
				this.blocks = blocks;
				return this;
			}

			public virtual Block[] GetBlocks()
			{
				return blocks;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteCompactBlockArray(blocks, @out);
				// clientId and callId
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				path = FSImageSerialization.ReadString(@in);
				this.blocks = FSImageSerialization.ReadCompactBlockArray(@in, logVersion);
				ReadRpcIds(@in, logVersion);
			}

			public virtual bool ShouldCompleteLastBlock()
			{
				return false;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("UpdateBlocksOp [path=").Append(path).Append(", blocks=").Append(Arrays
					.ToString(blocks));
				AppendRpcIdsToString(sb, rpcClientId, rpcCallId);
				sb.Append("]");
				return sb.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				foreach (Block b in blocks)
				{
					FSEditLogOp.BlockToXml(contentHandler, b);
				}
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.path = st.GetValue("PATH");
				IList<XMLUtils.Stanza> blocks = st.GetChildren("BLOCK");
				this.blocks = new Block[blocks.Count];
				for (int i = 0; i < blocks.Count; i++)
				{
					this.blocks[i] = FSEditLogOp.BlockFromXml(blocks[i]);
				}
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetReplication(string, short)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetReplicationOp : FSEditLogOp
		{
			internal string path;

			internal short replication;

			private SetReplicationOp()
				: base(FSEditLogOpCodes.OpSetReplication)
			{
			}

			internal static FSEditLogOp.SetReplicationOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetReplicationOp)cache.Get(FSEditLogOpCodes.OpSetReplication);
			}

			internal override void ResetSubFields()
			{
				path = null;
				replication = 0;
			}

			internal virtual FSEditLogOp.SetReplicationOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.SetReplicationOp SetReplication(short replication)
			{
				this.replication = replication;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteShort(replication, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.path = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.replication = FSImageSerialization.ReadShort(@in);
				}
				else
				{
					this.replication = ReadShort(@in);
				}
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetReplicationOp [path=");
				builder.Append(path);
				builder.Append(", replication=");
				builder.Append(replication);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "REPLICATION", short.ValueOf(replication).ToString
					());
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.path = st.GetValue("PATH");
				this.replication = short.ValueOf(st.GetValue("REPLICATION"));
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Concat(string, string[])
		/// 	"/>
		/// 
		/// </summary>
		internal class ConcatDeleteOp : FSEditLogOp
		{
			internal int length;

			internal string trg;

			internal string[] srcs;

			internal long timestamp;

			public const int MaxConcatSrc = 1024 * 1024;

			private ConcatDeleteOp()
				: base(FSEditLogOpCodes.OpConcatDelete)
			{
			}

			internal static FSEditLogOp.ConcatDeleteOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.ConcatDeleteOp)cache.Get(FSEditLogOpCodes.OpConcatDelete);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				trg = null;
				srcs = null;
				timestamp = 0L;
			}

			internal virtual FSEditLogOp.ConcatDeleteOp SetTarget(string trg)
			{
				this.trg = trg;
				return this;
			}

			internal virtual FSEditLogOp.ConcatDeleteOp SetSources(string[] srcs)
			{
				if (srcs.Length > MaxConcatSrc)
				{
					throw new RuntimeException("ConcatDeleteOp can only have " + MaxConcatSrc + " sources at most."
						);
				}
				this.srcs = srcs;
				return this;
			}

			internal virtual FSEditLogOp.ConcatDeleteOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(trg, @out);
				DeprecatedUTF8[] info = new DeprecatedUTF8[srcs.Length];
				int idx = 0;
				for (int i = 0; i < srcs.Length; i++)
				{
					info[idx++] = new DeprecatedUTF8(srcs[i]);
				}
				new ArrayWritable(typeof(DeprecatedUTF8), info).Write(@out);
				FSImageSerialization.WriteLong(timestamp, @out);
				// rpc ids
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (length < 3)
					{
						// trg, srcs.., timestamp
						throw new IOException("Incorrect data format " + "for ConcatDeleteOp.");
					}
				}
				this.trg = FSImageSerialization.ReadString(@in);
				int srcSize = 0;
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					srcSize = @in.ReadInt();
				}
				else
				{
					srcSize = this.length - 1 - 1;
				}
				// trg and timestamp
				if (srcSize < 0)
				{
					throw new IOException("Incorrect data format. " + "ConcatDeleteOp cannot have a negative number of data "
						 + " sources.");
				}
				else
				{
					if (srcSize > MaxConcatSrc)
					{
						throw new IOException("Incorrect data format. " + "ConcatDeleteOp can have at most "
							 + MaxConcatSrc + " sources, but we tried to have " + (length - 3) + " sources."
							);
					}
				}
				this.srcs = new string[srcSize];
				for (int i = 0; i < srcSize; i++)
				{
					srcs[i] = FSImageSerialization.ReadString(@in);
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.timestamp = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.timestamp = ReadLong(@in);
				}
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("ConcatDeleteOp [length=");
				builder.Append(length);
				builder.Append(", trg=");
				builder.Append(trg);
				builder.Append(", srcs=");
				builder.Append(Arrays.ToString(srcs));
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "TRG", trg);
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				contentHandler.StartElement(string.Empty, string.Empty, "SOURCES", new AttributesImpl
					());
				for (int i = 0; i < srcs.Length; ++i)
				{
					XMLUtils.AddSaxString(contentHandler, "SOURCE" + (i + 1), srcs[i]);
				}
				contentHandler.EndElement(string.Empty, string.Empty, "SOURCES");
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.trg = st.GetValue("TRG");
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				IList<XMLUtils.Stanza> sources = st.GetChildren("SOURCES");
				int i = 0;
				while (true)
				{
					if (!sources[0].HasChildren("SOURCE" + (i + 1)))
					{
						break;
					}
					i++;
				}
				srcs = new string[i];
				for (i = 0; i < srcs.Length; i++)
				{
					srcs[i] = sources[0].GetValue("SOURCE" + (i + 1));
				}
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Rename(string, string)"
		/// 	/>
		/// 
		/// </summary>
		internal class RenameOldOp : FSEditLogOp
		{
			internal int length;

			internal string src;

			internal string dst;

			internal long timestamp;

			private RenameOldOp()
				: base(FSEditLogOpCodes.OpRenameOld)
			{
			}

			internal static FSEditLogOp.RenameOldOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.RenameOldOp)cache.Get(FSEditLogOpCodes.OpRenameOld);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				src = null;
				dst = null;
				timestamp = 0L;
			}

			internal virtual FSEditLogOp.RenameOldOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.RenameOldOp SetDestination(string dst)
			{
				this.dst = dst;
				return this;
			}

			internal virtual FSEditLogOp.RenameOldOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteString(dst, @out);
				FSImageSerialization.WriteLong(timestamp, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (this.length != 3)
					{
						throw new IOException("Incorrect data format. " + "Old rename operation.");
					}
				}
				this.src = FSImageSerialization.ReadString(@in);
				this.dst = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.timestamp = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.timestamp = ReadLong(@in);
				}
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RenameOldOp [length=");
				builder.Append(length);
				builder.Append(", src=");
				builder.Append(src);
				builder.Append(", dst=");
				builder.Append(dst);
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "DST", dst);
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.src = st.GetValue("SRC");
				this.dst = st.GetValue("DST");
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Delete(string, bool)"/>
		/// 
		/// </summary>
		internal class DeleteOp : FSEditLogOp
		{
			internal int length;

			internal string path;

			internal long timestamp;

			private DeleteOp()
				: base(FSEditLogOpCodes.OpDelete)
			{
			}

			internal static FSEditLogOp.DeleteOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.DeleteOp)cache.Get(FSEditLogOpCodes.OpDelete);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				path = null;
				timestamp = 0L;
			}

			internal virtual FSEditLogOp.DeleteOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.DeleteOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteLong(timestamp, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (this.length != 2)
					{
						throw new IOException("Incorrect data format. " + "delete operation.");
					}
				}
				this.path = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.timestamp = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.timestamp = ReadLong(@in);
				}
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("DeleteOp [length=");
				builder.Append(length);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.path = st.GetValue("PATH");
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Mkdirs(string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)
		/// 	"/>
		/// 
		/// </summary>
		internal class MkdirOp : FSEditLogOp
		{
			internal int length;

			internal long inodeId;

			internal string path;

			internal long timestamp;

			internal PermissionStatus permissions;

			internal IList<AclEntry> aclEntries;

			internal IList<XAttr> xAttrs;

			private MkdirOp()
				: base(FSEditLogOpCodes.OpMkdir)
			{
			}

			internal static FSEditLogOp.MkdirOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.MkdirOp)cache.Get(FSEditLogOpCodes.OpMkdir);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				inodeId = 0L;
				path = null;
				timestamp = 0L;
				permissions = null;
				aclEntries = null;
				xAttrs = null;
			}

			internal virtual FSEditLogOp.MkdirOp SetInodeId(long inodeId)
			{
				this.inodeId = inodeId;
				return this;
			}

			internal virtual FSEditLogOp.MkdirOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.MkdirOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			internal virtual FSEditLogOp.MkdirOp SetPermissionStatus(PermissionStatus permissions
				)
			{
				this.permissions = permissions;
				return this;
			}

			internal virtual FSEditLogOp.MkdirOp SetAclEntries(IList<AclEntry> aclEntries)
			{
				this.aclEntries = aclEntries;
				return this;
			}

			internal virtual FSEditLogOp.MkdirOp SetXAttrs(IList<XAttr> xAttrs)
			{
				this.xAttrs = xAttrs;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(inodeId, @out);
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteLong(timestamp, @out);
				// mtime
				FSImageSerialization.WriteLong(timestamp, @out);
				// atime, unused at this
				permissions.Write(@out);
				FSEditLogOp.AclEditLogUtil.Write(aclEntries, @out);
				XAttrProtos.XAttrEditLogProto.Builder b = XAttrProtos.XAttrEditLogProto.NewBuilder
					();
				b.AddAllXAttrs(PBHelper.ConvertXAttrProto(xAttrs));
				((XAttrProtos.XAttrEditLogProto)b.Build()).WriteDelimitedTo(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
				}
				if (-17 < logVersion && length != 2 || logVersion <= -17 && length != 3 && !NameNodeLayoutVersion
					.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion))
				{
					throw new IOException("Incorrect data format. Mkdir operation.");
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, logVersion))
				{
					this.inodeId = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					// This id should be updated when this editLogOp is applied
					this.inodeId = INodeId.GrandfatherInodeId;
				}
				this.path = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.timestamp = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.timestamp = ReadLong(@in);
				}
				// The disk format stores atimes for directories as well.
				// However, currently this is not being updated/used because of
				// performance reasons.
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FileAccessTime, logVersion
					))
				{
					if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
						))
					{
						FSImageSerialization.ReadLong(@in);
					}
					else
					{
						ReadLong(@in);
					}
				}
				this.permissions = PermissionStatus.Read(@in);
				aclEntries = FSEditLogOp.AclEditLogUtil.Read(@in, logVersion);
				xAttrs = ReadXAttrsFromEditLog(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("MkdirOp [length=");
				builder.Append(length);
				builder.Append(", inodeId=");
				builder.Append(inodeId);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				builder.Append(", permissions=");
				builder.Append(permissions);
				builder.Append(", aclEntries=");
				builder.Append(aclEntries);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append(", xAttrs=");
				builder.Append(xAttrs);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "INODEID", System.Convert.ToString(inodeId)
					);
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				FSEditLogOp.PermissionStatusToXml(contentHandler, permissions);
				if (aclEntries != null)
				{
					AppendAclEntriesToXml(contentHandler, aclEntries);
				}
				if (xAttrs != null)
				{
					AppendXAttrsToXml(contentHandler, xAttrs);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.inodeId = long.Parse(st.GetValue("INODEID"));
				this.path = st.GetValue("PATH");
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				this.permissions = PermissionStatusFromXml(st);
				aclEntries = ReadAclEntriesFromXml(st);
				xAttrs = ReadXAttrsFromXml(st);
			}
		}

		/// <summary>
		/// The corresponding operations are either
		/// <literal>@Idempotent</literal>
		/// (
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.UpdateBlockForPipeline(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, string)
		/// 	"/>
		/// ,
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RecoverLease(string, string)
		/// 	"/>
		/// ,
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddBlock(string, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], long, string[])
		/// 	"/>
		/// ) or
		/// already bound with other editlog op which records rpc ids (
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, string, Org.Apache.Hadoop.IO.EnumSetWritable{E}, bool, short, long, Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[])
		/// 	"/>
		/// ). Thus no need to record rpc ids here.
		/// </summary>
		internal class SetGenstampV1Op : FSEditLogOp
		{
			internal long genStampV1;

			private SetGenstampV1Op()
				: base(FSEditLogOpCodes.OpSetGenstampV1)
			{
			}

			internal static FSEditLogOp.SetGenstampV1Op GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetGenstampV1Op)cache.Get(FSEditLogOpCodes.OpSetGenstampV1);
			}

			internal override void ResetSubFields()
			{
				genStampV1 = 0L;
			}

			internal virtual FSEditLogOp.SetGenstampV1Op SetGenerationStamp(long genStamp)
			{
				this.genStampV1 = genStamp;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(genStampV1, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.genStampV1 = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetGenstampOp [GenStamp=");
				builder.Append(genStampV1);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "GENSTAMP", System.Convert.ToString(genStampV1
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.genStampV1 = long.Parse(st.GetValue("GENSTAMP"));
			}
		}

		/// <summary>
		/// Similar with
		/// <see cref="SetGenstampV1Op"/>
		/// 
		/// </summary>
		internal class SetGenstampV2Op : FSEditLogOp
		{
			internal long genStampV2;

			private SetGenstampV2Op()
				: base(FSEditLogOpCodes.OpSetGenstampV2)
			{
			}

			internal static FSEditLogOp.SetGenstampV2Op GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetGenstampV2Op)cache.Get(FSEditLogOpCodes.OpSetGenstampV2);
			}

			internal override void ResetSubFields()
			{
				genStampV2 = 0L;
			}

			internal virtual FSEditLogOp.SetGenstampV2Op SetGenerationStamp(long genStamp)
			{
				this.genStampV2 = genStamp;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(genStampV2, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.genStampV2 = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetGenstampV2Op [GenStampV2=");
				builder.Append(genStampV2);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "GENSTAMPV2", System.Convert.ToString(genStampV2
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.genStampV2 = long.Parse(st.GetValue("GENSTAMPV2"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddBlock(string, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], long, string[])
		/// 	"/>
		/// 
		/// </summary>
		internal class AllocateBlockIdOp : FSEditLogOp
		{
			internal long blockId;

			private AllocateBlockIdOp()
				: base(FSEditLogOpCodes.OpAllocateBlockId)
			{
			}

			internal static FSEditLogOp.AllocateBlockIdOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.AllocateBlockIdOp)cache.Get(FSEditLogOpCodes.OpAllocateBlockId
					);
			}

			internal override void ResetSubFields()
			{
				blockId = 0L;
			}

			internal virtual FSEditLogOp.AllocateBlockIdOp SetBlockId(long blockId)
			{
				this.blockId = blockId;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(blockId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.blockId = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AllocateBlockIdOp [blockId=");
				builder.Append(blockId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "BLOCK_ID", System.Convert.ToString(blockId
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.blockId = long.Parse(st.GetValue("BLOCK_ID"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetPermission(string, Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetPermissionsOp : FSEditLogOp
		{
			internal string src;

			internal FsPermission permissions;

			private SetPermissionsOp()
				: base(FSEditLogOpCodes.OpSetPermissions)
			{
			}

			internal static FSEditLogOp.SetPermissionsOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetPermissionsOp)cache.Get(FSEditLogOpCodes.OpSetPermissions);
			}

			internal override void ResetSubFields()
			{
				src = null;
				permissions = null;
			}

			internal virtual FSEditLogOp.SetPermissionsOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.SetPermissionsOp SetPermissions(FsPermission permissions
				)
			{
				this.permissions = permissions;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				permissions.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
				this.permissions = FsPermission.Read(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetPermissionsOp [src=");
				builder.Append(src);
				builder.Append(", permissions=");
				builder.Append(permissions);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "MODE", short.ValueOf(permissions.ToShort()
					).ToString());
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.permissions = new FsPermission(short.ValueOf(st.GetValue("MODE")));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetOwner(string, string, string)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetOwnerOp : FSEditLogOp
		{
			internal string src;

			internal string username;

			internal string groupname;

			private SetOwnerOp()
				: base(FSEditLogOpCodes.OpSetOwner)
			{
			}

			internal static FSEditLogOp.SetOwnerOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.SetOwnerOp)cache.Get(FSEditLogOpCodes.OpSetOwner);
			}

			internal override void ResetSubFields()
			{
				src = null;
				username = null;
				groupname = null;
			}

			internal virtual FSEditLogOp.SetOwnerOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.SetOwnerOp SetUser(string username)
			{
				this.username = username;
				return this;
			}

			internal virtual FSEditLogOp.SetOwnerOp SetGroup(string groupname)
			{
				this.groupname = groupname;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteString(username == null ? string.Empty : username, @out
					);
				FSImageSerialization.WriteString(groupname == null ? string.Empty : groupname, @out
					);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
				this.username = FSImageSerialization.ReadString_EmptyAsNull(@in);
				this.groupname = FSImageSerialization.ReadString_EmptyAsNull(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetOwnerOp [src=");
				builder.Append(src);
				builder.Append(", username=");
				builder.Append(username);
				builder.Append(", groupname=");
				builder.Append(groupname);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				if (username != null)
				{
					XMLUtils.AddSaxString(contentHandler, "USERNAME", username);
				}
				if (groupname != null)
				{
					XMLUtils.AddSaxString(contentHandler, "GROUPNAME", groupname);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.username = (st.HasChildren("USERNAME")) ? st.GetValue("USERNAME") : null;
				this.groupname = (st.HasChildren("GROUPNAME")) ? st.GetValue("GROUPNAME") : null;
			}
		}

		internal class SetNSQuotaOp : FSEditLogOp
		{
			internal string src;

			internal long nsQuota;

			private SetNSQuotaOp()
				: base(FSEditLogOpCodes.OpSetNsQuota)
			{
			}

			internal static FSEditLogOp.SetNSQuotaOp GetInstance(FSEditLogOp.OpInstanceCache 
				cache)
			{
				return (FSEditLogOp.SetNSQuotaOp)cache.Get(FSEditLogOpCodes.OpSetNsQuota);
			}

			internal override void ResetSubFields()
			{
				src = null;
				nsQuota = 0L;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				throw new IOException("Deprecated");
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
				this.nsQuota = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetNSQuotaOp [src=");
				builder.Append(src);
				builder.Append(", nsQuota=");
				builder.Append(nsQuota);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "NSQUOTA", System.Convert.ToString(nsQuota)
					);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.nsQuota = long.Parse(st.GetValue("NSQUOTA"));
			}
		}

		internal class ClearNSQuotaOp : FSEditLogOp
		{
			internal string src;

			private ClearNSQuotaOp()
				: base(FSEditLogOpCodes.OpClearNsQuota)
			{
			}

			internal static FSEditLogOp.ClearNSQuotaOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.ClearNSQuotaOp)cache.Get(FSEditLogOpCodes.OpClearNsQuota);
			}

			internal override void ResetSubFields()
			{
				src = null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				throw new IOException("Deprecated");
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("ClearNSQuotaOp [src=");
				builder.Append(src);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetQuotaOp : FSEditLogOp
		{
			internal string src;

			internal long nsQuota;

			internal long dsQuota;

			private SetQuotaOp()
				: base(FSEditLogOpCodes.OpSetQuota)
			{
			}

			internal static FSEditLogOp.SetQuotaOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.SetQuotaOp)cache.Get(FSEditLogOpCodes.OpSetQuota);
			}

			internal override void ResetSubFields()
			{
				src = null;
				nsQuota = 0L;
				dsQuota = 0L;
			}

			internal virtual FSEditLogOp.SetQuotaOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.SetQuotaOp SetNSQuota(long nsQuota)
			{
				this.nsQuota = nsQuota;
				return this;
			}

			internal virtual FSEditLogOp.SetQuotaOp SetDSQuota(long dsQuota)
			{
				this.dsQuota = dsQuota;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteLong(nsQuota, @out);
				FSImageSerialization.WriteLong(dsQuota, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
				this.nsQuota = FSImageSerialization.ReadLong(@in);
				this.dsQuota = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetQuotaOp [src=");
				builder.Append(src);
				builder.Append(", nsQuota=");
				builder.Append(nsQuota);
				builder.Append(", dsQuota=");
				builder.Append(dsQuota);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "NSQUOTA", System.Convert.ToString(nsQuota)
					);
				XMLUtils.AddSaxString(contentHandler, "DSQUOTA", System.Convert.ToString(dsQuota)
					);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.nsQuota = long.Parse(st.GetValue("NSQUOTA"));
				this.dsQuota = long.Parse(st.GetValue("DSQUOTA"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetQuotaByStorageTypeOp : FSEditLogOp
		{
			internal string src;

			internal long dsQuota;

			internal StorageType type;

			private SetQuotaByStorageTypeOp()
				: base(FSEditLogOpCodes.OpSetQuotaByStoragetype)
			{
			}

			internal static FSEditLogOp.SetQuotaByStorageTypeOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetQuotaByStorageTypeOp)cache.Get(FSEditLogOpCodes.OpSetQuotaByStoragetype
					);
			}

			internal override void ResetSubFields()
			{
				src = null;
				dsQuota = -1L;
				type = StorageType.Default;
			}

			internal virtual FSEditLogOp.SetQuotaByStorageTypeOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.SetQuotaByStorageTypeOp SetQuotaByStorageType(long dsQuota
				, StorageType type)
			{
				this.type = type;
				this.dsQuota = dsQuota;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteInt((int)(type), @out);
				FSImageSerialization.WriteLong(dsQuota, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.src = FSImageSerialization.ReadString(@in);
				this.type = StorageType.ParseStorageType(FSImageSerialization.ReadInt(@in));
				this.dsQuota = FSImageSerialization.ReadLong(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetTypeQuotaOp [src=");
				builder.Append(src);
				builder.Append(", storageType=");
				builder.Append(type);
				builder.Append(", dsQuota=");
				builder.Append(dsQuota);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "STORAGETYPE", Sharpen.Extensions.ToString(
					(int)(type)));
				XMLUtils.AddSaxString(contentHandler, "DSQUOTA", System.Convert.ToString(dsQuota)
					);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.type = StorageType.ParseStorageType(System.Convert.ToInt32(st.GetValue("STORAGETYPE"
					)));
				this.dsQuota = long.Parse(st.GetValue("DSQUOTA"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetTimes(string, long, long)
		/// 	"/>
		/// 
		/// </summary>
		internal class TimesOp : FSEditLogOp
		{
			internal int length;

			internal string path;

			internal long mtime;

			internal long atime;

			private TimesOp()
				: base(FSEditLogOpCodes.OpTimes)
			{
			}

			internal static FSEditLogOp.TimesOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.TimesOp)cache.Get(FSEditLogOpCodes.OpTimes);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				path = null;
				mtime = 0L;
				atime = 0L;
			}

			internal virtual FSEditLogOp.TimesOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.TimesOp SetModificationTime(long mtime)
			{
				this.mtime = mtime;
				return this;
			}

			internal virtual FSEditLogOp.TimesOp SetAccessTime(long atime)
			{
				this.atime = atime;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteLong(mtime, @out);
				FSImageSerialization.WriteLong(atime, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (length != 3)
					{
						throw new IOException("Incorrect data format. " + "times operation.");
					}
				}
				this.path = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.mtime = FSImageSerialization.ReadLong(@in);
					this.atime = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.mtime = ReadLong(@in);
					this.atime = ReadLong(@in);
				}
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("TimesOp [length=");
				builder.Append(length);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", mtime=");
				builder.Append(mtime);
				builder.Append(", atime=");
				builder.Append(atime);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "MTIME", System.Convert.ToString(mtime));
				XMLUtils.AddSaxString(contentHandler, "ATIME", System.Convert.ToString(atime));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.path = st.GetValue("PATH");
				this.mtime = long.Parse(st.GetValue("MTIME"));
				this.atime = long.Parse(st.GetValue("ATIME"));
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.CreateSymlink(string, string, Org.Apache.Hadoop.FS.Permission.FsPermission, bool)
		/// 	"/>
		/// 
		/// </summary>
		internal class SymlinkOp : FSEditLogOp
		{
			internal int length;

			internal long inodeId;

			internal string path;

			internal string value;

			internal long mtime;

			internal long atime;

			internal PermissionStatus permissionStatus;

			private SymlinkOp()
				: base(FSEditLogOpCodes.OpSymlink)
			{
			}

			internal static FSEditLogOp.SymlinkOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.SymlinkOp)cache.Get(FSEditLogOpCodes.OpSymlink);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				inodeId = 0L;
				path = null;
				value = null;
				mtime = 0L;
				atime = 0L;
				permissionStatus = null;
			}

			internal virtual FSEditLogOp.SymlinkOp SetId(long inodeId)
			{
				this.inodeId = inodeId;
				return this;
			}

			internal virtual FSEditLogOp.SymlinkOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.SymlinkOp SetValue(string value)
			{
				this.value = value;
				return this;
			}

			internal virtual FSEditLogOp.SymlinkOp SetModificationTime(long mtime)
			{
				this.mtime = mtime;
				return this;
			}

			internal virtual FSEditLogOp.SymlinkOp SetAccessTime(long atime)
			{
				this.atime = atime;
				return this;
			}

			internal virtual FSEditLogOp.SymlinkOp SetPermissionStatus(PermissionStatus permissionStatus
				)
			{
				this.permissionStatus = permissionStatus;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(inodeId, @out);
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteString(value, @out);
				FSImageSerialization.WriteLong(mtime, @out);
				FSImageSerialization.WriteLong(atime, @out);
				permissionStatus.Write(@out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (this.length != 4)
					{
						throw new IOException("Incorrect data format. " + "symlink operation.");
					}
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, logVersion))
				{
					this.inodeId = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					// This id should be updated when the editLogOp is applied
					this.inodeId = INodeId.GrandfatherInodeId;
				}
				this.path = FSImageSerialization.ReadString(@in);
				this.value = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.mtime = FSImageSerialization.ReadLong(@in);
					this.atime = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.mtime = ReadLong(@in);
					this.atime = ReadLong(@in);
				}
				this.permissionStatus = PermissionStatus.Read(@in);
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SymlinkOp [length=");
				builder.Append(length);
				builder.Append(", inodeId=");
				builder.Append(inodeId);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", value=");
				builder.Append(value);
				builder.Append(", mtime=");
				builder.Append(mtime);
				builder.Append(", atime=");
				builder.Append(atime);
				builder.Append(", permissionStatus=");
				builder.Append(permissionStatus);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "INODEID", System.Convert.ToString(inodeId)
					);
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "VALUE", value);
				XMLUtils.AddSaxString(contentHandler, "MTIME", System.Convert.ToString(mtime));
				XMLUtils.AddSaxString(contentHandler, "ATIME", System.Convert.ToString(atime));
				FSEditLogOp.PermissionStatusToXml(contentHandler, permissionStatus);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.inodeId = long.Parse(st.GetValue("INODEID"));
				this.path = st.GetValue("PATH");
				this.value = st.GetValue("VALUE");
				this.mtime = long.Parse(st.GetValue("MTIME"));
				this.atime = long.Parse(st.GetValue("ATIME"));
				this.permissionStatus = PermissionStatusFromXml(st);
				ReadRpcIdsFromXml(st);
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Rename2(string, string, Org.Apache.Hadoop.FS.Options.Rename[])
		/// 	"/>
		/// 
		/// </summary>
		internal class RenameOp : FSEditLogOp
		{
			internal int length;

			internal string src;

			internal string dst;

			internal long timestamp;

			internal Options.Rename[] options;

			private RenameOp()
				: base(FSEditLogOpCodes.OpRename)
			{
			}

			internal static FSEditLogOp.RenameOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.RenameOp)cache.Get(FSEditLogOpCodes.OpRename);
			}

			internal override void ResetSubFields()
			{
				length = 0;
				src = null;
				dst = null;
				timestamp = 0L;
				options = null;
			}

			internal virtual FSEditLogOp.RenameOp SetSource(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.RenameOp SetDestination(string dst)
			{
				this.dst = dst;
				return this;
			}

			internal virtual FSEditLogOp.RenameOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			internal virtual FSEditLogOp.RenameOp SetOptions(Options.Rename[] options)
			{
				this.options = options;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteString(dst, @out);
				FSImageSerialization.WriteLong(timestamp, @out);
				ToBytesWritable(options).Write(@out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, 
					logVersion))
				{
					this.length = @in.ReadInt();
					if (this.length != 3)
					{
						throw new IOException("Incorrect data format. " + "Rename operation.");
					}
				}
				this.src = FSImageSerialization.ReadString(@in);
				this.dst = FSImageSerialization.ReadString(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.timestamp = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.timestamp = ReadLong(@in);
				}
				this.options = ReadRenameOptions(@in);
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			private static Options.Rename[] ReadRenameOptions(DataInputStream @in)
			{
				BytesWritable writable = new BytesWritable();
				writable.ReadFields(@in);
				byte[] bytes = writable.GetBytes();
				Options.Rename[] options = new Options.Rename[bytes.Length];
				for (int i = 0; i < bytes.Length; i++)
				{
					options[i] = Options.Rename.ValueOf(bytes[i]);
				}
				return options;
			}

			internal static BytesWritable ToBytesWritable(params Options.Rename[] options)
			{
				byte[] bytes = new byte[options.Length];
				for (int i = 0; i < options.Length; i++)
				{
					bytes[i] = options[i].Value();
				}
				return new BytesWritable(bytes);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RenameOp [length=");
				builder.Append(length);
				builder.Append(", src=");
				builder.Append(src);
				builder.Append(", dst=");
				builder.Append(dst);
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				builder.Append(", options=");
				builder.Append(Arrays.ToString(options));
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LENGTH", Sharpen.Extensions.ToString(length
					));
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "DST", dst);
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				StringBuilder bld = new StringBuilder();
				string prefix = string.Empty;
				foreach (Options.Rename r in options)
				{
					bld.Append(prefix).Append(r.ToString());
					prefix = "|";
				}
				XMLUtils.AddSaxString(contentHandler, "OPTIONS", bld.ToString());
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.length = System.Convert.ToInt32(st.GetValue("LENGTH"));
				this.src = st.GetValue("SRC");
				this.dst = st.GetValue("DST");
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				string opts = st.GetValue("OPTIONS");
				string[] o = opts.Split("\\|");
				this.options = new Options.Rename[o.Length];
				for (int i = 0; i < o.Length; i++)
				{
					if (o[i].Equals(string.Empty))
					{
						continue;
					}
					try
					{
						this.options[i] = Options.Rename.ValueOf(o[i]);
					}
					finally
					{
						if (this.options[i] == null)
						{
							System.Console.Error.WriteLine("error parsing Rename value: \"" + o[i] + "\"");
						}
					}
				}
				ReadRpcIdsFromXml(st);
			}
		}

		internal class TruncateOp : FSEditLogOp
		{
			internal string src;

			internal string clientName;

			internal string clientMachine;

			internal long newLength;

			internal long timestamp;

			internal Block truncateBlock;

			private TruncateOp()
				: base(FSEditLogOpCodes.OpTruncate)
			{
			}

			internal static FSEditLogOp.TruncateOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.TruncateOp)cache.Get(FSEditLogOpCodes.OpTruncate);
			}

			internal override void ResetSubFields()
			{
				src = null;
				clientName = null;
				clientMachine = null;
				newLength = 0L;
				timestamp = 0L;
			}

			internal virtual FSEditLogOp.TruncateOp SetPath(string src)
			{
				this.src = src;
				return this;
			}

			internal virtual FSEditLogOp.TruncateOp SetClientName(string clientName)
			{
				this.clientName = clientName;
				return this;
			}

			internal virtual FSEditLogOp.TruncateOp SetClientMachine(string clientMachine)
			{
				this.clientMachine = clientMachine;
				return this;
			}

			internal virtual FSEditLogOp.TruncateOp SetNewLength(long newLength)
			{
				this.newLength = newLength;
				return this;
			}

			internal virtual FSEditLogOp.TruncateOp SetTimestamp(long timestamp)
			{
				this.timestamp = timestamp;
				return this;
			}

			internal virtual FSEditLogOp.TruncateOp SetTruncateBlock(Block truncateBlock)
			{
				this.truncateBlock = truncateBlock;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				src = FSImageSerialization.ReadString(@in);
				clientName = FSImageSerialization.ReadString(@in);
				clientMachine = FSImageSerialization.ReadString(@in);
				newLength = FSImageSerialization.ReadLong(@in);
				timestamp = FSImageSerialization.ReadLong(@in);
				Block[] blocks = FSImageSerialization.ReadCompactBlockArray(@in, logVersion);
				System.Diagnostics.Debug.Assert(blocks.Length <= 1, "Truncate op should have 1 or 0 blocks"
					);
				truncateBlock = (blocks.Length == 0) ? null : blocks[0];
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(src, @out);
				FSImageSerialization.WriteString(clientName, @out);
				FSImageSerialization.WriteString(clientMachine, @out);
				FSImageSerialization.WriteLong(newLength, @out);
				FSImageSerialization.WriteLong(timestamp, @out);
				int size = truncateBlock != null ? 1 : 0;
				Block[] blocks = new Block[size];
				if (truncateBlock != null)
				{
					blocks[0] = truncateBlock;
				}
				FSImageSerialization.WriteCompactBlockArray(blocks, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				XMLUtils.AddSaxString(contentHandler, "CLIENTNAME", clientName);
				XMLUtils.AddSaxString(contentHandler, "CLIENTMACHINE", clientMachine);
				XMLUtils.AddSaxString(contentHandler, "NEWLENGTH", System.Convert.ToString(newLength
					));
				XMLUtils.AddSaxString(contentHandler, "TIMESTAMP", System.Convert.ToString(timestamp
					));
				if (truncateBlock != null)
				{
					FSEditLogOp.BlockToXml(contentHandler, truncateBlock);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.src = st.GetValue("SRC");
				this.clientName = st.GetValue("CLIENTNAME");
				this.clientMachine = st.GetValue("CLIENTMACHINE");
				this.newLength = long.Parse(st.GetValue("NEWLENGTH"));
				this.timestamp = long.Parse(st.GetValue("TIMESTAMP"));
				if (st.HasChildren("BLOCK"))
				{
					this.truncateBlock = FSEditLogOp.BlockFromXml(st);
				}
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("TruncateOp [src=");
				builder.Append(src);
				builder.Append(", clientName=");
				builder.Append(clientName);
				builder.Append(", clientMachine=");
				builder.Append(clientMachine);
				builder.Append(", newLength=");
				builder.Append(newLength);
				builder.Append(", timestamp=");
				builder.Append(timestamp);
				builder.Append(", truncateBlock=");
				builder.Append(truncateBlock);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RecoverLease(string, string)
		/// 	"/>
		/// . In the
		/// meanwhile, startFile and appendFile both have their own corresponding
		/// editlog op.
		/// </summary>
		internal class ReassignLeaseOp : FSEditLogOp
		{
			internal string leaseHolder;

			internal string path;

			internal string newHolder;

			private ReassignLeaseOp()
				: base(FSEditLogOpCodes.OpReassignLease)
			{
			}

			internal static FSEditLogOp.ReassignLeaseOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.ReassignLeaseOp)cache.Get(FSEditLogOpCodes.OpReassignLease);
			}

			internal override void ResetSubFields()
			{
				leaseHolder = null;
				path = null;
				newHolder = null;
			}

			internal virtual FSEditLogOp.ReassignLeaseOp SetLeaseHolder(string leaseHolder)
			{
				this.leaseHolder = leaseHolder;
				return this;
			}

			internal virtual FSEditLogOp.ReassignLeaseOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.ReassignLeaseOp SetNewHolder(string newHolder)
			{
				this.newHolder = newHolder;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(leaseHolder, @out);
				FSImageSerialization.WriteString(path, @out);
				FSImageSerialization.WriteString(newHolder, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.leaseHolder = FSImageSerialization.ReadString(@in);
				this.path = FSImageSerialization.ReadString(@in);
				this.newHolder = FSImageSerialization.ReadString(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("ReassignLeaseOp [leaseHolder=");
				builder.Append(leaseHolder);
				builder.Append(", path=");
				builder.Append(path);
				builder.Append(", newHolder=");
				builder.Append(newHolder);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "LEASEHOLDER", leaseHolder);
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "NEWHOLDER", newHolder);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.leaseHolder = st.GetValue("LEASEHOLDER");
				this.path = st.GetValue("PATH");
				this.newHolder = st.GetValue("NEWHOLDER");
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetDelegationToken(Org.Apache.Hadoop.IO.Text)
		/// 	"/>
		/// 
		/// </summary>
		internal class GetDelegationTokenOp : FSEditLogOp
		{
			internal DelegationTokenIdentifier token;

			internal long expiryTime;

			private GetDelegationTokenOp()
				: base(FSEditLogOpCodes.OpGetDelegationToken)
			{
			}

			internal static FSEditLogOp.GetDelegationTokenOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.GetDelegationTokenOp)cache.Get(FSEditLogOpCodes.OpGetDelegationToken
					);
			}

			internal override void ResetSubFields()
			{
				token = null;
				expiryTime = 0L;
			}

			internal virtual FSEditLogOp.GetDelegationTokenOp SetDelegationTokenIdentifier(DelegationTokenIdentifier
				 token)
			{
				this.token = token;
				return this;
			}

			internal virtual FSEditLogOp.GetDelegationTokenOp SetExpiryTime(long expiryTime)
			{
				this.expiryTime = expiryTime;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				token.Write(@out);
				FSImageSerialization.WriteLong(expiryTime, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.token = new DelegationTokenIdentifier();
				this.token.ReadFields(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.expiryTime = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.expiryTime = ReadLong(@in);
				}
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("GetDelegationTokenOp [token=");
				builder.Append(token);
				builder.Append(", expiryTime=");
				builder.Append(expiryTime);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSEditLogOp.DelegationTokenToXml(contentHandler, token);
				XMLUtils.AddSaxString(contentHandler, "EXPIRY_TIME", System.Convert.ToString(expiryTime
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.token = DelegationTokenFromXml(st.GetChildren("DELEGATION_TOKEN_IDENTIFIER")
					[0]);
				this.expiryTime = long.Parse(st.GetValue("EXPIRY_TIME"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token{T})
		/// 	"/>
		/// 
		/// </summary>
		internal class RenewDelegationTokenOp : FSEditLogOp
		{
			internal DelegationTokenIdentifier token;

			internal long expiryTime;

			private RenewDelegationTokenOp()
				: base(FSEditLogOpCodes.OpRenewDelegationToken)
			{
			}

			internal static FSEditLogOp.RenewDelegationTokenOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RenewDelegationTokenOp)cache.Get(FSEditLogOpCodes.OpRenewDelegationToken
					);
			}

			internal override void ResetSubFields()
			{
				token = null;
				expiryTime = 0L;
			}

			internal virtual FSEditLogOp.RenewDelegationTokenOp SetDelegationTokenIdentifier(
				DelegationTokenIdentifier token)
			{
				this.token = token;
				return this;
			}

			internal virtual FSEditLogOp.RenewDelegationTokenOp SetExpiryTime(long expiryTime
				)
			{
				this.expiryTime = expiryTime;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				token.Write(@out);
				FSImageSerialization.WriteLong(expiryTime, @out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.token = new DelegationTokenIdentifier();
				this.token.ReadFields(@in);
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditlogOpOptimization, logVersion
					))
				{
					this.expiryTime = FSImageSerialization.ReadLong(@in);
				}
				else
				{
					this.expiryTime = ReadLong(@in);
				}
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RenewDelegationTokenOp [token=");
				builder.Append(token);
				builder.Append(", expiryTime=");
				builder.Append(expiryTime);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSEditLogOp.DelegationTokenToXml(contentHandler, token);
				XMLUtils.AddSaxString(contentHandler, "EXPIRY_TIME", System.Convert.ToString(expiryTime
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.token = DelegationTokenFromXml(st.GetChildren("DELEGATION_TOKEN_IDENTIFIER")
					[0]);
				this.expiryTime = long.Parse(st.GetValue("EXPIRY_TIME"));
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token{T})
		/// 	"/>
		/// 
		/// </summary>
		internal class CancelDelegationTokenOp : FSEditLogOp
		{
			internal DelegationTokenIdentifier token;

			private CancelDelegationTokenOp()
				: base(FSEditLogOpCodes.OpCancelDelegationToken)
			{
			}

			internal static FSEditLogOp.CancelDelegationTokenOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.CancelDelegationTokenOp)cache.Get(FSEditLogOpCodes.OpCancelDelegationToken
					);
			}

			internal override void ResetSubFields()
			{
				token = null;
			}

			internal virtual FSEditLogOp.CancelDelegationTokenOp SetDelegationTokenIdentifier
				(DelegationTokenIdentifier token)
			{
				this.token = token;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				token.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.token = new DelegationTokenIdentifier();
				this.token.ReadFields(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("CancelDelegationTokenOp [token=");
				builder.Append(token);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSEditLogOp.DelegationTokenToXml(contentHandler, token);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.token = DelegationTokenFromXml(st.GetChildren("DELEGATION_TOKEN_IDENTIFIER")
					[0]);
			}
		}

		internal class UpdateMasterKeyOp : FSEditLogOp
		{
			internal DelegationKey key;

			private UpdateMasterKeyOp()
				: base(FSEditLogOpCodes.OpUpdateMasterKey)
			{
			}

			internal static FSEditLogOp.UpdateMasterKeyOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.UpdateMasterKeyOp)cache.Get(FSEditLogOpCodes.OpUpdateMasterKey
					);
			}

			internal override void ResetSubFields()
			{
				key = null;
			}

			internal virtual FSEditLogOp.UpdateMasterKeyOp SetDelegationKey(DelegationKey key
				)
			{
				this.key = key;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				key.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.key = new DelegationKey();
				this.key.ReadFields(@in);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("UpdateMasterKeyOp [key=");
				builder.Append(key);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSEditLogOp.DelegationKeyToXml(contentHandler, key);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.key = DelegationKeyFromXml(st.GetChildren("DELEGATION_KEY")[0]);
			}
		}

		internal class LogSegmentOp : FSEditLogOp
		{
			private LogSegmentOp(FSEditLogOpCodes code)
				: base(code)
			{
				System.Diagnostics.Debug.Assert(code == FSEditLogOpCodes.OpStartLogSegment || code
					 == FSEditLogOpCodes.OpEndLogSegment, "Bad op: " + code);
			}

			internal static FSEditLogOp.LogSegmentOp GetInstance(FSEditLogOp.OpInstanceCache 
				cache, FSEditLogOpCodes code)
			{
				return (FSEditLogOp.LogSegmentOp)cache.Get(code);
			}

			internal override void ResetSubFields()
			{
			}

			// no data stored in these ops yet
			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
			}

			// no data stored in these ops yet
			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
			}

			// no data stored
			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("LogSegmentOp [opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
			}

			// no data stored
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
			}
			// do nothing
		}

		internal class InvalidOp : FSEditLogOp
		{
			private InvalidOp()
				: base(FSEditLogOpCodes.OpInvalid)
			{
			}

			internal static FSEditLogOp.InvalidOp GetInstance(FSEditLogOp.OpInstanceCache cache
				)
			{
				return (FSEditLogOp.InvalidOp)cache.Get(FSEditLogOpCodes.OpInvalid);
			}

			internal override void ResetSubFields()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
			}

			// nothing to read
			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("InvalidOp [opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
			}

			// no data stored
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
			}
			// do nothing
		}

		/// <summary>Operation corresponding to creating a snapshot.</summary>
		/// <remarks>
		/// Operation corresponding to creating a snapshot.
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.CreateSnapshot(string, string)
		/// 	"/>
		/// .
		/// </remarks>
		internal class CreateSnapshotOp : FSEditLogOp
		{
			internal string snapshotRoot;

			internal string snapshotName;

			public CreateSnapshotOp()
				: base(FSEditLogOpCodes.OpCreateSnapshot)
			{
			}

			internal static FSEditLogOp.CreateSnapshotOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.CreateSnapshotOp)cache.Get(FSEditLogOpCodes.OpCreateSnapshot);
			}

			internal override void ResetSubFields()
			{
				snapshotRoot = null;
				snapshotName = null;
			}

			internal virtual FSEditLogOp.CreateSnapshotOp SetSnapshotName(string snapName)
			{
				this.snapshotName = snapName;
				return this;
			}

			public virtual FSEditLogOp.CreateSnapshotOp SetSnapshotRoot(string snapRoot)
			{
				snapshotRoot = snapRoot;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				snapshotRoot = FSImageSerialization.ReadString(@in);
				snapshotName = FSImageSerialization.ReadString(@in);
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(snapshotRoot, @out);
				FSImageSerialization.WriteString(snapshotName, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTNAME", snapshotName);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				snapshotRoot = st.GetValue("SNAPSHOTROOT");
				snapshotName = st.GetValue("SNAPSHOTNAME");
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("CreateSnapshotOp [snapshotRoot=");
				builder.Append(snapshotRoot);
				builder.Append(", snapshotName=");
				builder.Append(snapshotName);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>Operation corresponding to delete a snapshot.</summary>
		/// <remarks>
		/// Operation corresponding to delete a snapshot.
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.DeleteSnapshot(string, string)
		/// 	"/>
		/// .
		/// </remarks>
		internal class DeleteSnapshotOp : FSEditLogOp
		{
			internal string snapshotRoot;

			internal string snapshotName;

			internal DeleteSnapshotOp()
				: base(FSEditLogOpCodes.OpDeleteSnapshot)
			{
			}

			internal static FSEditLogOp.DeleteSnapshotOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.DeleteSnapshotOp)cache.Get(FSEditLogOpCodes.OpDeleteSnapshot);
			}

			internal override void ResetSubFields()
			{
				snapshotRoot = null;
				snapshotName = null;
			}

			internal virtual FSEditLogOp.DeleteSnapshotOp SetSnapshotName(string snapName)
			{
				this.snapshotName = snapName;
				return this;
			}

			internal virtual FSEditLogOp.DeleteSnapshotOp SetSnapshotRoot(string snapRoot)
			{
				snapshotRoot = snapRoot;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				snapshotRoot = FSImageSerialization.ReadString(@in);
				snapshotName = FSImageSerialization.ReadString(@in);
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(snapshotRoot, @out);
				FSImageSerialization.WriteString(snapshotName, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTNAME", snapshotName);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				snapshotRoot = st.GetValue("SNAPSHOTROOT");
				snapshotName = st.GetValue("SNAPSHOTNAME");
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("DeleteSnapshotOp [snapshotRoot=");
				builder.Append(snapshotRoot);
				builder.Append(", snapshotName=");
				builder.Append(snapshotName);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>Operation corresponding to rename a snapshot.</summary>
		/// <remarks>
		/// Operation corresponding to rename a snapshot.
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RenameSnapshot(string, string, string)
		/// 	"/>
		/// .
		/// </remarks>
		internal class RenameSnapshotOp : FSEditLogOp
		{
			internal string snapshotRoot;

			internal string snapshotOldName;

			internal string snapshotNewName;

			internal RenameSnapshotOp()
				: base(FSEditLogOpCodes.OpRenameSnapshot)
			{
			}

			internal static FSEditLogOp.RenameSnapshotOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RenameSnapshotOp)cache.Get(FSEditLogOpCodes.OpRenameSnapshot);
			}

			internal override void ResetSubFields()
			{
				snapshotRoot = null;
				snapshotOldName = null;
				snapshotNewName = null;
			}

			internal virtual FSEditLogOp.RenameSnapshotOp SetSnapshotOldName(string snapshotOldName
				)
			{
				this.snapshotOldName = snapshotOldName;
				return this;
			}

			internal virtual FSEditLogOp.RenameSnapshotOp SetSnapshotNewName(string snapshotNewName
				)
			{
				this.snapshotNewName = snapshotNewName;
				return this;
			}

			internal virtual FSEditLogOp.RenameSnapshotOp SetSnapshotRoot(string snapshotRoot
				)
			{
				this.snapshotRoot = snapshotRoot;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				snapshotRoot = FSImageSerialization.ReadString(@in);
				snapshotOldName = FSImageSerialization.ReadString(@in);
				snapshotNewName = FSImageSerialization.ReadString(@in);
				// read RPC ids if necessary
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(snapshotRoot, @out);
				FSImageSerialization.WriteString(snapshotOldName, @out);
				FSImageSerialization.WriteString(snapshotNewName, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTOLDNAME", snapshotOldName);
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTNEWNAME", snapshotNewName);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				snapshotRoot = st.GetValue("SNAPSHOTROOT");
				snapshotOldName = st.GetValue("SNAPSHOTOLDNAME");
				snapshotNewName = st.GetValue("SNAPSHOTNEWNAME");
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RenameSnapshotOp [snapshotRoot=");
				builder.Append(snapshotRoot);
				builder.Append(", snapshotOldName=");
				builder.Append(snapshotOldName);
				builder.Append(", snapshotNewName=");
				builder.Append(snapshotNewName);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>Operation corresponding to allow creating snapshot on a directory</summary>
		internal class AllowSnapshotOp : FSEditLogOp
		{
			internal string snapshotRoot;

			public AllowSnapshotOp()
				: base(FSEditLogOpCodes.OpAllowSnapshot)
			{
			}

			public AllowSnapshotOp(string snapRoot)
				: base(FSEditLogOpCodes.OpAllowSnapshot)
			{
				// @Idempotent
				snapshotRoot = snapRoot;
			}

			internal static FSEditLogOp.AllowSnapshotOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.AllowSnapshotOp)cache.Get(FSEditLogOpCodes.OpAllowSnapshot);
			}

			internal override void ResetSubFields()
			{
				snapshotRoot = null;
			}

			public virtual FSEditLogOp.AllowSnapshotOp SetSnapshotRoot(string snapRoot)
			{
				snapshotRoot = snapRoot;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				snapshotRoot = FSImageSerialization.ReadString(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(snapshotRoot, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				snapshotRoot = st.GetValue("SNAPSHOTROOT");
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AllowSnapshotOp [snapshotRoot=");
				builder.Append(snapshotRoot);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>Operation corresponding to disallow creating snapshot on a directory</summary>
		internal class DisallowSnapshotOp : FSEditLogOp
		{
			internal string snapshotRoot;

			public DisallowSnapshotOp()
				: base(FSEditLogOpCodes.OpDisallowSnapshot)
			{
			}

			public DisallowSnapshotOp(string snapRoot)
				: base(FSEditLogOpCodes.OpDisallowSnapshot)
			{
				// @Idempotent
				snapshotRoot = snapRoot;
			}

			internal static FSEditLogOp.DisallowSnapshotOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.DisallowSnapshotOp)cache.Get(FSEditLogOpCodes.OpDisallowSnapshot
					);
			}

			internal override void ResetSubFields()
			{
				snapshotRoot = null;
			}

			public virtual FSEditLogOp.DisallowSnapshotOp SetSnapshotRoot(string snapRoot)
			{
				snapshotRoot = snapRoot;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				snapshotRoot = FSImageSerialization.ReadString(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(snapshotRoot, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SNAPSHOTROOT", snapshotRoot);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				snapshotRoot = st.GetValue("SNAPSHOTROOT");
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("DisallowSnapshotOp [snapshotRoot=");
				builder.Append(snapshotRoot);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddCacheDirective(Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo, Sharpen.EnumSet{E})
		/// 	"/>
		/// </summary>
		internal class AddCacheDirectiveInfoOp : FSEditLogOp
		{
			internal CacheDirectiveInfo directive;

			public AddCacheDirectiveInfoOp()
				: base(FSEditLogOpCodes.OpAddCacheDirective)
			{
			}

			internal static FSEditLogOp.AddCacheDirectiveInfoOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.AddCacheDirectiveInfoOp)cache.Get(FSEditLogOpCodes.OpAddCacheDirective
					);
			}

			internal override void ResetSubFields()
			{
				directive = null;
			}

			public virtual FSEditLogOp.AddCacheDirectiveInfoOp SetDirective(CacheDirectiveInfo
				 directive)
			{
				this.directive = directive;
				System.Diagnostics.Debug.Assert((directive.GetId() != null));
				System.Diagnostics.Debug.Assert((directive.GetPath() != null));
				System.Diagnostics.Debug.Assert((directive.GetReplication() != null));
				System.Diagnostics.Debug.Assert((directive.GetPool() != null));
				System.Diagnostics.Debug.Assert((directive.GetExpiration() != null));
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				directive = FSImageSerialization.ReadCacheDirectiveInfo(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteCacheDirectiveInfo(@out, directive);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSImageSerialization.WriteCacheDirectiveInfo(contentHandler, directive);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				directive = FSImageSerialization.ReadCacheDirectiveInfo(st);
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AddCacheDirectiveInfo [");
				builder.Append("id=" + directive.GetId() + ",");
				builder.Append("path=" + directive.GetPath().ToUri().GetPath() + ",");
				builder.Append("replication=" + directive.GetReplication() + ",");
				builder.Append("pool=" + directive.GetPool() + ",");
				builder.Append("expiration=" + directive.GetExpiration().GetMillis());
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.ModifyCacheDirective(Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo, Sharpen.EnumSet{E})
		/// 	"/>
		/// </summary>
		internal class ModifyCacheDirectiveInfoOp : FSEditLogOp
		{
			internal CacheDirectiveInfo directive;

			public ModifyCacheDirectiveInfoOp()
				: base(FSEditLogOpCodes.OpModifyCacheDirective)
			{
			}

			internal static FSEditLogOp.ModifyCacheDirectiveInfoOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.ModifyCacheDirectiveInfoOp)cache.Get(FSEditLogOpCodes.OpModifyCacheDirective
					);
			}

			internal override void ResetSubFields()
			{
				directive = null;
			}

			public virtual FSEditLogOp.ModifyCacheDirectiveInfoOp SetDirective(CacheDirectiveInfo
				 directive)
			{
				this.directive = directive;
				System.Diagnostics.Debug.Assert((directive.GetId() != null));
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.directive = FSImageSerialization.ReadCacheDirectiveInfo(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteCacheDirectiveInfo(@out, directive);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSImageSerialization.WriteCacheDirectiveInfo(contentHandler, directive);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.directive = FSImageSerialization.ReadCacheDirectiveInfo(st);
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("ModifyCacheDirectiveInfoOp[");
				builder.Append("id=").Append(directive.GetId());
				if (directive.GetPath() != null)
				{
					builder.Append(",").Append("path=").Append(directive.GetPath());
				}
				if (directive.GetReplication() != null)
				{
					builder.Append(",").Append("replication=").Append(directive.GetReplication());
				}
				if (directive.GetPool() != null)
				{
					builder.Append(",").Append("pool=").Append(directive.GetPool());
				}
				if (directive.GetExpiration() != null)
				{
					builder.Append(",").Append("expiration=").Append(directive.GetExpiration().GetMillis
						());
				}
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RemoveCacheDirective(long)
		/// 	"/>
		/// </summary>
		internal class RemoveCacheDirectiveInfoOp : FSEditLogOp
		{
			internal long id;

			public RemoveCacheDirectiveInfoOp()
				: base(FSEditLogOpCodes.OpRemoveCacheDirective)
			{
			}

			internal static FSEditLogOp.RemoveCacheDirectiveInfoOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RemoveCacheDirectiveInfoOp)cache.Get(FSEditLogOpCodes.OpRemoveCacheDirective
					);
			}

			internal override void ResetSubFields()
			{
				id = 0L;
			}

			public virtual FSEditLogOp.RemoveCacheDirectiveInfoOp SetId(long id)
			{
				this.id = id;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.id = FSImageSerialization.ReadLong(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(id, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "ID", System.Convert.ToString(id));
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.id = long.Parse(st.GetValue("ID"));
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RemoveCacheDirectiveInfo [");
				builder.Append("id=" + System.Convert.ToString(id));
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddCachePool(Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo)
		/// 	"/>
		/// 
		/// </summary>
		internal class AddCachePoolOp : FSEditLogOp
		{
			internal CachePoolInfo info;

			public AddCachePoolOp()
				: base(FSEditLogOpCodes.OpAddCachePool)
			{
			}

			internal static FSEditLogOp.AddCachePoolOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.AddCachePoolOp)cache.Get(FSEditLogOpCodes.OpAddCachePool);
			}

			internal override void ResetSubFields()
			{
				info = null;
			}

			public virtual FSEditLogOp.AddCachePoolOp SetPool(CachePoolInfo info)
			{
				this.info = info;
				System.Diagnostics.Debug.Assert((info.GetPoolName() != null));
				System.Diagnostics.Debug.Assert((info.GetOwnerName() != null));
				System.Diagnostics.Debug.Assert((info.GetGroupName() != null));
				System.Diagnostics.Debug.Assert((info.GetMode() != null));
				System.Diagnostics.Debug.Assert((info.GetLimit() != null));
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				info = FSImageSerialization.ReadCachePoolInfo(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteCachePoolInfo(@out, info);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSImageSerialization.WriteCachePoolInfo(contentHandler, info);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.info = FSImageSerialization.ReadCachePoolInfo(st);
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("AddCachePoolOp [");
				builder.Append("poolName=" + info.GetPoolName() + ",");
				builder.Append("ownerName=" + info.GetOwnerName() + ",");
				builder.Append("groupName=" + info.GetGroupName() + ",");
				builder.Append("mode=" + short.ToString(info.GetMode().ToShort()) + ",");
				builder.Append("limit=" + System.Convert.ToString(info.GetLimit()));
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.ModifyCachePool(Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo)
		/// 	"/>
		/// 
		/// </summary>
		internal class ModifyCachePoolOp : FSEditLogOp
		{
			internal CachePoolInfo info;

			public ModifyCachePoolOp()
				: base(FSEditLogOpCodes.OpModifyCachePool)
			{
			}

			internal static FSEditLogOp.ModifyCachePoolOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.ModifyCachePoolOp)cache.Get(FSEditLogOpCodes.OpModifyCachePool
					);
			}

			internal override void ResetSubFields()
			{
				info = null;
			}

			public virtual FSEditLogOp.ModifyCachePoolOp SetInfo(CachePoolInfo info)
			{
				this.info = info;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				info = FSImageSerialization.ReadCachePoolInfo(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteCachePoolInfo(@out, info);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				FSImageSerialization.WriteCachePoolInfo(contentHandler, info);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.info = FSImageSerialization.ReadCachePoolInfo(st);
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("ModifyCachePoolOp [");
				AList<string> fields = new AList<string>(5);
				if (info.GetPoolName() != null)
				{
					fields.AddItem("poolName=" + info.GetPoolName());
				}
				if (info.GetOwnerName() != null)
				{
					fields.AddItem("ownerName=" + info.GetOwnerName());
				}
				if (info.GetGroupName() != null)
				{
					fields.AddItem("groupName=" + info.GetGroupName());
				}
				if (info.GetMode() != null)
				{
					fields.AddItem("mode=" + info.GetMode().ToString());
				}
				if (info.GetLimit() != null)
				{
					fields.AddItem("limit=" + info.GetLimit());
				}
				builder.Append(Joiner.On(",").Join(fields));
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		/// <summary>
		/// <literal>@AtMostOnce</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.RemoveCachePool(string)
		/// 	"/>
		/// 
		/// </summary>
		internal class RemoveCachePoolOp : FSEditLogOp
		{
			internal string poolName;

			public RemoveCachePoolOp()
				: base(FSEditLogOpCodes.OpRemoveCachePool)
			{
			}

			internal static FSEditLogOp.RemoveCachePoolOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RemoveCachePoolOp)cache.Get(FSEditLogOpCodes.OpRemoveCachePool
					);
			}

			internal override void ResetSubFields()
			{
				poolName = null;
			}

			public virtual FSEditLogOp.RemoveCachePoolOp SetPoolName(string poolName)
			{
				this.poolName = poolName;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				poolName = FSImageSerialization.ReadString(@in);
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(poolName, @out);
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "POOLNAME", poolName);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.poolName = st.GetValue("POOLNAME");
				ReadRpcIdsFromXml(st);
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("RemoveCachePoolOp [");
				builder.Append("poolName=" + poolName);
				AppendRpcIdsToString(builder, rpcClientId, rpcCallId);
				builder.Append("]");
				return builder.ToString();
			}
		}

		internal class RemoveXAttrOp : FSEditLogOp
		{
			internal IList<XAttr> xAttrs;

			internal string src;

			private RemoveXAttrOp()
				: base(FSEditLogOpCodes.OpRemoveXattr)
			{
			}

			internal static FSEditLogOp.RemoveXAttrOp GetInstance()
			{
				return new FSEditLogOp.RemoveXAttrOp();
			}

			internal override void ResetSubFields()
			{
				xAttrs = null;
				src = null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				XAttrProtos.XAttrEditLogProto p = XAttrProtos.XAttrEditLogProto.ParseDelimitedFrom
					(@in);
				src = p.GetSrc();
				xAttrs = PBHelper.ConvertXAttrs(p.GetXAttrsList());
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				XAttrProtos.XAttrEditLogProto.Builder b = XAttrProtos.XAttrEditLogProto.NewBuilder
					();
				if (src != null)
				{
					b.SetSrc(src);
				}
				b.AddAllXAttrs(PBHelper.ConvertXAttrProto(xAttrs));
				((XAttrProtos.XAttrEditLogProto)b.Build()).WriteDelimitedTo(@out);
				// clientId and callId
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				AppendXAttrsToXml(contentHandler, xAttrs);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				src = st.GetValue("SRC");
				xAttrs = ReadXAttrsFromXml(st);
				ReadRpcIdsFromXml(st);
			}
		}

		internal class SetXAttrOp : FSEditLogOp
		{
			internal IList<XAttr> xAttrs;

			internal string src;

			private SetXAttrOp()
				: base(FSEditLogOpCodes.OpSetXattr)
			{
			}

			internal static FSEditLogOp.SetXAttrOp GetInstance()
			{
				return new FSEditLogOp.SetXAttrOp();
			}

			internal override void ResetSubFields()
			{
				xAttrs = null;
				src = null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				XAttrProtos.XAttrEditLogProto p = XAttrProtos.XAttrEditLogProto.ParseDelimitedFrom
					(@in);
				src = p.GetSrc();
				xAttrs = PBHelper.ConvertXAttrs(p.GetXAttrsList());
				ReadRpcIds(@in, logVersion);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				XAttrProtos.XAttrEditLogProto.Builder b = XAttrProtos.XAttrEditLogProto.NewBuilder
					();
				if (src != null)
				{
					b.SetSrc(src);
				}
				b.AddAllXAttrs(PBHelper.ConvertXAttrProto(xAttrs));
				((XAttrProtos.XAttrEditLogProto)b.Build()).WriteDelimitedTo(@out);
				// clientId and callId
				WriteRpcIds(rpcClientId, rpcCallId, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				AppendXAttrsToXml(contentHandler, xAttrs);
				AppendRpcIdsToXml(contentHandler, rpcClientId, rpcCallId);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				src = st.GetValue("SRC");
				xAttrs = ReadXAttrsFromXml(st);
				ReadRpcIdsFromXml(st);
			}
		}

		internal class SetAclOp : FSEditLogOp
		{
			internal IList<AclEntry> aclEntries = Lists.NewArrayList();

			internal string src;

			private SetAclOp()
				: base(FSEditLogOpCodes.OpSetAcl)
			{
			}

			internal static FSEditLogOp.SetAclOp GetInstance()
			{
				return new FSEditLogOp.SetAclOp();
			}

			internal override void ResetSubFields()
			{
				aclEntries = null;
				src = null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				AclProtos.AclEditLogProto p = AclProtos.AclEditLogProto.ParseDelimitedFrom(@in);
				if (p == null)
				{
					throw new IOException("Failed to read fields from SetAclOp");
				}
				src = p.GetSrc();
				aclEntries = PBHelper.ConvertAclEntry(p.GetEntriesList());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				AclProtos.AclEditLogProto.Builder b = AclProtos.AclEditLogProto.NewBuilder();
				if (src != null)
				{
					b.SetSrc(src);
				}
				b.AddAllEntries(PBHelper.ConvertAclEntryProto(aclEntries));
				((AclProtos.AclEditLogProto)b.Build()).WriteDelimitedTo(@out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "SRC", src);
				AppendAclEntriesToXml(contentHandler, aclEntries);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				src = st.GetValue("SRC");
				aclEntries = ReadAclEntriesFromXml(st);
				if (aclEntries == null)
				{
					aclEntries = Lists.NewArrayList();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static short ReadShort(DataInputStream @in)
		{
			return short.ParseShort(FSImageSerialization.ReadString(@in));
		}

		/// <exception cref="System.IO.IOException"/>
		private static long ReadLong(DataInputStream @in)
		{
			return long.Parse(FSImageSerialization.ReadString(@in));
		}

		/// <summary>A class to read in blocks stored in the old format.</summary>
		/// <remarks>
		/// A class to read in blocks stored in the old format. The only two
		/// fields in the block were blockid and length.
		/// </remarks>
		internal class BlockTwo : Writable
		{
			internal long blkid;

			internal long len;

			static BlockTwo()
			{
				// register a ctor
				WritableFactories.SetFactory(typeof(FSEditLogOp.BlockTwo), new _WritableFactory_4319
					());
			}

			private sealed class _WritableFactory_4319 : WritableFactory
			{
				public _WritableFactory_4319()
				{
				}

				public Writable NewInstance()
				{
					return new FSEditLogOp.BlockTwo();
				}
			}

			internal BlockTwo()
			{
				blkid = 0;
				len = 0;
			}

			/////////////////////////////////////
			// Writable
			/////////////////////////////////////
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(blkid);
				@out.WriteLong(len);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				this.blkid = @in.ReadLong();
				this.len = @in.ReadLong();
			}
		}

		/// <summary>Operation corresponding to upgrade</summary>
		internal class RollingUpgradeOp : FSEditLogOp
		{
			private readonly string name;

			private long time;

			public RollingUpgradeOp(FSEditLogOpCodes code, string name)
				: base(code)
			{
				// @Idempotent
				this.name = StringUtils.ToUpperCase(name);
			}

			internal static FSEditLogOp.RollingUpgradeOp GetStartInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RollingUpgradeOp)cache.Get(FSEditLogOpCodes.OpRollingUpgradeStart
					);
			}

			internal static FSEditLogOp.RollingUpgradeOp GetFinalizeInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.RollingUpgradeOp)cache.Get(FSEditLogOpCodes.OpRollingUpgradeFinalize
					);
			}

			internal override void ResetSubFields()
			{
				time = 0L;
			}

			internal virtual long GetTime()
			{
				return time;
			}

			internal virtual void SetTime(long time)
			{
				this.time = time;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				time = @in.ReadLong();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteLong(time, @out);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, name + "TIME", System.Convert.ToString(time
					));
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.time = long.Parse(st.GetValue(name + "TIME"));
			}

			public override string ToString()
			{
				return new StringBuilder().Append("RollingUpgradeOp [").Append(name).Append(", time="
					).Append(time).Append("]").ToString();
			}

			[System.Serializable]
			internal class RollbackException : IOException
			{
				private const long serialVersionUID = 1L;
			}
		}

		/// <summary>
		/// <literal>@Idempotent</literal>
		/// for
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetStoragePolicy(string, string)
		/// 	"/>
		/// 
		/// </summary>
		internal class SetStoragePolicyOp : FSEditLogOp
		{
			internal string path;

			internal byte policyId;

			private SetStoragePolicyOp()
				: base(FSEditLogOpCodes.OpSetStoragePolicy)
			{
			}

			internal static FSEditLogOp.SetStoragePolicyOp GetInstance(FSEditLogOp.OpInstanceCache
				 cache)
			{
				return (FSEditLogOp.SetStoragePolicyOp)cache.Get(FSEditLogOpCodes.OpSetStoragePolicy
					);
			}

			internal override void ResetSubFields()
			{
				path = null;
				policyId = 0;
			}

			internal virtual FSEditLogOp.SetStoragePolicyOp SetPath(string path)
			{
				this.path = path;
				return this;
			}

			internal virtual FSEditLogOp.SetStoragePolicyOp SetPolicyId(byte policyId)
			{
				this.policyId = policyId;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				FSImageSerialization.WriteString(path, @out);
				@out.WriteByte(policyId);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				this.path = FSImageSerialization.ReadString(@in);
				this.policyId = @in.ReadByte();
			}

			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("SetStoragePolicyOp [path=");
				builder.Append(path);
				builder.Append(", policyId=");
				builder.Append(policyId);
				builder.Append(", opCode=");
				builder.Append(opCode);
				builder.Append(", txid=");
				builder.Append(txid);
				builder.Append("]");
				return builder.ToString();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				XMLUtils.AddSaxString(contentHandler, "PATH", path);
				XMLUtils.AddSaxString(contentHandler, "POLICYID", byte.ValueOf(policyId).ToString
					());
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				this.path = st.GetValue("PATH");
				this.policyId = byte.ValueOf(st.GetValue("POLICYID"));
			}
		}

		/// <summary>Class for writing editlog ops</summary>
		public class Writer
		{
			private readonly DataOutputBuffer buf;

			private readonly Checksum checksum;

			public Writer(DataOutputBuffer @out)
			{
				this.buf = @out;
				this.checksum = DataChecksum.NewCrc32();
			}

			/// <summary>Write an operation to the output stream</summary>
			/// <param name="op">The operation to write</param>
			/// <exception cref="System.IO.IOException">if an error occurs during writing.</exception>
			public virtual void WriteOp(FSEditLogOp op)
			{
				int start = buf.GetLength();
				// write the op code first to make padding and terminator verification
				// work
				buf.WriteByte(op.opCode.GetOpCode());
				buf.WriteInt(0);
				// write 0 for the length first
				buf.WriteLong(op.txid);
				op.WriteFields(buf);
				int end = buf.GetLength();
				// write the length back: content of the op + 4 bytes checksum - op_code
				int length = end - start - 1;
				buf.WriteInt(length, start + 1);
				checksum.Reset();
				checksum.Update(buf.GetData(), start, end - start);
				int sum = (int)checksum.GetValue();
				buf.WriteInt(sum);
			}
		}

		/// <summary>Class for reading editlog ops from a stream</summary>
		public class Reader
		{
			private readonly DataInputStream @in;

			private readonly StreamLimiter limiter;

			private readonly int logVersion;

			private readonly Checksum checksum;

			private readonly FSEditLogOp.OpInstanceCache cache;

			private int maxOpSize;

			private readonly bool supportEditLogLength;

			/// <summary>Construct the reader</summary>
			/// <param name="in">The stream to read from.</param>
			/// <param name="logVersion">The version of the data coming from the stream.</param>
			public Reader(DataInputStream @in, StreamLimiter limiter, int logVersion)
			{
				this.logVersion = logVersion;
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.EditsCheskum, logVersion
					))
				{
					this.checksum = DataChecksum.NewCrc32();
				}
				else
				{
					this.checksum = null;
				}
				// It is possible that the logVersion is actually a future layoutversion
				// during the rolling upgrade (e.g., the NN gets upgraded first). We
				// assume future layout will also support length of editlog op.
				this.supportEditLogLength = NameNodeLayoutVersion.Supports(NameNodeLayoutVersion.Feature
					.EditlogLength, logVersion) || logVersion < NameNodeLayoutVersion.CurrentLayoutVersion;
				if (this.checksum != null)
				{
					this.@in = new DataInputStream(new CheckedInputStream(@in, this.checksum));
				}
				else
				{
					this.@in = @in;
				}
				this.limiter = limiter;
				this.cache = new FSEditLogOp.OpInstanceCache();
				this.maxOpSize = DFSConfigKeys.DfsNamenodeMaxOpSizeDefault;
			}

			public virtual void SetMaxOpSize(int maxOpSize)
			{
				this.maxOpSize = maxOpSize;
			}

			/// <summary>Read an operation from the input stream.</summary>
			/// <remarks>
			/// Read an operation from the input stream.
			/// Note that the objects returned from this method may be re-used by future
			/// calls to the same method.
			/// </remarks>
			/// <param name="skipBrokenEdits">
			/// If true, attempt to skip over damaged parts of
			/// the input stream, rather than throwing an IOException
			/// </param>
			/// <returns>
			/// the operation read from the stream, or null at the end of the
			/// file
			/// </returns>
			/// <exception cref="System.IO.IOException">
			/// on error.  This function should only throw an
			/// exception when skipBrokenEdits is false.
			/// </exception>
			public virtual FSEditLogOp ReadOp(bool skipBrokenEdits)
			{
				while (true)
				{
					try
					{
						return DecodeOp();
					}
					catch (IOException e)
					{
						@in.Reset();
						if (!skipBrokenEdits)
						{
							throw;
						}
					}
					catch (RuntimeException e)
					{
						// FSEditLogOp#decodeOp is not supposed to throw RuntimeException.
						// However, we handle it here for recovery mode, just to be more
						// robust.
						@in.Reset();
						if (!skipBrokenEdits)
						{
							throw;
						}
					}
					catch (Exception e)
					{
						@in.Reset();
						if (!skipBrokenEdits)
						{
							throw new IOException("got unexpected exception " + e.Message, e);
						}
					}
					// Move ahead one byte and re-try the decode process.
					if (@in.Skip(1) < 1)
					{
						return null;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void VerifyTerminator()
			{
				byte[] buf = new byte[4096];
				limiter.ClearLimit();
				int numRead = -1;
				int idx = 0;
				while (true)
				{
					try
					{
						numRead = -1;
						idx = 0;
						numRead = @in.Read(buf);
						if (numRead == -1)
						{
							return;
						}
						while (idx < numRead)
						{
							if ((buf[idx] != unchecked((byte)0)) && (buf[idx] != unchecked((byte)-1)))
							{
								throw new IOException("Read extra bytes after " + "the terminator!");
							}
							idx++;
						}
					}
					finally
					{
						// After reading each group of bytes, we reposition the mark one
						// byte before the next group.  Similarly, if there is an error, we
						// want to reposition the mark one byte before the error
						if (numRead != -1)
						{
							@in.Reset();
							IOUtils.SkipFully(@in, idx);
							@in.Mark(buf.Length + 1);
							IOUtils.SkipFully(@in, 1);
						}
					}
				}
			}

			/// <summary>Read an opcode from the input stream.</summary>
			/// <returns>
			/// the opcode, or null on EOF.
			/// If an exception is thrown, the stream's mark will be set to the first
			/// problematic byte.  This usually means the beginning of the opcode.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private FSEditLogOp DecodeOp()
			{
				limiter.SetLimit(maxOpSize);
				@in.Mark(maxOpSize);
				if (checksum != null)
				{
					checksum.Reset();
				}
				byte opCodeByte;
				try
				{
					opCodeByte = @in.ReadByte();
				}
				catch (EOFException)
				{
					// EOF at an opcode boundary is expected.
					return null;
				}
				FSEditLogOpCodes opCode = FSEditLogOpCodes.FromByte(opCodeByte);
				if (opCode == FSEditLogOpCodes.OpInvalid)
				{
					VerifyTerminator();
					return null;
				}
				FSEditLogOp op = cache.Get(opCode);
				if (op == null)
				{
					throw new IOException("Read invalid opcode " + opCode);
				}
				if (supportEditLogLength)
				{
					@in.ReadInt();
				}
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.StoredTxids, logVersion))
				{
					// Read the txid
					op.SetTransactionId(@in.ReadLong());
				}
				else
				{
					op.SetTransactionId(HdfsConstants.InvalidTxid);
				}
				op.ReadFields(@in, logVersion);
				ValidateChecksum(@in, checksum, op.txid);
				return op;
			}

			/// <summary>
			/// Similar with decodeOp(), but instead of doing the real decoding, we skip
			/// the content of the op if the length of the editlog is supported.
			/// </summary>
			/// <returns>the last txid of the segment, or INVALID_TXID on exception</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual long ScanOp()
			{
				if (supportEditLogLength)
				{
					limiter.SetLimit(maxOpSize);
					@in.Mark(maxOpSize);
					byte opCodeByte;
					try
					{
						opCodeByte = @in.ReadByte();
					}
					catch (EOFException)
					{
						// op code
						return HdfsConstants.InvalidTxid;
					}
					FSEditLogOpCodes opCode = FSEditLogOpCodes.FromByte(opCodeByte);
					if (opCode == FSEditLogOpCodes.OpInvalid)
					{
						VerifyTerminator();
						return HdfsConstants.InvalidTxid;
					}
					int length = @in.ReadInt();
					// read the length of the op
					long txid = @in.ReadLong();
					// read the txid
					// skip the remaining content
					IOUtils.SkipFully(@in, length - 8);
					// TODO: do we want to verify checksum for JN? For now we don't.
					return txid;
				}
				else
				{
					FSEditLogOp op = DecodeOp();
					return op == null ? HdfsConstants.InvalidTxid : op.GetTransactionId();
				}
			}

			/// <summary>Validate a transaction's checksum</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ValidateChecksum(DataInputStream @in, Checksum checksum, long txid)
			{
				if (checksum != null)
				{
					int calculatedChecksum = (int)checksum.GetValue();
					int readChecksum = @in.ReadInt();
					// read in checksum
					if (readChecksum != calculatedChecksum)
					{
						throw new ChecksumException("Transaction is corrupt. Calculated checksum is " + calculatedChecksum
							 + " but read checksum " + readChecksum, txid);
					}
				}
			}
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public virtual void OutputToXml(ContentHandler contentHandler)
		{
			contentHandler.StartElement(string.Empty, string.Empty, "RECORD", new AttributesImpl
				());
			XMLUtils.AddSaxString(contentHandler, "OPCODE", opCode.ToString());
			contentHandler.StartElement(string.Empty, string.Empty, "DATA", new AttributesImpl
				());
			XMLUtils.AddSaxString(contentHandler, "TXID", string.Empty + txid);
			ToXml(contentHandler);
			contentHandler.EndElement(string.Empty, string.Empty, "DATA");
			contentHandler.EndElement(string.Empty, string.Empty, "RECORD");
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal abstract void ToXml(ContentHandler contentHandler);

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		internal abstract void FromXml(XMLUtils.Stanza st);

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public virtual void DecodeXml(XMLUtils.Stanza st)
		{
			this.txid = long.Parse(st.GetValue("TXID"));
			FromXml(st);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void BlockToXml(ContentHandler contentHandler, Block block)
		{
			contentHandler.StartElement(string.Empty, string.Empty, "BLOCK", new AttributesImpl
				());
			XMLUtils.AddSaxString(contentHandler, "BLOCK_ID", System.Convert.ToString(block.GetBlockId
				()));
			XMLUtils.AddSaxString(contentHandler, "NUM_BYTES", System.Convert.ToString(block.
				GetNumBytes()));
			XMLUtils.AddSaxString(contentHandler, "GENSTAMP", System.Convert.ToString(block.GetGenerationStamp
				()));
			contentHandler.EndElement(string.Empty, string.Empty, "BLOCK");
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static Block BlockFromXml(XMLUtils.Stanza st)
		{
			long blockId = long.Parse(st.GetValue("BLOCK_ID"));
			long numBytes = long.Parse(st.GetValue("NUM_BYTES"));
			long generationStamp = long.Parse(st.GetValue("GENSTAMP"));
			return new Block(blockId, numBytes, generationStamp);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void DelegationTokenToXml(ContentHandler contentHandler, DelegationTokenIdentifier
			 token)
		{
			contentHandler.StartElement(string.Empty, string.Empty, "DELEGATION_TOKEN_IDENTIFIER"
				, new AttributesImpl());
			XMLUtils.AddSaxString(contentHandler, "KIND", token.GetKind().ToString());
			XMLUtils.AddSaxString(contentHandler, "SEQUENCE_NUMBER", Sharpen.Extensions.ToString
				(token.GetSequenceNumber()));
			XMLUtils.AddSaxString(contentHandler, "OWNER", token.GetOwner().ToString());
			XMLUtils.AddSaxString(contentHandler, "RENEWER", token.GetRenewer().ToString());
			XMLUtils.AddSaxString(contentHandler, "REALUSER", token.GetRealUser().ToString());
			XMLUtils.AddSaxString(contentHandler, "ISSUE_DATE", System.Convert.ToString(token
				.GetIssueDate()));
			XMLUtils.AddSaxString(contentHandler, "MAX_DATE", System.Convert.ToString(token.GetMaxDate
				()));
			XMLUtils.AddSaxString(contentHandler, "MASTER_KEY_ID", Sharpen.Extensions.ToString
				(token.GetMasterKeyId()));
			contentHandler.EndElement(string.Empty, string.Empty, "DELEGATION_TOKEN_IDENTIFIER"
				);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static DelegationTokenIdentifier DelegationTokenFromXml(XMLUtils.Stanza st
			)
		{
			string kind = st.GetValue("KIND");
			if (!kind.Equals(DelegationTokenIdentifier.HdfsDelegationKind.ToString()))
			{
				throw new XMLUtils.InvalidXmlException("can't understand " + "DelegationTokenIdentifier KIND "
					 + kind);
			}
			int seqNum = System.Convert.ToInt32(st.GetValue("SEQUENCE_NUMBER"));
			string owner = st.GetValue("OWNER");
			string renewer = st.GetValue("RENEWER");
			string realuser = st.GetValue("REALUSER");
			long issueDate = long.Parse(st.GetValue("ISSUE_DATE"));
			long maxDate = long.Parse(st.GetValue("MAX_DATE"));
			int masterKeyId = System.Convert.ToInt32(st.GetValue("MASTER_KEY_ID"));
			DelegationTokenIdentifier token = new DelegationTokenIdentifier(new Org.Apache.Hadoop.IO.Text
				(owner), new Org.Apache.Hadoop.IO.Text(renewer), new Org.Apache.Hadoop.IO.Text(realuser
				));
			token.SetSequenceNumber(seqNum);
			token.SetIssueDate(issueDate);
			token.SetMaxDate(maxDate);
			token.SetMasterKeyId(masterKeyId);
			return token;
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void DelegationKeyToXml(ContentHandler contentHandler, DelegationKey
			 key)
		{
			contentHandler.StartElement(string.Empty, string.Empty, "DELEGATION_KEY", new AttributesImpl
				());
			XMLUtils.AddSaxString(contentHandler, "KEY_ID", Sharpen.Extensions.ToString(key.GetKeyId
				()));
			XMLUtils.AddSaxString(contentHandler, "EXPIRY_DATE", System.Convert.ToString(key.
				GetExpiryDate()));
			if (key.GetEncodedKey() != null)
			{
				XMLUtils.AddSaxString(contentHandler, "KEY", Hex.EncodeHexString(key.GetEncodedKey
					()));
			}
			contentHandler.EndElement(string.Empty, string.Empty, "DELEGATION_KEY");
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static DelegationKey DelegationKeyFromXml(XMLUtils.Stanza st)
		{
			int keyId = System.Convert.ToInt32(st.GetValue("KEY_ID"));
			long expiryDate = long.Parse(st.GetValue("EXPIRY_DATE"));
			byte[] key = null;
			try
			{
				key = Hex.DecodeHex(st.GetValue("KEY").ToCharArray());
			}
			catch (DecoderException e)
			{
				throw new XMLUtils.InvalidXmlException(e.ToString());
			}
			catch (XMLUtils.InvalidXmlException)
			{
			}
			return new DelegationKey(keyId, expiryDate, key);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void PermissionStatusToXml(ContentHandler contentHandler, PermissionStatus
			 perm)
		{
			contentHandler.StartElement(string.Empty, string.Empty, "PERMISSION_STATUS", new 
				AttributesImpl());
			XMLUtils.AddSaxString(contentHandler, "USERNAME", perm.GetUserName());
			XMLUtils.AddSaxString(contentHandler, "GROUPNAME", perm.GetGroupName());
			FsPermissionToXml(contentHandler, perm.GetPermission());
			contentHandler.EndElement(string.Empty, string.Empty, "PERMISSION_STATUS");
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static PermissionStatus PermissionStatusFromXml(XMLUtils.Stanza st)
		{
			XMLUtils.Stanza status = st.GetChildren("PERMISSION_STATUS")[0];
			string username = status.GetValue("USERNAME");
			string groupname = status.GetValue("GROUPNAME");
			FsPermission mode = FsPermissionFromXml(status);
			return new PermissionStatus(username, groupname, mode);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void FsPermissionToXml(ContentHandler contentHandler, FsPermission 
			mode)
		{
			XMLUtils.AddSaxString(contentHandler, "MODE", short.ValueOf(mode.ToShort()).ToString
				());
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		public static FsPermission FsPermissionFromXml(XMLUtils.Stanza st)
		{
			short mode = short.ValueOf(st.GetValue("MODE"));
			return new FsPermission(mode);
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private static void FsActionToXml(ContentHandler contentHandler, FsAction v)
		{
			XMLUtils.AddSaxString(contentHandler, "PERM", v.Symbol);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		private static FsAction FsActionFromXml(XMLUtils.Stanza st)
		{
			FsAction v = FsactionSymbolMap[st.GetValue("PERM")];
			if (v == null)
			{
				throw new XMLUtils.InvalidXmlException("Invalid value for FsAction");
			}
			return v;
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private static void AppendAclEntriesToXml(ContentHandler contentHandler, IList<AclEntry
			> aclEntries)
		{
			foreach (AclEntry e in aclEntries)
			{
				contentHandler.StartElement(string.Empty, string.Empty, "ENTRY", new AttributesImpl
					());
				XMLUtils.AddSaxString(contentHandler, "SCOPE", e.GetScope().ToString());
				XMLUtils.AddSaxString(contentHandler, "TYPE", e.GetType().ToString());
				if (e.GetName() != null)
				{
					XMLUtils.AddSaxString(contentHandler, "NAME", e.GetName());
				}
				FsActionToXml(contentHandler, e.GetPermission());
				contentHandler.EndElement(string.Empty, string.Empty, "ENTRY");
			}
		}

		private static IList<AclEntry> ReadAclEntriesFromXml(XMLUtils.Stanza st)
		{
			IList<AclEntry> aclEntries = Lists.NewArrayList();
			if (!st.HasChildren("ENTRY"))
			{
				return null;
			}
			IList<XMLUtils.Stanza> stanzas = st.GetChildren("ENTRY");
			foreach (XMLUtils.Stanza s in stanzas)
			{
				AclEntry e = new AclEntry.Builder().SetScope(AclEntryScope.ValueOf(s.GetValue("SCOPE"
					))).SetType(AclEntryType.ValueOf(s.GetValue("TYPE"))).SetName(s.GetValueOrNull("NAME"
					)).SetPermission(FsActionFromXml(s)).Build();
				aclEntries.AddItem(e);
			}
			return aclEntries;
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private static void AppendXAttrsToXml(ContentHandler contentHandler, IList<XAttr>
			 xAttrs)
		{
			foreach (XAttr xAttr in xAttrs)
			{
				contentHandler.StartElement(string.Empty, string.Empty, "XATTR", new AttributesImpl
					());
				XMLUtils.AddSaxString(contentHandler, "NAMESPACE", xAttr.GetNameSpace().ToString(
					));
				XMLUtils.AddSaxString(contentHandler, "NAME", xAttr.GetName());
				if (xAttr.GetValue() != null)
				{
					try
					{
						XMLUtils.AddSaxString(contentHandler, "VALUE", XAttrCodec.EncodeValue(xAttr.GetValue
							(), XAttrCodec.Hex));
					}
					catch (IOException e)
					{
						throw new SAXException(e);
					}
				}
				contentHandler.EndElement(string.Empty, string.Empty, "XATTR");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
		private static IList<XAttr> ReadXAttrsFromXml(XMLUtils.Stanza st)
		{
			if (!st.HasChildren("XATTR"))
			{
				return null;
			}
			IList<XMLUtils.Stanza> stanzas = st.GetChildren("XATTR");
			IList<XAttr> xattrs = Lists.NewArrayListWithCapacity(stanzas.Count);
			foreach (XMLUtils.Stanza a in stanzas)
			{
				XAttr.Builder builder = new XAttr.Builder();
				builder.SetNameSpace(XAttr.NameSpace.ValueOf(a.GetValue("NAMESPACE"))).SetName(a.
					GetValue("NAME"));
				string v = a.GetValueOrNull("VALUE");
				if (v != null)
				{
					try
					{
						builder.SetValue(XAttrCodec.DecodeValue(v));
					}
					catch (IOException e)
					{
						throw new XMLUtils.InvalidXmlException(e.ToString());
					}
				}
				xattrs.AddItem(builder.Build());
			}
			return xattrs;
		}
	}
}
