using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Primitives;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Utilities for converting protobuf classes to and from implementation classes
	/// and other helper utilities to help in dealing with protobuf.
	/// </summary>
	/// <remarks>
	/// Utilities for converting protobuf classes to and from implementation classes
	/// and other helper utilities to help in dealing with protobuf.
	/// Note that when converting from an internal type to protobuf type, the
	/// converter never return null for protobuf type. The check for internal type
	/// being null must be done before calling the convert() method.
	/// </remarks>
	public class PBHelper
	{
		private static readonly DatanodeProtocolProtos.RegisterCommandProto RegCmdProto = 
			((DatanodeProtocolProtos.RegisterCommandProto)DatanodeProtocolProtos.RegisterCommandProto
			.NewBuilder().Build());

		private static readonly RegisterCommand RegCmd = new RegisterCommand();

		private static readonly AclEntryScope[] AclEntryScopeValues = AclEntryScope.Values
			();

		private static readonly AclEntryType[] AclEntryTypeValues = AclEntryType.Values();

		private static readonly FsAction[] FsactionValues = FsAction.Values();

		private static readonly XAttr.NameSpace[] XattrNamespaceValues = XAttr.NameSpace.
			Values();

		private PBHelper()
		{
		}

		public static ByteString GetByteString(byte[] bytes)
		{
			return ByteString.CopyFrom(bytes);
		}

		private static U CastEnum<T, U>(T from, U[] to)
			where T : Enum<T>
			where U : Enum<U>
		{
			return to[from.Ordinal()];
		}

		public static HdfsServerConstants.NamenodeRole Convert(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto
			 role)
		{
			switch (role)
			{
				case HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Namenode:
				{
					return HdfsServerConstants.NamenodeRole.Namenode;
				}

				case HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Backup:
				{
					return HdfsServerConstants.NamenodeRole.Backup;
				}

				case HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Checkpoint:
				{
					return HdfsServerConstants.NamenodeRole.Checkpoint;
				}
			}
			return null;
		}

		public static HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto Convert(HdfsServerConstants.NamenodeRole
			 role)
		{
			switch (role)
			{
				case HdfsServerConstants.NamenodeRole.Namenode:
				{
					return HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Namenode;
				}

				case HdfsServerConstants.NamenodeRole.Backup:
				{
					return HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Backup;
				}

				case HdfsServerConstants.NamenodeRole.Checkpoint:
				{
					return HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Checkpoint;
				}
			}
			return null;
		}

		public static BlockStoragePolicy[] ConvertStoragePolicies(IList<HdfsProtos.BlockStoragePolicyProto
			> policyProtos)
		{
			if (policyProtos == null || policyProtos.Count == 0)
			{
				return new BlockStoragePolicy[0];
			}
			BlockStoragePolicy[] policies = new BlockStoragePolicy[policyProtos.Count];
			int i = 0;
			foreach (HdfsProtos.BlockStoragePolicyProto proto in policyProtos)
			{
				policies[i++] = Convert(proto);
			}
			return policies;
		}

		public static BlockStoragePolicy Convert(HdfsProtos.BlockStoragePolicyProto proto
			)
		{
			IList<HdfsProtos.StorageTypeProto> cList = proto.GetCreationPolicy().GetStorageTypesList
				();
			StorageType[] creationTypes = ConvertStorageTypes(cList, cList.Count);
			IList<HdfsProtos.StorageTypeProto> cfList = proto.HasCreationFallbackPolicy() ? proto
				.GetCreationFallbackPolicy().GetStorageTypesList() : null;
			StorageType[] creationFallbackTypes = cfList == null ? StorageType.EmptyArray : ConvertStorageTypes
				(cfList, cfList.Count);
			IList<HdfsProtos.StorageTypeProto> rfList = proto.HasReplicationFallbackPolicy() ? 
				proto.GetReplicationFallbackPolicy().GetStorageTypesList() : null;
			StorageType[] replicationFallbackTypes = rfList == null ? StorageType.EmptyArray : 
				ConvertStorageTypes(rfList, rfList.Count);
			return new BlockStoragePolicy(unchecked((byte)proto.GetPolicyId()), proto.GetName
				(), creationTypes, creationFallbackTypes, replicationFallbackTypes);
		}

		public static HdfsProtos.BlockStoragePolicyProto Convert(BlockStoragePolicy policy
			)
		{
			HdfsProtos.BlockStoragePolicyProto.Builder builder = HdfsProtos.BlockStoragePolicyProto
				.NewBuilder().SetPolicyId(policy.GetId()).SetName(policy.GetName());
			// creation storage types
			HdfsProtos.StorageTypesProto creationProto = Convert(policy.GetStorageTypes());
			Preconditions.CheckArgument(creationProto != null);
			builder.SetCreationPolicy(creationProto);
			// creation fallback
			HdfsProtos.StorageTypesProto creationFallbackProto = Convert(policy.GetCreationFallbacks
				());
			if (creationFallbackProto != null)
			{
				builder.SetCreationFallbackPolicy(creationFallbackProto);
			}
			// replication fallback
			HdfsProtos.StorageTypesProto replicationFallbackProto = Convert(policy.GetReplicationFallbacks
				());
			if (replicationFallbackProto != null)
			{
				builder.SetReplicationFallbackPolicy(replicationFallbackProto);
			}
			return ((HdfsProtos.BlockStoragePolicyProto)builder.Build());
		}

		public static HdfsProtos.StorageTypesProto Convert(StorageType[] types)
		{
			if (types == null || types.Length == 0)
			{
				return null;
			}
			IList<HdfsProtos.StorageTypeProto> list = ConvertStorageTypes(types);
			return ((HdfsProtos.StorageTypesProto)HdfsProtos.StorageTypesProto.NewBuilder().AddAllStorageTypes
				(list).Build());
		}

		public static HdfsProtos.StorageInfoProto Convert(StorageInfo info)
		{
			return ((HdfsProtos.StorageInfoProto)HdfsProtos.StorageInfoProto.NewBuilder().SetClusterID
				(info.GetClusterID()).SetCTime(info.GetCTime()).SetLayoutVersion(info.GetLayoutVersion
				()).SetNamespceID(info.GetNamespaceID()).Build());
		}

		public static StorageInfo Convert(HdfsProtos.StorageInfoProto info, HdfsServerConstants.NodeType
			 type)
		{
			return new StorageInfo(info.GetLayoutVersion(), info.GetNamespceID(), info.GetClusterID
				(), info.GetCTime(), type);
		}

		public static HdfsProtos.NamenodeRegistrationProto Convert(NamenodeRegistration reg
			)
		{
			return ((HdfsProtos.NamenodeRegistrationProto)HdfsProtos.NamenodeRegistrationProto
				.NewBuilder().SetHttpAddress(reg.GetHttpAddress()).SetRole(Convert(reg.GetRole()
				)).SetRpcAddress(reg.GetAddress()).SetStorageInfo(Convert((StorageInfo)reg)).Build
				());
		}

		public static NamenodeRegistration Convert(HdfsProtos.NamenodeRegistrationProto reg
			)
		{
			StorageInfo si = Convert(reg.GetStorageInfo(), HdfsServerConstants.NodeType.NameNode
				);
			return new NamenodeRegistration(reg.GetRpcAddress(), reg.GetHttpAddress(), si, Convert
				(reg.GetRole()));
		}

		// DatanodeId
		public static DatanodeID Convert(HdfsProtos.DatanodeIDProto dn)
		{
			return new DatanodeID(dn.GetIpAddr(), dn.GetHostName(), dn.GetDatanodeUuid(), dn.
				GetXferPort(), dn.GetInfoPort(), dn.HasInfoSecurePort() ? dn.GetInfoSecurePort()
				 : 0, dn.GetIpcPort());
		}

		public static HdfsProtos.DatanodeIDProto Convert(DatanodeID dn)
		{
			// For wire compatibility with older versions we transmit the StorageID
			// which is the same as the DatanodeUuid. Since StorageID is a required
			// field we pass the empty string if the DatanodeUuid is not yet known.
			return ((HdfsProtos.DatanodeIDProto)HdfsProtos.DatanodeIDProto.NewBuilder().SetIpAddr
				(dn.GetIpAddr()).SetHostName(dn.GetHostName()).SetXferPort(dn.GetXferPort()).SetDatanodeUuid
				(dn.GetDatanodeUuid() != null ? dn.GetDatanodeUuid() : string.Empty).SetInfoPort
				(dn.GetInfoPort()).SetInfoSecurePort(dn.GetInfoSecurePort()).SetIpcPort(dn.GetIpcPort
				()).Build());
		}

		// Arrays of DatanodeId
		public static HdfsProtos.DatanodeIDProto[] Convert(DatanodeID[] did)
		{
			if (did == null)
			{
				return null;
			}
			int len = did.Length;
			HdfsProtos.DatanodeIDProto[] result = new HdfsProtos.DatanodeIDProto[len];
			for (int i = 0; i < len; ++i)
			{
				result[i] = Convert(did[i]);
			}
			return result;
		}

		public static DatanodeID[] Convert(HdfsProtos.DatanodeIDProto[] did)
		{
			if (did == null)
			{
				return null;
			}
			int len = did.Length;
			DatanodeID[] result = new DatanodeID[len];
			for (int i = 0; i < len; ++i)
			{
				result[i] = Convert(did[i]);
			}
			return result;
		}

		// Block
		public static HdfsProtos.BlockProto Convert(Block b)
		{
			return ((HdfsProtos.BlockProto)HdfsProtos.BlockProto.NewBuilder().SetBlockId(b.GetBlockId
				()).SetGenStamp(b.GetGenerationStamp()).SetNumBytes(b.GetNumBytes()).Build());
		}

		public static Block Convert(HdfsProtos.BlockProto b)
		{
			return new Block(b.GetBlockId(), b.GetNumBytes(), b.GetGenStamp());
		}

		public static HdfsProtos.BlockWithLocationsProto Convert(BlocksWithLocations.BlockWithLocations
			 blk)
		{
			return ((HdfsProtos.BlockWithLocationsProto)HdfsProtos.BlockWithLocationsProto.NewBuilder
				().SetBlock(Convert(blk.GetBlock())).AddAllDatanodeUuids(Arrays.AsList(blk.GetDatanodeUuids
				())).AddAllStorageUuids(Arrays.AsList(blk.GetStorageIDs())).AddAllStorageTypes(ConvertStorageTypes
				(blk.GetStorageTypes())).Build());
		}

		public static BlocksWithLocations.BlockWithLocations Convert(HdfsProtos.BlockWithLocationsProto
			 b)
		{
			IList<string> datanodeUuids = b.GetDatanodeUuidsList();
			IList<string> storageUuids = b.GetStorageUuidsList();
			IList<HdfsProtos.StorageTypeProto> storageTypes = b.GetStorageTypesList();
			return new BlocksWithLocations.BlockWithLocations(Convert(b.GetBlock()), Sharpen.Collections.ToArray
				(datanodeUuids, new string[datanodeUuids.Count]), Sharpen.Collections.ToArray(storageUuids
				, new string[storageUuids.Count]), ConvertStorageTypes(storageTypes, storageUuids
				.Count));
		}

		public static HdfsProtos.BlocksWithLocationsProto Convert(BlocksWithLocations blks
			)
		{
			HdfsProtos.BlocksWithLocationsProto.Builder builder = HdfsProtos.BlocksWithLocationsProto
				.NewBuilder();
			foreach (BlocksWithLocations.BlockWithLocations b in blks.GetBlocks())
			{
				builder.AddBlocks(Convert(b));
			}
			return ((HdfsProtos.BlocksWithLocationsProto)builder.Build());
		}

		public static BlocksWithLocations Convert(HdfsProtos.BlocksWithLocationsProto blocks
			)
		{
			IList<HdfsProtos.BlockWithLocationsProto> b = blocks.GetBlocksList();
			BlocksWithLocations.BlockWithLocations[] ret = new BlocksWithLocations.BlockWithLocations
				[b.Count];
			int i = 0;
			foreach (HdfsProtos.BlockWithLocationsProto entry in b)
			{
				ret[i++] = Convert(entry);
			}
			return new BlocksWithLocations(ret);
		}

		public static HdfsProtos.BlockKeyProto Convert(BlockKey key)
		{
			byte[] encodedKey = key.GetEncodedKey();
			ByteString keyBytes = ByteString.CopyFrom(encodedKey == null ? DFSUtil.EmptyBytes
				 : encodedKey);
			return ((HdfsProtos.BlockKeyProto)HdfsProtos.BlockKeyProto.NewBuilder().SetKeyId(
				key.GetKeyId()).SetKeyBytes(keyBytes).SetExpiryDate(key.GetExpiryDate()).Build()
				);
		}

		public static BlockKey Convert(HdfsProtos.BlockKeyProto k)
		{
			return new BlockKey(k.GetKeyId(), k.GetExpiryDate(), k.GetKeyBytes().ToByteArray(
				));
		}

		public static HdfsProtos.ExportedBlockKeysProto Convert(ExportedBlockKeys keys)
		{
			HdfsProtos.ExportedBlockKeysProto.Builder builder = HdfsProtos.ExportedBlockKeysProto
				.NewBuilder();
			builder.SetIsBlockTokenEnabled(keys.IsBlockTokenEnabled()).SetKeyUpdateInterval(keys
				.GetKeyUpdateInterval()).SetTokenLifeTime(keys.GetTokenLifetime()).SetCurrentKey
				(Convert(keys.GetCurrentKey()));
			foreach (BlockKey k in keys.GetAllKeys())
			{
				builder.AddAllKeys(Convert(k));
			}
			return ((HdfsProtos.ExportedBlockKeysProto)builder.Build());
		}

		public static ExportedBlockKeys Convert(HdfsProtos.ExportedBlockKeysProto keys)
		{
			return new ExportedBlockKeys(keys.GetIsBlockTokenEnabled(), keys.GetKeyUpdateInterval
				(), keys.GetTokenLifeTime(), Convert(keys.GetCurrentKey()), ConvertBlockKeys(keys
				.GetAllKeysList()));
		}

		public static HdfsProtos.CheckpointSignatureProto Convert(CheckpointSignature s)
		{
			return ((HdfsProtos.CheckpointSignatureProto)HdfsProtos.CheckpointSignatureProto.
				NewBuilder().SetBlockPoolId(s.GetBlockpoolID()).SetCurSegmentTxId(s.GetCurSegmentTxId
				()).SetMostRecentCheckpointTxId(s.GetMostRecentCheckpointTxId()).SetStorageInfo(
				Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((StorageInfo)s)).Build());
		}

		public static CheckpointSignature Convert(HdfsProtos.CheckpointSignatureProto s)
		{
			StorageInfo si = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(s.GetStorageInfo
				(), HdfsServerConstants.NodeType.NameNode);
			return new CheckpointSignature(si, s.GetBlockPoolId(), s.GetMostRecentCheckpointTxId
				(), s.GetCurSegmentTxId());
		}

		public static HdfsProtos.RemoteEditLogProto Convert(RemoteEditLog log)
		{
			return ((HdfsProtos.RemoteEditLogProto)HdfsProtos.RemoteEditLogProto.NewBuilder()
				.SetStartTxId(log.GetStartTxId()).SetEndTxId(log.GetEndTxId()).SetIsInProgress(log
				.IsInProgress()).Build());
		}

		public static RemoteEditLog Convert(HdfsProtos.RemoteEditLogProto l)
		{
			return new RemoteEditLog(l.GetStartTxId(), l.GetEndTxId(), l.GetIsInProgress());
		}

		public static HdfsProtos.RemoteEditLogManifestProto Convert(RemoteEditLogManifest
			 manifest)
		{
			HdfsProtos.RemoteEditLogManifestProto.Builder builder = HdfsProtos.RemoteEditLogManifestProto
				.NewBuilder();
			foreach (RemoteEditLog log in manifest.GetLogs())
			{
				builder.AddLogs(Convert(log));
			}
			return ((HdfsProtos.RemoteEditLogManifestProto)builder.Build());
		}

		public static RemoteEditLogManifest Convert(HdfsProtos.RemoteEditLogManifestProto
			 manifest)
		{
			IList<RemoteEditLog> logs = new AList<RemoteEditLog>(manifest.GetLogsList().Count
				);
			foreach (HdfsProtos.RemoteEditLogProto l in manifest.GetLogsList())
			{
				logs.AddItem(Convert(l));
			}
			return new RemoteEditLogManifest(logs);
		}

		public static HdfsProtos.CheckpointCommandProto Convert(CheckpointCommand cmd)
		{
			return ((HdfsProtos.CheckpointCommandProto)HdfsProtos.CheckpointCommandProto.NewBuilder
				().SetSignature(Convert(cmd.GetSignature())).SetNeedToReturnImage(cmd.NeedToReturnImage
				()).Build());
		}

		public static HdfsProtos.NamenodeCommandProto Convert(NamenodeCommand cmd)
		{
			if (cmd is CheckpointCommand)
			{
				return ((HdfsProtos.NamenodeCommandProto)HdfsProtos.NamenodeCommandProto.NewBuilder
					().SetAction(cmd.GetAction()).SetType(HdfsProtos.NamenodeCommandProto.Type.CheckPointCommand
					).SetCheckpointCmd(Convert((CheckpointCommand)cmd)).Build());
			}
			return ((HdfsProtos.NamenodeCommandProto)HdfsProtos.NamenodeCommandProto.NewBuilder
				().SetType(HdfsProtos.NamenodeCommandProto.Type.NamenodeCommand).SetAction(cmd.GetAction
				()).Build());
		}

		public static BlockKey[] ConvertBlockKeys(IList<HdfsProtos.BlockKeyProto> list)
		{
			BlockKey[] ret = new BlockKey[list.Count];
			int i = 0;
			foreach (HdfsProtos.BlockKeyProto k in list)
			{
				ret[i++] = Convert(k);
			}
			return ret;
		}

		public static NamespaceInfo Convert(HdfsProtos.NamespaceInfoProto info)
		{
			HdfsProtos.StorageInfoProto storage = info.GetStorageInfo();
			return new NamespaceInfo(storage.GetNamespceID(), storage.GetClusterID(), info.GetBlockPoolID
				(), storage.GetCTime(), info.GetBuildVersion(), info.GetSoftwareVersion(), info.
				GetCapabilities());
		}

		public static NamenodeCommand Convert(HdfsProtos.NamenodeCommandProto cmd)
		{
			if (cmd == null)
			{
				return null;
			}
			switch (cmd.GetType())
			{
				case HdfsProtos.NamenodeCommandProto.Type.CheckPointCommand:
				{
					HdfsProtos.CheckpointCommandProto chkPt = cmd.GetCheckpointCmd();
					return new CheckpointCommand(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(chkPt
						.GetSignature()), chkPt.GetNeedToReturnImage());
				}

				default:
				{
					return new NamenodeCommand(cmd.GetAction());
				}
			}
		}

		public static ExtendedBlock Convert(HdfsProtos.ExtendedBlockProto eb)
		{
			if (eb == null)
			{
				return null;
			}
			return new ExtendedBlock(eb.GetPoolId(), eb.GetBlockId(), eb.GetNumBytes(), eb.GetGenerationStamp
				());
		}

		public static HdfsProtos.ExtendedBlockProto Convert(ExtendedBlock b)
		{
			if (b == null)
			{
				return null;
			}
			return ((HdfsProtos.ExtendedBlockProto)HdfsProtos.ExtendedBlockProto.NewBuilder()
				.SetPoolId(b.GetBlockPoolId()).SetBlockId(b.GetBlockId()).SetNumBytes(b.GetNumBytes
				()).SetGenerationStamp(b.GetGenerationStamp()).Build());
		}

		public static HdfsProtos.RecoveringBlockProto Convert(BlockRecoveryCommand.RecoveringBlock
			 b)
		{
			if (b == null)
			{
				return null;
			}
			HdfsProtos.LocatedBlockProto lb = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert
				((LocatedBlock)b);
			HdfsProtos.RecoveringBlockProto.Builder builder = HdfsProtos.RecoveringBlockProto
				.NewBuilder();
			builder.SetBlock(lb).SetNewGenStamp(b.GetNewGenerationStamp());
			if (b.GetNewBlock() != null)
			{
				builder.SetTruncateBlock(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(b.GetNewBlock
					()));
			}
			return ((HdfsProtos.RecoveringBlockProto)builder.Build());
		}

		public static BlockRecoveryCommand.RecoveringBlock Convert(HdfsProtos.RecoveringBlockProto
			 b)
		{
			ExtendedBlock block = Convert(b.GetBlock().GetB());
			DatanodeInfo[] locs = Convert(b.GetBlock().GetLocsList());
			return (b.HasTruncateBlock()) ? new BlockRecoveryCommand.RecoveringBlock(block, locs
				, Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(b.GetTruncateBlock())) : new 
				BlockRecoveryCommand.RecoveringBlock(block, locs, b.GetNewGenStamp());
		}

		public static HdfsProtos.DatanodeInfoProto.AdminState Convert(DatanodeInfo.AdminStates
			 inAs)
		{
			switch (inAs)
			{
				case DatanodeInfo.AdminStates.Normal:
				{
					return HdfsProtos.DatanodeInfoProto.AdminState.Normal;
				}

				case DatanodeInfo.AdminStates.DecommissionInprogress:
				{
					return HdfsProtos.DatanodeInfoProto.AdminState.DecommissionInprogress;
				}

				case DatanodeInfo.AdminStates.Decommissioned:
				{
					return HdfsProtos.DatanodeInfoProto.AdminState.Decommissioned;
				}

				default:
				{
					return HdfsProtos.DatanodeInfoProto.AdminState.Normal;
				}
			}
		}

		public static DatanodeInfo Convert(HdfsProtos.DatanodeInfoProto di)
		{
			if (di == null)
			{
				return null;
			}
			return new DatanodeInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(di.GetId
				()), di.HasLocation() ? di.GetLocation() : null, di.GetCapacity(), di.GetDfsUsed
				(), di.GetRemaining(), di.GetBlockPoolUsed(), di.GetCacheCapacity(), di.GetCacheUsed
				(), di.GetLastUpdate(), di.GetLastUpdateMonotonic(), di.GetXceiverCount(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(di.GetAdminState()));
		}

		public static HdfsProtos.DatanodeInfoProto ConvertDatanodeInfo(DatanodeInfo di)
		{
			if (di == null)
			{
				return null;
			}
			return Convert(di);
		}

		public static DatanodeInfo[] Convert(HdfsProtos.DatanodeInfoProto[] di)
		{
			if (di == null)
			{
				return null;
			}
			DatanodeInfo[] result = new DatanodeInfo[di.Length];
			for (int i = 0; i < di.Length; i++)
			{
				result[i] = Convert(di[i]);
			}
			return result;
		}

		public static IList<HdfsProtos.DatanodeInfoProto> Convert(DatanodeInfo[] dnInfos)
		{
			return Convert(dnInfos, 0);
		}

		/// <summary>
		/// Copy from
		/// <paramref name="dnInfos"/>
		/// to a target of list of same size starting at
		/// <paramref name="startIdx"/>
		/// .
		/// </summary>
		public static IList<HdfsProtos.DatanodeInfoProto> Convert(DatanodeInfo[] dnInfos, 
			int startIdx)
		{
			if (dnInfos == null)
			{
				return null;
			}
			AList<HdfsProtos.DatanodeInfoProto> protos = Lists.NewArrayListWithCapacity(dnInfos
				.Length);
			for (int i = startIdx; i < dnInfos.Length; i++)
			{
				protos.AddItem(Convert(dnInfos[i]));
			}
			return protos;
		}

		public static DatanodeInfo[] Convert(IList<HdfsProtos.DatanodeInfoProto> list)
		{
			DatanodeInfo[] info = new DatanodeInfo[list.Count];
			for (int i = 0; i < info.Length; i++)
			{
				info[i] = Convert(list[i]);
			}
			return info;
		}

		public static HdfsProtos.DatanodeInfoProto Convert(DatanodeInfo info)
		{
			HdfsProtos.DatanodeInfoProto.Builder builder = HdfsProtos.DatanodeInfoProto.NewBuilder
				();
			if (info.GetNetworkLocation() != null)
			{
				builder.SetLocation(info.GetNetworkLocation());
			}
			builder.SetId(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((DatanodeID)info
				)).SetCapacity(info.GetCapacity()).SetDfsUsed(info.GetDfsUsed()).SetRemaining(info
				.GetRemaining()).SetBlockPoolUsed(info.GetBlockPoolUsed()).SetCacheCapacity(info
				.GetCacheCapacity()).SetCacheUsed(info.GetCacheUsed()).SetLastUpdate(info.GetLastUpdate
				()).SetLastUpdateMonotonic(info.GetLastUpdateMonotonic()).SetXceiverCount(info.GetXceiverCount
				()).SetAdminState(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(info.GetAdminState
				())).Build();
			return ((HdfsProtos.DatanodeInfoProto)builder.Build());
		}

		public static ClientNamenodeProtocolProtos.DatanodeStorageReportProto ConvertDatanodeStorageReport
			(DatanodeStorageReport report)
		{
			return ((ClientNamenodeProtocolProtos.DatanodeStorageReportProto)ClientNamenodeProtocolProtos.DatanodeStorageReportProto
				.NewBuilder().SetDatanodeInfo(Convert(report.GetDatanodeInfo())).AddAllStorageReports
				(ConvertStorageReports(report.GetStorageReports())).Build());
		}

		public static IList<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> ConvertDatanodeStorageReports
			(DatanodeStorageReport[] reports)
		{
			IList<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> protos = new AList
				<ClientNamenodeProtocolProtos.DatanodeStorageReportProto>(reports.Length);
			for (int i = 0; i < reports.Length; i++)
			{
				protos.AddItem(ConvertDatanodeStorageReport(reports[i]));
			}
			return protos;
		}

		public static DatanodeStorageReport ConvertDatanodeStorageReport(ClientNamenodeProtocolProtos.DatanodeStorageReportProto
			 proto)
		{
			return new DatanodeStorageReport(Convert(proto.GetDatanodeInfo()), ConvertStorageReports
				(proto.GetStorageReportsList()));
		}

		public static DatanodeStorageReport[] ConvertDatanodeStorageReports(IList<ClientNamenodeProtocolProtos.DatanodeStorageReportProto
			> protos)
		{
			DatanodeStorageReport[] reports = new DatanodeStorageReport[protos.Count];
			for (int i = 0; i < reports.Length; i++)
			{
				reports[i] = ConvertDatanodeStorageReport(protos[i]);
			}
			return reports;
		}

		public static DatanodeInfo.AdminStates Convert(HdfsProtos.DatanodeInfoProto.AdminState
			 adminState)
		{
			switch (adminState)
			{
				case HdfsProtos.DatanodeInfoProto.AdminState.DecommissionInprogress:
				{
					return DatanodeInfo.AdminStates.DecommissionInprogress;
				}

				case HdfsProtos.DatanodeInfoProto.AdminState.Decommissioned:
				{
					return DatanodeInfo.AdminStates.Decommissioned;
				}

				case HdfsProtos.DatanodeInfoProto.AdminState.Normal:
				default:
				{
					return DatanodeInfo.AdminStates.Normal;
				}
			}
		}

		public static HdfsProtos.LocatedBlockProto Convert(LocatedBlock b)
		{
			if (b == null)
			{
				return null;
			}
			HdfsProtos.LocatedBlockProto.Builder builder = HdfsProtos.LocatedBlockProto.NewBuilder
				();
			DatanodeInfo[] locs = b.GetLocations();
			IList<DatanodeInfo> cachedLocs = Lists.NewLinkedList(Arrays.AsList(b.GetCachedLocations
				()));
			for (int i = 0; i < locs.Length; i++)
			{
				DatanodeInfo loc = locs[i];
				builder.AddLocs(i, Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(loc));
				bool locIsCached = cachedLocs.Contains(loc);
				builder.AddIsCached(locIsCached);
				if (locIsCached)
				{
					cachedLocs.Remove(loc);
				}
			}
			Preconditions.CheckArgument(cachedLocs.Count == 0, "Found additional cached replica locations that are not in the set of"
				 + " storage-backed locations!");
			StorageType[] storageTypes = b.GetStorageTypes();
			if (storageTypes != null)
			{
				for (int i_1 = 0; i_1 < storageTypes.Length; ++i_1)
				{
					builder.AddStorageTypes(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.ConvertStorageType
						(storageTypes[i_1]));
				}
			}
			string[] storageIDs = b.GetStorageIDs();
			if (storageIDs != null)
			{
				builder.AddAllStorageIDs(Arrays.AsList(storageIDs));
			}
			return ((HdfsProtos.LocatedBlockProto)builder.SetB(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(b.GetBlock())).SetBlockToken(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(b.GetBlockToken())).SetCorrupt(b.IsCorrupt()).SetOffset(b.GetStartOffset
				()).Build());
		}

		public static LocatedBlock Convert(HdfsProtos.LocatedBlockProto proto)
		{
			if (proto == null)
			{
				return null;
			}
			IList<HdfsProtos.DatanodeInfoProto> locs = proto.GetLocsList();
			DatanodeInfo[] targets = new DatanodeInfo[locs.Count];
			for (int i = 0; i < locs.Count; i++)
			{
				targets[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(locs[i]);
			}
			StorageType[] storageTypes = ConvertStorageTypes(proto.GetStorageTypesList(), locs
				.Count);
			int storageIDsCount = proto.GetStorageIDsCount();
			string[] storageIDs;
			if (storageIDsCount == 0)
			{
				storageIDs = null;
			}
			else
			{
				Preconditions.CheckState(storageIDsCount == locs.Count);
				storageIDs = Sharpen.Collections.ToArray(proto.GetStorageIDsList(), new string[storageIDsCount
					]);
			}
			// Set values from the isCached list, re-using references from loc
			IList<DatanodeInfo> cachedLocs = new AList<DatanodeInfo>(locs.Count);
			IList<bool> isCachedList = proto.GetIsCachedList();
			for (int i_1 = 0; i_1 < isCachedList.Count; i_1++)
			{
				if (isCachedList[i_1])
				{
					cachedLocs.AddItem(targets[i_1]);
				}
			}
			LocatedBlock lb = new LocatedBlock(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert
				(proto.GetB()), targets, storageIDs, storageTypes, proto.GetOffset(), proto.GetCorrupt
				(), Sharpen.Collections.ToArray(cachedLocs, new DatanodeInfo[0]));
			lb.SetBlockToken(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetBlockToken
				()));
			return lb;
		}

		public static SecurityProtos.TokenProto Convert<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> tok)
			where _T0 : TokenIdentifier
		{
			return ((SecurityProtos.TokenProto)SecurityProtos.TokenProto.NewBuilder().SetIdentifier
				(ByteString.CopyFrom(tok.GetIdentifier())).SetPassword(ByteString.CopyFrom(tok.GetPassword
				())).SetKind(tok.GetKind().ToString()).SetService(tok.GetService().ToString()).Build
				());
		}

		public static Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> Convert
			(SecurityProtos.TokenProto blockToken)
		{
			return new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>(blockToken
				.GetIdentifier().ToByteArray(), blockToken.GetPassword().ToByteArray(), new Text
				(blockToken.GetKind()), new Text(blockToken.GetService()));
		}

		public static Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> ConvertDelegationToken
			(SecurityProtos.TokenProto blockToken)
		{
			return new Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>(blockToken
				.GetIdentifier().ToByteArray(), blockToken.GetPassword().ToByteArray(), new Text
				(blockToken.GetKind()), new Text(blockToken.GetService()));
		}

		public static HdfsServerConstants.ReplicaState Convert(HdfsProtos.ReplicaStateProto
			 state)
		{
			switch (state)
			{
				case HdfsProtos.ReplicaStateProto.Rbw:
				{
					return HdfsServerConstants.ReplicaState.Rbw;
				}

				case HdfsProtos.ReplicaStateProto.Rur:
				{
					return HdfsServerConstants.ReplicaState.Rur;
				}

				case HdfsProtos.ReplicaStateProto.Rwr:
				{
					return HdfsServerConstants.ReplicaState.Rwr;
				}

				case HdfsProtos.ReplicaStateProto.Temporary:
				{
					return HdfsServerConstants.ReplicaState.Temporary;
				}

				case HdfsProtos.ReplicaStateProto.Finalized:
				default:
				{
					return HdfsServerConstants.ReplicaState.Finalized;
				}
			}
		}

		public static HdfsProtos.ReplicaStateProto Convert(HdfsServerConstants.ReplicaState
			 state)
		{
			switch (state)
			{
				case HdfsServerConstants.ReplicaState.Rbw:
				{
					return HdfsProtos.ReplicaStateProto.Rbw;
				}

				case HdfsServerConstants.ReplicaState.Rur:
				{
					return HdfsProtos.ReplicaStateProto.Rur;
				}

				case HdfsServerConstants.ReplicaState.Rwr:
				{
					return HdfsProtos.ReplicaStateProto.Rwr;
				}

				case HdfsServerConstants.ReplicaState.Temporary:
				{
					return HdfsProtos.ReplicaStateProto.Temporary;
				}

				case HdfsServerConstants.ReplicaState.Finalized:
				default:
				{
					return HdfsProtos.ReplicaStateProto.Finalized;
				}
			}
		}

		public static DatanodeProtocolProtos.DatanodeRegistrationProto Convert(DatanodeRegistration
			 registration)
		{
			DatanodeProtocolProtos.DatanodeRegistrationProto.Builder builder = DatanodeProtocolProtos.DatanodeRegistrationProto
				.NewBuilder();
			return ((DatanodeProtocolProtos.DatanodeRegistrationProto)builder.SetDatanodeID(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert((DatanodeID)registration)).SetStorageInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(registration.GetStorageInfo())).SetKeys(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(registration.GetExportedKeys())).SetSoftwareVersion(registration.GetSoftwareVersion
				()).Build());
		}

		public static DatanodeRegistration Convert(DatanodeProtocolProtos.DatanodeRegistrationProto
			 proto)
		{
			StorageInfo si = Convert(proto.GetStorageInfo(), HdfsServerConstants.NodeType.DataNode
				);
			return new DatanodeRegistration(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert
				(proto.GetDatanodeID()), si, Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(
				proto.GetKeys()), proto.GetSoftwareVersion());
		}

		public static DatanodeCommand Convert(DatanodeProtocolProtos.DatanodeCommandProto
			 proto)
		{
			switch (proto.GetCmdType())
			{
				case DatanodeProtocolProtos.DatanodeCommandProto.Type.BalancerBandwidthCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetBalancerCmd());
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetBlkCmd());
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockRecoveryCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetRecoveryCmd());
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.FinalizeCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetFinalizeCmd());
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.KeyUpdateCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetKeyUpdateCmd()
						);
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.RegisterCommand:
				{
					return RegCmd;
				}

				case DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockIdCommand:
				{
					return Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetBlkIdCmd());
				}

				default:
				{
					return null;
				}
			}
		}

		public static DatanodeProtocolProtos.BalancerBandwidthCommandProto Convert(BalancerBandwidthCommand
			 bbCmd)
		{
			return ((DatanodeProtocolProtos.BalancerBandwidthCommandProto)DatanodeProtocolProtos.BalancerBandwidthCommandProto
				.NewBuilder().SetBandwidth(bbCmd.GetBalancerBandwidthValue()).Build());
		}

		public static DatanodeProtocolProtos.KeyUpdateCommandProto Convert(KeyUpdateCommand
			 cmd)
		{
			return ((DatanodeProtocolProtos.KeyUpdateCommandProto)DatanodeProtocolProtos.KeyUpdateCommandProto
				.NewBuilder().SetKeys(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(cmd.GetExportedKeys
				())).Build());
		}

		public static DatanodeProtocolProtos.BlockRecoveryCommandProto Convert(BlockRecoveryCommand
			 cmd)
		{
			DatanodeProtocolProtos.BlockRecoveryCommandProto.Builder builder = DatanodeProtocolProtos.BlockRecoveryCommandProto
				.NewBuilder();
			foreach (BlockRecoveryCommand.RecoveringBlock b in cmd.GetRecoveringBlocks())
			{
				builder.AddBlocks(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(b));
			}
			return ((DatanodeProtocolProtos.BlockRecoveryCommandProto)builder.Build());
		}

		public static DatanodeProtocolProtos.FinalizeCommandProto Convert(FinalizeCommand
			 cmd)
		{
			return ((DatanodeProtocolProtos.FinalizeCommandProto)DatanodeProtocolProtos.FinalizeCommandProto
				.NewBuilder().SetBlockPoolId(cmd.GetBlockPoolId()).Build());
		}

		public static DatanodeProtocolProtos.BlockCommandProto Convert(BlockCommand cmd)
		{
			DatanodeProtocolProtos.BlockCommandProto.Builder builder = DatanodeProtocolProtos.BlockCommandProto
				.NewBuilder().SetBlockPoolId(cmd.GetBlockPoolId());
			switch (cmd.GetAction())
			{
				case DatanodeProtocol.DnaTransfer:
				{
					builder.SetAction(DatanodeProtocolProtos.BlockCommandProto.Action.Transfer);
					break;
				}

				case DatanodeProtocol.DnaInvalidate:
				{
					builder.SetAction(DatanodeProtocolProtos.BlockCommandProto.Action.Invalidate);
					break;
				}

				case DatanodeProtocol.DnaShutdown:
				{
					builder.SetAction(DatanodeProtocolProtos.BlockCommandProto.Action.Shutdown);
					break;
				}

				default:
				{
					throw new Exception("Invalid action");
				}
			}
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks = cmd.GetBlocks();
			for (int i = 0; i < blocks.Length; i++)
			{
				builder.AddBlocks(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(blocks[i]));
			}
			builder.AddAllTargets(Convert(cmd.GetTargets())).AddAllTargetStorageUuids(Convert
				(cmd.GetTargetStorageIDs()));
			StorageType[][] types = cmd.GetTargetStorageTypes();
			if (types != null)
			{
				builder.AddAllTargetStorageTypes(Convert(types));
			}
			return ((DatanodeProtocolProtos.BlockCommandProto)builder.Build());
		}

		private static IList<HdfsProtos.StorageTypesProto> Convert(StorageType[][] types)
		{
			IList<HdfsProtos.StorageTypesProto> list = Lists.NewArrayList();
			if (types != null)
			{
				foreach (StorageType[] ts in types)
				{
					HdfsProtos.StorageTypesProto.Builder builder = HdfsProtos.StorageTypesProto.NewBuilder
						();
					builder.AddAllStorageTypes(ConvertStorageTypes(ts));
					list.AddItem(((HdfsProtos.StorageTypesProto)builder.Build()));
				}
			}
			return list;
		}

		public static DatanodeProtocolProtos.BlockIdCommandProto Convert(BlockIdCommand cmd
			)
		{
			DatanodeProtocolProtos.BlockIdCommandProto.Builder builder = DatanodeProtocolProtos.BlockIdCommandProto
				.NewBuilder().SetBlockPoolId(cmd.GetBlockPoolId());
			switch (cmd.GetAction())
			{
				case DatanodeProtocol.DnaCache:
				{
					builder.SetAction(DatanodeProtocolProtos.BlockIdCommandProto.Action.Cache);
					break;
				}

				case DatanodeProtocol.DnaUncache:
				{
					builder.SetAction(DatanodeProtocolProtos.BlockIdCommandProto.Action.Uncache);
					break;
				}

				default:
				{
					throw new Exception("Invalid action");
				}
			}
			long[] blockIds = cmd.GetBlockIds();
			for (int i = 0; i < blockIds.Length; i++)
			{
				builder.AddBlockIds(blockIds[i]);
			}
			return ((DatanodeProtocolProtos.BlockIdCommandProto)builder.Build());
		}

		private static IList<HdfsProtos.DatanodeInfosProto> Convert(DatanodeInfo[][] targets
			)
		{
			HdfsProtos.DatanodeInfosProto[] ret = new HdfsProtos.DatanodeInfosProto[targets.Length
				];
			for (int i = 0; i < targets.Length; i++)
			{
				ret[i] = ((HdfsProtos.DatanodeInfosProto)HdfsProtos.DatanodeInfosProto.NewBuilder
					().AddAllDatanodes(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(targets[i]
					)).Build());
			}
			return Arrays.AsList(ret);
		}

		private static IList<HdfsProtos.StorageUuidsProto> Convert(string[][] targetStorageUuids
			)
		{
			HdfsProtos.StorageUuidsProto[] ret = new HdfsProtos.StorageUuidsProto[targetStorageUuids
				.Length];
			for (int i = 0; i < targetStorageUuids.Length; i++)
			{
				ret[i] = ((HdfsProtos.StorageUuidsProto)HdfsProtos.StorageUuidsProto.NewBuilder()
					.AddAllStorageUuids(Arrays.AsList(targetStorageUuids[i])).Build());
			}
			return Arrays.AsList(ret);
		}

		public static DatanodeProtocolProtos.DatanodeCommandProto Convert(DatanodeCommand
			 datanodeCommand)
		{
			DatanodeProtocolProtos.DatanodeCommandProto.Builder builder = DatanodeProtocolProtos.DatanodeCommandProto
				.NewBuilder();
			if (datanodeCommand == null)
			{
				return ((DatanodeProtocolProtos.DatanodeCommandProto)builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type
					.NullDatanodeCommand).Build());
			}
			switch (datanodeCommand.GetAction())
			{
				case DatanodeProtocol.DnaBalancerbandwidthupdate:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.BalancerBandwidthCommand
						).SetBalancerCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((BalancerBandwidthCommand
						)datanodeCommand));
					break;
				}

				case DatanodeProtocol.DnaAccesskeyupdate:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.KeyUpdateCommand
						).SetKeyUpdateCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((KeyUpdateCommand
						)datanodeCommand));
					break;
				}

				case DatanodeProtocol.DnaRecoverblock:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockRecoveryCommand
						).SetRecoveryCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((BlockRecoveryCommand
						)datanodeCommand));
					break;
				}

				case DatanodeProtocol.DnaFinalize:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.FinalizeCommand
						).SetFinalizeCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((FinalizeCommand
						)datanodeCommand));
					break;
				}

				case DatanodeProtocol.DnaRegister:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.RegisterCommand
						).SetRegisterCmd(RegCmdProto);
					break;
				}

				case DatanodeProtocol.DnaTransfer:
				case DatanodeProtocol.DnaInvalidate:
				case DatanodeProtocol.DnaShutdown:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockCommand)
						.SetBlkCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((BlockCommand)datanodeCommand
						));
					break;
				}

				case DatanodeProtocol.DnaCache:
				case DatanodeProtocol.DnaUncache:
				{
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockIdCommand
						).SetBlkIdCmd(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((BlockIdCommand
						)datanodeCommand));
					break;
				}

				case DatanodeProtocol.DnaUnknown:
				default:
				{
					//Not expected
					builder.SetCmdType(DatanodeProtocolProtos.DatanodeCommandProto.Type.NullDatanodeCommand
						);
					break;
				}
			}
			return ((DatanodeProtocolProtos.DatanodeCommandProto)builder.Build());
		}

		public static KeyUpdateCommand Convert(DatanodeProtocolProtos.KeyUpdateCommandProto
			 keyUpdateCmd)
		{
			return new KeyUpdateCommand(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(keyUpdateCmd
				.GetKeys()));
		}

		public static FinalizeCommand Convert(DatanodeProtocolProtos.FinalizeCommandProto
			 finalizeCmd)
		{
			return new FinalizeCommand(finalizeCmd.GetBlockPoolId());
		}

		public static BlockRecoveryCommand Convert(DatanodeProtocolProtos.BlockRecoveryCommandProto
			 recoveryCmd)
		{
			IList<HdfsProtos.RecoveringBlockProto> list = recoveryCmd.GetBlocksList();
			IList<BlockRecoveryCommand.RecoveringBlock> recoveringBlocks = new AList<BlockRecoveryCommand.RecoveringBlock
				>(list.Count);
			foreach (HdfsProtos.RecoveringBlockProto rbp in list)
			{
				recoveringBlocks.AddItem(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(rbp));
			}
			return new BlockRecoveryCommand(recoveringBlocks);
		}

		public static BlockCommand Convert(DatanodeProtocolProtos.BlockCommandProto blkCmd
			)
		{
			IList<HdfsProtos.BlockProto> blockProtoList = blkCmd.GetBlocksList();
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks = new Org.Apache.Hadoop.Hdfs.Protocol.Block
				[blockProtoList.Count];
			for (int i = 0; i < blockProtoList.Count; i++)
			{
				blocks[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(blockProtoList[i]);
			}
			IList<HdfsProtos.DatanodeInfosProto> targetList = blkCmd.GetTargetsList();
			DatanodeInfo[][] targets = new DatanodeInfo[targetList.Count][];
			for (int i_1 = 0; i_1 < targetList.Count; i_1++)
			{
				targets[i_1] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(targetList[i_1]
					);
			}
			StorageType[][] targetStorageTypes = new StorageType[targetList.Count][];
			IList<HdfsProtos.StorageTypesProto> targetStorageTypesList = blkCmd.GetTargetStorageTypesList
				();
			if (targetStorageTypesList.IsEmpty())
			{
				// missing storage types
				for (int i_2 = 0; i_2 < targetStorageTypes.Length; i_2++)
				{
					targetStorageTypes[i_2] = new StorageType[targets[i_2].Length];
					Arrays.Fill(targetStorageTypes[i_2], StorageType.Default);
				}
			}
			else
			{
				for (int i_2 = 0; i_2 < targetStorageTypes.Length; i_2++)
				{
					IList<HdfsProtos.StorageTypeProto> p = targetStorageTypesList[i_2].GetStorageTypesList
						();
					targetStorageTypes[i_2] = ConvertStorageTypes(p, targets[i_2].Length);
				}
			}
			IList<HdfsProtos.StorageUuidsProto> targetStorageUuidsList = blkCmd.GetTargetStorageUuidsList
				();
			string[][] targetStorageIDs = new string[targetStorageUuidsList.Count][];
			for (int i_3 = 0; i_3 < targetStorageIDs.Length; i_3++)
			{
				IList<string> storageIDs = targetStorageUuidsList[i_3].GetStorageUuidsList();
				targetStorageIDs[i_3] = Sharpen.Collections.ToArray(storageIDs, new string[storageIDs
					.Count]);
			}
			int action = DatanodeProtocol.DnaUnknown;
			switch (blkCmd.GetAction())
			{
				case DatanodeProtocolProtos.BlockCommandProto.Action.Transfer:
				{
					action = DatanodeProtocol.DnaTransfer;
					break;
				}

				case DatanodeProtocolProtos.BlockCommandProto.Action.Invalidate:
				{
					action = DatanodeProtocol.DnaInvalidate;
					break;
				}

				case DatanodeProtocolProtos.BlockCommandProto.Action.Shutdown:
				{
					action = DatanodeProtocol.DnaShutdown;
					break;
				}

				default:
				{
					throw new Exception("Unknown action type: " + blkCmd.GetAction());
				}
			}
			return new BlockCommand(action, blkCmd.GetBlockPoolId(), blocks, targets, targetStorageTypes
				, targetStorageIDs);
		}

		public static BlockIdCommand Convert(DatanodeProtocolProtos.BlockIdCommandProto blkIdCmd
			)
		{
			int numBlockIds = blkIdCmd.GetBlockIdsCount();
			long[] blockIds = new long[numBlockIds];
			for (int i = 0; i < numBlockIds; i++)
			{
				blockIds[i] = blkIdCmd.GetBlockIds(i);
			}
			int action = DatanodeProtocol.DnaUnknown;
			switch (blkIdCmd.GetAction())
			{
				case DatanodeProtocolProtos.BlockIdCommandProto.Action.Cache:
				{
					action = DatanodeProtocol.DnaCache;
					break;
				}

				case DatanodeProtocolProtos.BlockIdCommandProto.Action.Uncache:
				{
					action = DatanodeProtocol.DnaUncache;
					break;
				}

				default:
				{
					throw new Exception("Unknown action type: " + blkIdCmd.GetAction());
				}
			}
			return new BlockIdCommand(action, blkIdCmd.GetBlockPoolId(), blockIds);
		}

		public static DatanodeInfo[] Convert(HdfsProtos.DatanodeInfosProto datanodeInfosProto
			)
		{
			IList<HdfsProtos.DatanodeInfoProto> proto = datanodeInfosProto.GetDatanodesList();
			DatanodeInfo[] infos = new DatanodeInfo[proto.Count];
			for (int i = 0; i < infos.Length; i++)
			{
				infos[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto[i]);
			}
			return infos;
		}

		public static BalancerBandwidthCommand Convert(DatanodeProtocolProtos.BalancerBandwidthCommandProto
			 balancerCmd)
		{
			return new BalancerBandwidthCommand(balancerCmd.GetBandwidth());
		}

		public static DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto Convert(ReceivedDeletedBlockInfo
			 receivedDeletedBlockInfo)
		{
			DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.Builder builder = DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto
				.NewBuilder();
			DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus status;
			switch (receivedDeletedBlockInfo.GetStatus())
			{
				case ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock:
				{
					status = DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Receiving;
					break;
				}

				case ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock:
				{
					status = DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Received;
					break;
				}

				case ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock:
				{
					status = DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Deleted;
					break;
				}

				default:
				{
					throw new ArgumentException("Bad status: " + receivedDeletedBlockInfo.GetStatus()
						);
				}
			}
			builder.SetStatus(status);
			if (receivedDeletedBlockInfo.GetDelHints() != null)
			{
				builder.SetDeleteHint(receivedDeletedBlockInfo.GetDelHints());
			}
			return ((DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto)builder.SetBlock(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(receivedDeletedBlockInfo.GetBlock())).Build());
		}

		public static ReceivedDeletedBlockInfo Convert(DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto
			 proto)
		{
			ReceivedDeletedBlockInfo.BlockStatus status = null;
			switch (proto.GetStatus())
			{
				case DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Receiving:
				{
					status = ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock;
					break;
				}

				case DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Received:
				{
					status = ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock;
					break;
				}

				case DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto.BlockStatus.Deleted:
				{
					status = ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock;
					break;
				}
			}
			return new ReceivedDeletedBlockInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert
				(proto.GetBlock()), status, proto.HasDeleteHint() ? proto.GetDeleteHint() : null
				);
		}

		public static HdfsProtos.NamespaceInfoProto Convert(NamespaceInfo info)
		{
			return ((HdfsProtos.NamespaceInfoProto)HdfsProtos.NamespaceInfoProto.NewBuilder()
				.SetBlockPoolID(info.GetBlockPoolID()).SetBuildVersion(info.GetBuildVersion()).SetUnused
				(0).SetStorageInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert((StorageInfo
				)info)).SetSoftwareVersion(info.GetSoftwareVersion()).SetCapabilities(info.GetCapabilities
				()).Build());
		}

		// Located Block Arrays and Lists
		public static HdfsProtos.LocatedBlockProto[] ConvertLocatedBlock(LocatedBlock[] lb
			)
		{
			if (lb == null)
			{
				return null;
			}
			return Sharpen.Collections.ToArray(ConvertLocatedBlock2(Arrays.AsList(lb)), new HdfsProtos.LocatedBlockProto
				[lb.Length]);
		}

		public static LocatedBlock[] ConvertLocatedBlock(HdfsProtos.LocatedBlockProto[] lb
			)
		{
			if (lb == null)
			{
				return null;
			}
			return Sharpen.Collections.ToArray(ConvertLocatedBlock(Arrays.AsList(lb)), new LocatedBlock
				[lb.Length]);
		}

		public static IList<LocatedBlock> ConvertLocatedBlock(IList<HdfsProtos.LocatedBlockProto
			> lb)
		{
			if (lb == null)
			{
				return null;
			}
			int len = lb.Count;
			IList<LocatedBlock> result = new AList<LocatedBlock>(len);
			for (int i = 0; i < len; ++i)
			{
				result.AddItem(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(lb[i]));
			}
			return result;
		}

		public static IList<HdfsProtos.LocatedBlockProto> ConvertLocatedBlock2(IList<LocatedBlock
			> lb)
		{
			if (lb == null)
			{
				return null;
			}
			int len = lb.Count;
			IList<HdfsProtos.LocatedBlockProto> result = new AList<HdfsProtos.LocatedBlockProto
				>(len);
			for (int i = 0; i < len; ++i)
			{
				result.AddItem(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(lb[i]));
			}
			return result;
		}

		// LocatedBlocks
		public static LocatedBlocks Convert(HdfsProtos.LocatedBlocksProto lb)
		{
			return new LocatedBlocks(lb.GetFileLength(), lb.GetUnderConstruction(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.ConvertLocatedBlock(lb.GetBlocksList()), lb.HasLastBlock() ? Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(lb.GetLastBlock()) : null, lb.GetIsLastBlockComplete(), lb.HasFileEncryptionInfo
				() ? Convert(lb.GetFileEncryptionInfo()) : null);
		}

		public static HdfsProtos.LocatedBlocksProto Convert(LocatedBlocks lb)
		{
			if (lb == null)
			{
				return null;
			}
			HdfsProtos.LocatedBlocksProto.Builder builder = HdfsProtos.LocatedBlocksProto.NewBuilder
				();
			if (lb.GetLastLocatedBlock() != null)
			{
				builder.SetLastBlock(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(lb.GetLastLocatedBlock
					()));
			}
			if (lb.GetFileEncryptionInfo() != null)
			{
				builder.SetFileEncryptionInfo(Convert(lb.GetFileEncryptionInfo()));
			}
			return ((HdfsProtos.LocatedBlocksProto)builder.SetFileLength(lb.GetFileLength()).
				SetUnderConstruction(lb.IsUnderConstruction()).AddAllBlocks(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.ConvertLocatedBlock2(lb.GetLocatedBlocks())).SetIsLastBlockComplete(lb.IsLastBlockComplete
				()).Build());
		}

		// DataEncryptionKey
		public static DataEncryptionKey Convert(HdfsProtos.DataEncryptionKeyProto bet)
		{
			string encryptionAlgorithm = bet.GetEncryptionAlgorithm();
			return new DataEncryptionKey(bet.GetKeyId(), bet.GetBlockPoolId(), bet.GetNonce()
				.ToByteArray(), bet.GetEncryptionKey().ToByteArray(), bet.GetExpiryDate(), encryptionAlgorithm
				.IsEmpty() ? null : encryptionAlgorithm);
		}

		public static HdfsProtos.DataEncryptionKeyProto Convert(DataEncryptionKey bet)
		{
			HdfsProtos.DataEncryptionKeyProto.Builder b = HdfsProtos.DataEncryptionKeyProto.NewBuilder
				().SetKeyId(bet.keyId).SetBlockPoolId(bet.blockPoolId).SetNonce(ByteString.CopyFrom
				(bet.nonce)).SetEncryptionKey(ByteString.CopyFrom(bet.encryptionKey)).SetExpiryDate
				(bet.expiryDate);
			if (bet.encryptionAlgorithm != null)
			{
				b.SetEncryptionAlgorithm(bet.encryptionAlgorithm);
			}
			return ((HdfsProtos.DataEncryptionKeyProto)b.Build());
		}

		public static FsServerDefaults Convert(HdfsProtos.FsServerDefaultsProto fs)
		{
			if (fs == null)
			{
				return null;
			}
			return new FsServerDefaults(fs.GetBlockSize(), fs.GetBytesPerChecksum(), fs.GetWritePacketSize
				(), (short)fs.GetReplication(), fs.GetFileBufferSize(), fs.GetEncryptDataTransfer
				(), fs.GetTrashInterval(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(fs
				.GetChecksumType()));
		}

		public static HdfsProtos.FsServerDefaultsProto Convert(FsServerDefaults fs)
		{
			if (fs == null)
			{
				return null;
			}
			return ((HdfsProtos.FsServerDefaultsProto)HdfsProtos.FsServerDefaultsProto.NewBuilder
				().SetBlockSize(fs.GetBlockSize()).SetBytesPerChecksum(fs.GetBytesPerChecksum())
				.SetWritePacketSize(fs.GetWritePacketSize()).SetReplication(fs.GetReplication())
				.SetFileBufferSize(fs.GetFileBufferSize()).SetEncryptDataTransfer(fs.GetEncryptDataTransfer
				()).SetTrashInterval(fs.GetTrashInterval()).SetChecksumType(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(fs.GetChecksumType())).Build());
		}

		public static HdfsProtos.FsPermissionProto Convert(FsPermission p)
		{
			return ((HdfsProtos.FsPermissionProto)HdfsProtos.FsPermissionProto.NewBuilder().SetPerm
				(p.ToExtendedShort()).Build());
		}

		public static FsPermission Convert(HdfsProtos.FsPermissionProto p)
		{
			return new FsPermissionExtension((short)p.GetPerm());
		}

		// The creatFlag field in PB is a bitmask whose values are the same a the 
		// emum values of CreateFlag
		public static int ConvertCreateFlag(EnumSetWritable<CreateFlag> flag)
		{
			int value = 0;
			if (flag.Contains(CreateFlag.Append))
			{
				value |= ClientNamenodeProtocolProtos.CreateFlagProto.Append.GetNumber();
			}
			if (flag.Contains(CreateFlag.Create))
			{
				value |= ClientNamenodeProtocolProtos.CreateFlagProto.Create.GetNumber();
			}
			if (flag.Contains(CreateFlag.Overwrite))
			{
				value |= ClientNamenodeProtocolProtos.CreateFlagProto.Overwrite.GetNumber();
			}
			if (flag.Contains(CreateFlag.LazyPersist))
			{
				value |= ClientNamenodeProtocolProtos.CreateFlagProto.LazyPersist.GetNumber();
			}
			if (flag.Contains(CreateFlag.NewBlock))
			{
				value |= ClientNamenodeProtocolProtos.CreateFlagProto.NewBlock.GetNumber();
			}
			return value;
		}

		public static EnumSetWritable<CreateFlag> ConvertCreateFlag(int flag)
		{
			EnumSet<CreateFlag> result = EnumSet.NoneOf<CreateFlag>();
			if ((flag & ClientNamenodeProtocolProtos.CreateFlagProto.AppendValue) == ClientNamenodeProtocolProtos.CreateFlagProto
				.AppendValue)
			{
				result.AddItem(CreateFlag.Append);
			}
			if ((flag & ClientNamenodeProtocolProtos.CreateFlagProto.CreateValue) == ClientNamenodeProtocolProtos.CreateFlagProto
				.CreateValue)
			{
				result.AddItem(CreateFlag.Create);
			}
			if ((flag & ClientNamenodeProtocolProtos.CreateFlagProto.OverwriteValue) == ClientNamenodeProtocolProtos.CreateFlagProto
				.OverwriteValue)
			{
				result.AddItem(CreateFlag.Overwrite);
			}
			if ((flag & ClientNamenodeProtocolProtos.CreateFlagProto.LazyPersistValue) == ClientNamenodeProtocolProtos.CreateFlagProto
				.LazyPersistValue)
			{
				result.AddItem(CreateFlag.LazyPersist);
			}
			if ((flag & ClientNamenodeProtocolProtos.CreateFlagProto.NewBlockValue) == ClientNamenodeProtocolProtos.CreateFlagProto
				.NewBlockValue)
			{
				result.AddItem(CreateFlag.NewBlock);
			}
			return new EnumSetWritable<CreateFlag>(result, typeof(CreateFlag));
		}

		public static int ConvertCacheFlags(EnumSet<CacheFlag> flags)
		{
			int value = 0;
			if (flags.Contains(CacheFlag.Force))
			{
				value |= ClientNamenodeProtocolProtos.CacheFlagProto.Force.GetNumber();
			}
			return value;
		}

		public static EnumSet<CacheFlag> ConvertCacheFlags(int flags)
		{
			EnumSet<CacheFlag> result = EnumSet.NoneOf<CacheFlag>();
			if ((flags & ClientNamenodeProtocolProtos.CacheFlagProto.ForceValue) == ClientNamenodeProtocolProtos.CacheFlagProto
				.ForceValue)
			{
				result.AddItem(CacheFlag.Force);
			}
			return result;
		}

		public static HdfsFileStatus Convert(HdfsProtos.HdfsFileStatusProto fs)
		{
			if (fs == null)
			{
				return null;
			}
			return new HdfsLocatedFileStatus(fs.GetLength(), fs.GetFileType().Equals(HdfsProtos.HdfsFileStatusProto.FileType
				.IsDir), fs.GetBlockReplication(), fs.GetBlocksize(), fs.GetModificationTime(), 
				fs.GetAccessTime(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(fs.GetPermission
				()), fs.GetOwner(), fs.GetGroup(), fs.GetFileType().Equals(HdfsProtos.HdfsFileStatusProto.FileType
				.IsSymlink) ? fs.GetSymlink().ToByteArray() : null, fs.GetPath().ToByteArray(), 
				fs.HasFileId() ? fs.GetFileId() : INodeId.GrandfatherInodeId, fs.HasLocations() ? 
				Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(fs.GetLocations()) : null, fs
				.HasChildrenNum() ? fs.GetChildrenNum() : -1, fs.HasFileEncryptionInfo() ? Convert
				(fs.GetFileEncryptionInfo()) : null, fs.HasStoragePolicy() ? unchecked((byte)fs.
				GetStoragePolicy()) : BlockStoragePolicySuite.IdUnspecified);
		}

		public static SnapshottableDirectoryStatus Convert(HdfsProtos.SnapshottableDirectoryStatusProto
			 sdirStatusProto)
		{
			if (sdirStatusProto == null)
			{
				return null;
			}
			HdfsProtos.HdfsFileStatusProto status = sdirStatusProto.GetDirStatus();
			return new SnapshottableDirectoryStatus(status.GetModificationTime(), status.GetAccessTime
				(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(status.GetPermission()), 
				status.GetOwner(), status.GetGroup(), status.GetPath().ToByteArray(), status.GetFileId
				(), status.GetChildrenNum(), sdirStatusProto.GetSnapshotNumber(), sdirStatusProto
				.GetSnapshotQuota(), sdirStatusProto.GetParentFullpath().ToByteArray());
		}

		public static HdfsProtos.HdfsFileStatusProto Convert(HdfsFileStatus fs)
		{
			if (fs == null)
			{
				return null;
			}
			HdfsProtos.HdfsFileStatusProto.FileType fType = HdfsProtos.HdfsFileStatusProto.FileType
				.IsFile;
			if (fs.IsDir())
			{
				fType = HdfsProtos.HdfsFileStatusProto.FileType.IsDir;
			}
			else
			{
				if (fs.IsSymlink())
				{
					fType = HdfsProtos.HdfsFileStatusProto.FileType.IsSymlink;
				}
			}
			HdfsProtos.HdfsFileStatusProto.Builder builder = HdfsProtos.HdfsFileStatusProto.NewBuilder
				().SetLength(fs.GetLen()).SetFileType(fType).SetBlockReplication(fs.GetReplication
				()).SetBlocksize(fs.GetBlockSize()).SetModificationTime(fs.GetModificationTime()
				).SetAccessTime(fs.GetAccessTime()).SetPermission(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(fs.GetPermission())).SetOwner(fs.GetOwner()).SetGroup(fs.GetGroup()).SetFileId
				(fs.GetFileId()).SetChildrenNum(fs.GetChildrenNum()).SetPath(ByteString.CopyFrom
				(fs.GetLocalNameInBytes())).SetStoragePolicy(fs.GetStoragePolicy());
			if (fs.IsSymlink())
			{
				builder.SetSymlink(ByteString.CopyFrom(fs.GetSymlinkInBytes()));
			}
			if (fs.GetFileEncryptionInfo() != null)
			{
				builder.SetFileEncryptionInfo(Convert(fs.GetFileEncryptionInfo()));
			}
			if (fs is HdfsLocatedFileStatus)
			{
				HdfsLocatedFileStatus lfs = (HdfsLocatedFileStatus)fs;
				LocatedBlocks locations = lfs.GetBlockLocations();
				if (locations != null)
				{
					builder.SetLocations(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(locations
						));
				}
			}
			return ((HdfsProtos.HdfsFileStatusProto)builder.Build());
		}

		public static HdfsProtos.SnapshottableDirectoryStatusProto Convert(SnapshottableDirectoryStatus
			 status)
		{
			if (status == null)
			{
				return null;
			}
			int snapshotNumber = status.GetSnapshotNumber();
			int snapshotQuota = status.GetSnapshotQuota();
			byte[] parentFullPath = status.GetParentFullPath();
			ByteString parentFullPathBytes = ByteString.CopyFrom(parentFullPath == null ? DFSUtil
				.EmptyBytes : parentFullPath);
			HdfsProtos.HdfsFileStatusProto fs = Convert(status.GetDirStatus());
			HdfsProtos.SnapshottableDirectoryStatusProto.Builder builder = HdfsProtos.SnapshottableDirectoryStatusProto
				.NewBuilder().SetSnapshotNumber(snapshotNumber).SetSnapshotQuota(snapshotQuota).
				SetParentFullpath(parentFullPathBytes).SetDirStatus(fs);
			return ((HdfsProtos.SnapshottableDirectoryStatusProto)builder.Build());
		}

		public static HdfsProtos.HdfsFileStatusProto[] Convert(HdfsFileStatus[] fs)
		{
			if (fs == null)
			{
				return null;
			}
			int len = fs.Length;
			HdfsProtos.HdfsFileStatusProto[] result = new HdfsProtos.HdfsFileStatusProto[len]
				;
			for (int i = 0; i < len; ++i)
			{
				result[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(fs[i]);
			}
			return result;
		}

		public static HdfsFileStatus[] Convert(HdfsProtos.HdfsFileStatusProto[] fs)
		{
			if (fs == null)
			{
				return null;
			}
			int len = fs.Length;
			HdfsFileStatus[] result = new HdfsFileStatus[len];
			for (int i = 0; i < len; ++i)
			{
				result[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(fs[i]);
			}
			return result;
		}

		public static DirectoryListing Convert(HdfsProtos.DirectoryListingProto dl)
		{
			if (dl == null)
			{
				return null;
			}
			IList<HdfsProtos.HdfsFileStatusProto> partList = dl.GetPartialListingList();
			return new DirectoryListing(partList.IsEmpty() ? new HdfsLocatedFileStatus[0] : Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(Sharpen.Collections.ToArray(partList, new HdfsProtos.HdfsFileStatusProto
				[partList.Count])), dl.GetRemainingEntries());
		}

		public static HdfsProtos.DirectoryListingProto Convert(DirectoryListing d)
		{
			if (d == null)
			{
				return null;
			}
			return ((HdfsProtos.DirectoryListingProto)HdfsProtos.DirectoryListingProto.NewBuilder
				().AddAllPartialListing(Arrays.AsList(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.Convert(d.GetPartialListing()))).SetRemainingEntries(d.GetRemainingEntries()).Build
				());
		}

		public static long[] Convert(ClientNamenodeProtocolProtos.GetFsStatsResponseProto
			 res)
		{
			long[] result = new long[7];
			result[ClientProtocol.GetStatsCapacityIdx] = res.GetCapacity();
			result[ClientProtocol.GetStatsUsedIdx] = res.GetUsed();
			result[ClientProtocol.GetStatsRemainingIdx] = res.GetRemaining();
			result[ClientProtocol.GetStatsUnderReplicatedIdx] = res.GetUnderReplicated();
			result[ClientProtocol.GetStatsCorruptBlocksIdx] = res.GetCorruptBlocks();
			result[ClientProtocol.GetStatsMissingBlocksIdx] = res.GetMissingBlocks();
			result[ClientProtocol.GetStatsMissingReplOneBlocksIdx] = res.GetMissingReplOneBlocks
				();
			return result;
		}

		public static ClientNamenodeProtocolProtos.GetFsStatsResponseProto Convert(long[]
			 fsStats)
		{
			ClientNamenodeProtocolProtos.GetFsStatsResponseProto.Builder result = ClientNamenodeProtocolProtos.GetFsStatsResponseProto
				.NewBuilder();
			if (fsStats.Length >= ClientProtocol.GetStatsCapacityIdx + 1)
			{
				result.SetCapacity(fsStats[ClientProtocol.GetStatsCapacityIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsUsedIdx + 1)
			{
				result.SetUsed(fsStats[ClientProtocol.GetStatsUsedIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsRemainingIdx + 1)
			{
				result.SetRemaining(fsStats[ClientProtocol.GetStatsRemainingIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsUnderReplicatedIdx + 1)
			{
				result.SetUnderReplicated(fsStats[ClientProtocol.GetStatsUnderReplicatedIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsCorruptBlocksIdx + 1)
			{
				result.SetCorruptBlocks(fsStats[ClientProtocol.GetStatsCorruptBlocksIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsMissingBlocksIdx + 1)
			{
				result.SetMissingBlocks(fsStats[ClientProtocol.GetStatsMissingBlocksIdx]);
			}
			if (fsStats.Length >= ClientProtocol.GetStatsMissingReplOneBlocksIdx + 1)
			{
				result.SetMissingReplOneBlocks(fsStats[ClientProtocol.GetStatsMissingReplOneBlocksIdx
					]);
			}
			return ((ClientNamenodeProtocolProtos.GetFsStatsResponseProto)result.Build());
		}

		public static ClientNamenodeProtocolProtos.DatanodeReportTypeProto Convert(HdfsConstants.DatanodeReportType
			 t)
		{
			switch (t)
			{
				case HdfsConstants.DatanodeReportType.All:
				{
					return ClientNamenodeProtocolProtos.DatanodeReportTypeProto.All;
				}

				case HdfsConstants.DatanodeReportType.Live:
				{
					return ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Live;
				}

				case HdfsConstants.DatanodeReportType.Dead:
				{
					return ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Dead;
				}

				case HdfsConstants.DatanodeReportType.Decommissioning:
				{
					return ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Decommissioning;
				}

				default:
				{
					throw new ArgumentException("Unexpected data type report:" + t);
				}
			}
		}

		public static HdfsConstants.DatanodeReportType Convert(ClientNamenodeProtocolProtos.DatanodeReportTypeProto
			 t)
		{
			switch (t)
			{
				case ClientNamenodeProtocolProtos.DatanodeReportTypeProto.All:
				{
					return HdfsConstants.DatanodeReportType.All;
				}

				case ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Live:
				{
					return HdfsConstants.DatanodeReportType.Live;
				}

				case ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Dead:
				{
					return HdfsConstants.DatanodeReportType.Dead;
				}

				case ClientNamenodeProtocolProtos.DatanodeReportTypeProto.Decommissioning:
				{
					return HdfsConstants.DatanodeReportType.Decommissioning;
				}

				default:
				{
					throw new ArgumentException("Unexpected data type report:" + t);
				}
			}
		}

		public static ClientNamenodeProtocolProtos.SafeModeActionProto Convert(HdfsConstants.SafeModeAction
			 a)
		{
			switch (a)
			{
				case HdfsConstants.SafeModeAction.SafemodeLeave:
				{
					return ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeLeave;
				}

				case HdfsConstants.SafeModeAction.SafemodeEnter:
				{
					return ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeEnter;
				}

				case HdfsConstants.SafeModeAction.SafemodeGet:
				{
					return ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeGet;
				}

				default:
				{
					throw new ArgumentException("Unexpected SafeModeAction :" + a);
				}
			}
		}

		public static HdfsConstants.SafeModeAction Convert(ClientNamenodeProtocolProtos.SafeModeActionProto
			 a)
		{
			switch (a)
			{
				case ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeLeave:
				{
					return HdfsConstants.SafeModeAction.SafemodeLeave;
				}

				case ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeEnter:
				{
					return HdfsConstants.SafeModeAction.SafemodeEnter;
				}

				case ClientNamenodeProtocolProtos.SafeModeActionProto.SafemodeGet:
				{
					return HdfsConstants.SafeModeAction.SafemodeGet;
				}

				default:
				{
					throw new ArgumentException("Unexpected SafeModeAction :" + a);
				}
			}
		}

		public static ClientNamenodeProtocolProtos.RollingUpgradeActionProto Convert(HdfsConstants.RollingUpgradeAction
			 a)
		{
			switch (a)
			{
				case HdfsConstants.RollingUpgradeAction.Query:
				{
					return ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Query;
				}

				case HdfsConstants.RollingUpgradeAction.Prepare:
				{
					return ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Start;
				}

				case HdfsConstants.RollingUpgradeAction.Finalize:
				{
					return ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Finalize;
				}

				default:
				{
					throw new ArgumentException("Unexpected value: " + a);
				}
			}
		}

		public static HdfsConstants.RollingUpgradeAction Convert(ClientNamenodeProtocolProtos.RollingUpgradeActionProto
			 a)
		{
			switch (a)
			{
				case ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Query:
				{
					return HdfsConstants.RollingUpgradeAction.Query;
				}

				case ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Start:
				{
					return HdfsConstants.RollingUpgradeAction.Prepare;
				}

				case ClientNamenodeProtocolProtos.RollingUpgradeActionProto.Finalize:
				{
					return HdfsConstants.RollingUpgradeAction.Finalize;
				}

				default:
				{
					throw new ArgumentException("Unexpected value: " + a);
				}
			}
		}

		public static HdfsProtos.RollingUpgradeStatusProto ConvertRollingUpgradeStatus(RollingUpgradeStatus
			 status)
		{
			return ((HdfsProtos.RollingUpgradeStatusProto)HdfsProtos.RollingUpgradeStatusProto
				.NewBuilder().SetBlockPoolId(status.GetBlockPoolId()).SetFinalized(status.IsFinalized
				()).Build());
		}

		public static RollingUpgradeStatus Convert(HdfsProtos.RollingUpgradeStatusProto proto
			)
		{
			return new RollingUpgradeStatus(proto.GetBlockPoolId(), proto.GetFinalized());
		}

		public static ClientNamenodeProtocolProtos.RollingUpgradeInfoProto Convert(RollingUpgradeInfo
			 info)
		{
			return ((ClientNamenodeProtocolProtos.RollingUpgradeInfoProto)ClientNamenodeProtocolProtos.RollingUpgradeInfoProto
				.NewBuilder().SetStatus(ConvertRollingUpgradeStatus(info)).SetCreatedRollbackImages
				(info.CreatedRollbackImages()).SetStartTime(info.GetStartTime()).SetFinalizeTime
				(info.GetFinalizeTime()).Build());
		}

		public static RollingUpgradeInfo Convert(ClientNamenodeProtocolProtos.RollingUpgradeInfoProto
			 proto)
		{
			HdfsProtos.RollingUpgradeStatusProto status = proto.GetStatus();
			return new RollingUpgradeInfo(status.GetBlockPoolId(), proto.GetCreatedRollbackImages
				(), proto.GetStartTime(), proto.GetFinalizeTime());
		}

		public static CorruptFileBlocks Convert(HdfsProtos.CorruptFileBlocksProto c)
		{
			if (c == null)
			{
				return null;
			}
			IList<string> fileList = c.GetFilesList();
			return new CorruptFileBlocks(Sharpen.Collections.ToArray(fileList, new string[fileList
				.Count]), c.GetCookie());
		}

		public static HdfsProtos.CorruptFileBlocksProto Convert(CorruptFileBlocks c)
		{
			if (c == null)
			{
				return null;
			}
			return ((HdfsProtos.CorruptFileBlocksProto)HdfsProtos.CorruptFileBlocksProto.NewBuilder
				().AddAllFiles(Arrays.AsList(c.GetFiles())).SetCookie(c.GetCookie()).Build());
		}

		public static ContentSummary Convert(HdfsProtos.ContentSummaryProto cs)
		{
			if (cs == null)
			{
				return null;
			}
			ContentSummary.Builder builder = new ContentSummary.Builder();
			builder.Length(cs.GetLength()).FileCount(cs.GetFileCount()).DirectoryCount(cs.GetDirectoryCount
				()).Quota(cs.GetQuota()).SpaceConsumed(cs.GetSpaceConsumed()).SpaceQuota(cs.GetSpaceQuota
				());
			if (cs.HasTypeQuotaInfos())
			{
				foreach (HdfsProtos.StorageTypeQuotaInfoProto info in cs.GetTypeQuotaInfos().GetTypeQuotaInfoList
					())
				{
					StorageType type = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.ConvertStorageType(
						info.GetType());
					builder.TypeConsumed(type, info.GetConsumed());
					builder.TypeQuota(type, info.GetQuota());
				}
			}
			return builder.Build();
		}

		public static HdfsProtos.ContentSummaryProto Convert(ContentSummary cs)
		{
			if (cs == null)
			{
				return null;
			}
			HdfsProtos.ContentSummaryProto.Builder builder = HdfsProtos.ContentSummaryProto.NewBuilder
				();
			builder.SetLength(cs.GetLength()).SetFileCount(cs.GetFileCount()).SetDirectoryCount
				(cs.GetDirectoryCount()).SetQuota(cs.GetQuota()).SetSpaceConsumed(cs.GetSpaceConsumed
				()).SetSpaceQuota(cs.GetSpaceQuota());
			if (cs.IsTypeQuotaSet() || cs.IsTypeConsumedAvailable())
			{
				HdfsProtos.StorageTypeQuotaInfosProto.Builder isb = HdfsProtos.StorageTypeQuotaInfosProto
					.NewBuilder();
				foreach (StorageType t in StorageType.GetTypesSupportingQuota())
				{
					HdfsProtos.StorageTypeQuotaInfoProto info = ((HdfsProtos.StorageTypeQuotaInfoProto
						)HdfsProtos.StorageTypeQuotaInfoProto.NewBuilder().SetType(ConvertStorageType(t)
						).SetConsumed(cs.GetTypeConsumed(t)).SetQuota(cs.GetTypeQuota(t)).Build());
					isb.AddTypeQuotaInfo(info);
				}
				builder.SetTypeQuotaInfos(isb);
			}
			return ((HdfsProtos.ContentSummaryProto)builder.Build());
		}

		public static NNHAStatusHeartbeat Convert(DatanodeProtocolProtos.NNHAStatusHeartbeatProto
			 s)
		{
			if (s == null)
			{
				return null;
			}
			switch (s.GetState())
			{
				case DatanodeProtocolProtos.NNHAStatusHeartbeatProto.State.Active:
				{
					return new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active, s.GetTxid
						());
				}

				case DatanodeProtocolProtos.NNHAStatusHeartbeatProto.State.Standby:
				{
					return new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Standby, s.GetTxid
						());
				}

				default:
				{
					throw new ArgumentException("Unexpected NNHAStatusHeartbeat.State:" + s.GetState(
						));
				}
			}
		}

		public static DatanodeProtocolProtos.NNHAStatusHeartbeatProto Convert(NNHAStatusHeartbeat
			 hb)
		{
			if (hb == null)
			{
				return null;
			}
			DatanodeProtocolProtos.NNHAStatusHeartbeatProto.Builder builder = DatanodeProtocolProtos.NNHAStatusHeartbeatProto
				.NewBuilder();
			switch (hb.GetState())
			{
				case HAServiceProtocol.HAServiceState.Active:
				{
					builder.SetState(DatanodeProtocolProtos.NNHAStatusHeartbeatProto.State.Active);
					break;
				}

				case HAServiceProtocol.HAServiceState.Standby:
				{
					builder.SetState(DatanodeProtocolProtos.NNHAStatusHeartbeatProto.State.Standby);
					break;
				}

				default:
				{
					throw new ArgumentException("Unexpected NNHAStatusHeartbeat.State:" + hb.GetState
						());
				}
			}
			builder.SetTxid(hb.GetTxId());
			return ((DatanodeProtocolProtos.NNHAStatusHeartbeatProto)builder.Build());
		}

		public static HdfsProtos.DatanodeStorageProto Convert(DatanodeStorage s)
		{
			return ((HdfsProtos.DatanodeStorageProto)HdfsProtos.DatanodeStorageProto.NewBuilder
				().SetState(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.ConvertState(s.GetState()
				)).SetStorageType(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.ConvertStorageType(
				s.GetStorageType())).SetStorageUuid(s.GetStorageID()).Build());
		}

		private static HdfsProtos.DatanodeStorageProto.StorageState ConvertState(DatanodeStorage.State
			 state)
		{
			switch (state)
			{
				case DatanodeStorage.State.ReadOnlyShared:
				{
					return HdfsProtos.DatanodeStorageProto.StorageState.ReadOnlyShared;
				}

				case DatanodeStorage.State.Normal:
				default:
				{
					return HdfsProtos.DatanodeStorageProto.StorageState.Normal;
				}
			}
		}

		public static IList<HdfsProtos.StorageTypeProto> ConvertStorageTypes(StorageType[]
			 types)
		{
			return ConvertStorageTypes(types, 0);
		}

		public static IList<HdfsProtos.StorageTypeProto> ConvertStorageTypes(StorageType[]
			 types, int startIdx)
		{
			if (types == null)
			{
				return null;
			}
			IList<HdfsProtos.StorageTypeProto> protos = new AList<HdfsProtos.StorageTypeProto
				>(types.Length);
			for (int i = startIdx; i < types.Length; ++i)
			{
				protos.AddItem(ConvertStorageType(types[i]));
			}
			return protos;
		}

		public static HdfsProtos.StorageTypeProto ConvertStorageType(StorageType type)
		{
			switch (type)
			{
				case StorageType.Disk:
				{
					return HdfsProtos.StorageTypeProto.Disk;
				}

				case StorageType.Ssd:
				{
					return HdfsProtos.StorageTypeProto.Ssd;
				}

				case StorageType.Archive:
				{
					return HdfsProtos.StorageTypeProto.Archive;
				}

				case StorageType.RamDisk:
				{
					return HdfsProtos.StorageTypeProto.RamDisk;
				}

				default:
				{
					throw new InvalidOperationException("BUG: StorageType not found, type=" + type);
				}
			}
		}

		public static DatanodeStorage Convert(HdfsProtos.DatanodeStorageProto s)
		{
			return new DatanodeStorage(s.GetStorageUuid(), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper
				.ConvertState(s.GetState()), Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.ConvertStorageType
				(s.GetStorageType()));
		}

		private static DatanodeStorage.State ConvertState(HdfsProtos.DatanodeStorageProto.StorageState
			 state)
		{
			switch (state)
			{
				case HdfsProtos.DatanodeStorageProto.StorageState.ReadOnlyShared:
				{
					return DatanodeStorage.State.ReadOnlyShared;
				}

				case HdfsProtos.DatanodeStorageProto.StorageState.Normal:
				default:
				{
					return DatanodeStorage.State.Normal;
				}
			}
		}

		public static StorageType ConvertStorageType(HdfsProtos.StorageTypeProto type)
		{
			switch (type)
			{
				case HdfsProtos.StorageTypeProto.Disk:
				{
					return StorageType.Disk;
				}

				case HdfsProtos.StorageTypeProto.Ssd:
				{
					return StorageType.Ssd;
				}

				case HdfsProtos.StorageTypeProto.Archive:
				{
					return StorageType.Archive;
				}

				case HdfsProtos.StorageTypeProto.RamDisk:
				{
					return StorageType.RamDisk;
				}

				default:
				{
					throw new InvalidOperationException("BUG: StorageTypeProto not found, type=" + type
						);
				}
			}
		}

		public static StorageType[] ConvertStorageTypes(IList<HdfsProtos.StorageTypeProto
			> storageTypesList, int expectedSize)
		{
			StorageType[] storageTypes = new StorageType[expectedSize];
			if (storageTypesList.Count != expectedSize)
			{
				// missing storage types
				Preconditions.CheckState(storageTypesList.IsEmpty());
				Arrays.Fill(storageTypes, StorageType.Default);
			}
			else
			{
				for (int i = 0; i < storageTypes.Length; ++i)
				{
					storageTypes[i] = ConvertStorageType(storageTypesList[i]);
				}
			}
			return storageTypes;
		}

		public static HdfsProtos.StorageReportProto Convert(StorageReport r)
		{
			HdfsProtos.StorageReportProto.Builder builder = HdfsProtos.StorageReportProto.NewBuilder
				().SetBlockPoolUsed(r.GetBlockPoolUsed()).SetCapacity(r.GetCapacity()).SetDfsUsed
				(r.GetDfsUsed()).SetRemaining(r.GetRemaining()).SetStorageUuid(r.GetStorage().GetStorageID
				()).SetStorage(Convert(r.GetStorage()));
			return ((HdfsProtos.StorageReportProto)builder.Build());
		}

		public static StorageReport Convert(HdfsProtos.StorageReportProto p)
		{
			return new StorageReport(p.HasStorage() ? Convert(p.GetStorage()) : new DatanodeStorage
				(p.GetStorageUuid()), p.GetFailed(), p.GetCapacity(), p.GetDfsUsed(), p.GetRemaining
				(), p.GetBlockPoolUsed());
		}

		public static StorageReport[] ConvertStorageReports(IList<HdfsProtos.StorageReportProto
			> list)
		{
			StorageReport[] report = new StorageReport[list.Count];
			for (int i = 0; i < report.Length; i++)
			{
				report[i] = Convert(list[i]);
			}
			return report;
		}

		public static IList<HdfsProtos.StorageReportProto> ConvertStorageReports(StorageReport
			[] storages)
		{
			IList<HdfsProtos.StorageReportProto> protos = new AList<HdfsProtos.StorageReportProto
				>(storages.Length);
			for (int i = 0; i < storages.Length; i++)
			{
				protos.AddItem(Convert(storages[i]));
			}
			return protos;
		}

		public static VolumeFailureSummary ConvertVolumeFailureSummary(DatanodeProtocolProtos.VolumeFailureSummaryProto
			 proto)
		{
			IList<string> failedStorageLocations = proto.GetFailedStorageLocationsList();
			return new VolumeFailureSummary(Sharpen.Collections.ToArray(failedStorageLocations
				, new string[failedStorageLocations.Count]), proto.GetLastVolumeFailureDate(), proto
				.GetEstimatedCapacityLostTotal());
		}

		public static DatanodeProtocolProtos.VolumeFailureSummaryProto ConvertVolumeFailureSummary
			(VolumeFailureSummary volumeFailureSummary)
		{
			DatanodeProtocolProtos.VolumeFailureSummaryProto.Builder builder = DatanodeProtocolProtos.VolumeFailureSummaryProto
				.NewBuilder();
			foreach (string failedStorageLocation in volumeFailureSummary.GetFailedStorageLocations
				())
			{
				builder.AddFailedStorageLocations(failedStorageLocation);
			}
			builder.SetLastVolumeFailureDate(volumeFailureSummary.GetLastVolumeFailureDate());
			builder.SetEstimatedCapacityLostTotal(volumeFailureSummary.GetEstimatedCapacityLostTotal
				());
			return ((DatanodeProtocolProtos.VolumeFailureSummaryProto)builder.Build());
		}

		public static JournalInfo Convert(JournalProtocolProtos.JournalInfoProto info)
		{
			int lv = info.HasLayoutVersion() ? info.GetLayoutVersion() : 0;
			int nsID = info.HasNamespaceID() ? info.GetNamespaceID() : 0;
			return new JournalInfo(lv, info.GetClusterID(), nsID);
		}

		/// <summary>
		/// Method used for converting
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.Proto.JournalProtocolProtos.JournalInfoProto
		/// 	"/>
		/// sent from Namenode
		/// to Journal receivers to
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.NamenodeRegistration"/>
		/// .
		/// </summary>
		public static JournalProtocolProtos.JournalInfoProto Convert(JournalInfo j)
		{
			return ((JournalProtocolProtos.JournalInfoProto)JournalProtocolProtos.JournalInfoProto
				.NewBuilder().SetClusterID(j.GetClusterId()).SetLayoutVersion(j.GetLayoutVersion
				()).SetNamespaceID(j.GetNamespaceId()).Build());
		}

		public static SnapshottableDirectoryStatus[] Convert(HdfsProtos.SnapshottableDirectoryListingProto
			 sdlp)
		{
			if (sdlp == null)
			{
				return null;
			}
			IList<HdfsProtos.SnapshottableDirectoryStatusProto> list = sdlp.GetSnapshottableDirListingList
				();
			if (list.IsEmpty())
			{
				return new SnapshottableDirectoryStatus[0];
			}
			else
			{
				SnapshottableDirectoryStatus[] result = new SnapshottableDirectoryStatus[list.Count
					];
				for (int i = 0; i < list.Count; i++)
				{
					result[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(list[i]);
				}
				return result;
			}
		}

		public static HdfsProtos.SnapshottableDirectoryListingProto Convert(SnapshottableDirectoryStatus
			[] status)
		{
			if (status == null)
			{
				return null;
			}
			HdfsProtos.SnapshottableDirectoryStatusProto[] protos = new HdfsProtos.SnapshottableDirectoryStatusProto
				[status.Length];
			for (int i = 0; i < status.Length; i++)
			{
				protos[i] = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(status[i]);
			}
			IList<HdfsProtos.SnapshottableDirectoryStatusProto> protoList = Arrays.AsList(protos
				);
			return ((HdfsProtos.SnapshottableDirectoryListingProto)HdfsProtos.SnapshottableDirectoryListingProto
				.NewBuilder().AddAllSnapshottableDirListing(protoList).Build());
		}

		public static SnapshotDiffReport.DiffReportEntry Convert(HdfsProtos.SnapshotDiffReportEntryProto
			 entry)
		{
			if (entry == null)
			{
				return null;
			}
			SnapshotDiffReport.DiffType type = SnapshotDiffReport.DiffType.GetTypeFromLabel(entry
				.GetModificationLabel());
			return type == null ? null : new SnapshotDiffReport.DiffReportEntry(type, entry.GetFullpath
				().ToByteArray(), entry.HasTargetPath() ? entry.GetTargetPath().ToByteArray() : 
				null);
		}

		public static HdfsProtos.SnapshotDiffReportEntryProto Convert(SnapshotDiffReport.DiffReportEntry
			 entry)
		{
			if (entry == null)
			{
				return null;
			}
			ByteString sourcePath = ByteString.CopyFrom(entry.GetSourcePath() == null ? DFSUtil
				.EmptyBytes : entry.GetSourcePath());
			string modification = entry.GetType().GetLabel();
			HdfsProtos.SnapshotDiffReportEntryProto.Builder builder = HdfsProtos.SnapshotDiffReportEntryProto
				.NewBuilder().SetFullpath(sourcePath).SetModificationLabel(modification);
			if (entry.GetType() == SnapshotDiffReport.DiffType.Rename)
			{
				ByteString targetPath = ByteString.CopyFrom(entry.GetTargetPath() == null ? DFSUtil
					.EmptyBytes : entry.GetTargetPath());
				builder.SetTargetPath(targetPath);
			}
			return ((HdfsProtos.SnapshotDiffReportEntryProto)builder.Build());
		}

		public static SnapshotDiffReport Convert(HdfsProtos.SnapshotDiffReportProto reportProto
			)
		{
			if (reportProto == null)
			{
				return null;
			}
			string snapshotDir = reportProto.GetSnapshotRoot();
			string fromSnapshot = reportProto.GetFromSnapshot();
			string toSnapshot = reportProto.GetToSnapshot();
			IList<HdfsProtos.SnapshotDiffReportEntryProto> list = reportProto.GetDiffReportEntriesList
				();
			IList<SnapshotDiffReport.DiffReportEntry> entries = new AList<SnapshotDiffReport.DiffReportEntry
				>();
			foreach (HdfsProtos.SnapshotDiffReportEntryProto entryProto in list)
			{
				SnapshotDiffReport.DiffReportEntry entry = Convert(entryProto);
				if (entry != null)
				{
					entries.AddItem(entry);
				}
			}
			return new SnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot, entries);
		}

		public static HdfsProtos.SnapshotDiffReportProto Convert(SnapshotDiffReport report
			)
		{
			if (report == null)
			{
				return null;
			}
			IList<SnapshotDiffReport.DiffReportEntry> entries = report.GetDiffList();
			IList<HdfsProtos.SnapshotDiffReportEntryProto> entryProtos = new AList<HdfsProtos.SnapshotDiffReportEntryProto
				>();
			foreach (SnapshotDiffReport.DiffReportEntry entry in entries)
			{
				HdfsProtos.SnapshotDiffReportEntryProto entryProto = Convert(entry);
				if (entryProto != null)
				{
					entryProtos.AddItem(entryProto);
				}
			}
			HdfsProtos.SnapshotDiffReportProto reportProto = ((HdfsProtos.SnapshotDiffReportProto
				)HdfsProtos.SnapshotDiffReportProto.NewBuilder().SetSnapshotRoot(report.GetSnapshotRoot
				()).SetFromSnapshot(report.GetFromSnapshot()).SetToSnapshot(report.GetLaterSnapshotName
				()).AddAllDiffReportEntries(entryProtos).Build());
			return reportProto;
		}

		public static DataChecksum.Type Convert(HdfsProtos.ChecksumTypeProto type)
		{
			return DataChecksum.Type.ValueOf(type.GetNumber());
		}

		public static ClientNamenodeProtocolProtos.CacheDirectiveInfoProto Convert(CacheDirectiveInfo
			 info)
		{
			ClientNamenodeProtocolProtos.CacheDirectiveInfoProto.Builder builder = ClientNamenodeProtocolProtos.CacheDirectiveInfoProto
				.NewBuilder();
			if (info.GetId() != null)
			{
				builder.SetId(info.GetId());
			}
			if (info.GetPath() != null)
			{
				builder.SetPath(info.GetPath().ToUri().GetPath());
			}
			if (info.GetReplication() != null)
			{
				builder.SetReplication(info.GetReplication());
			}
			if (info.GetPool() != null)
			{
				builder.SetPool(info.GetPool());
			}
			if (info.GetExpiration() != null)
			{
				builder.SetExpiration(Convert(info.GetExpiration()));
			}
			return ((ClientNamenodeProtocolProtos.CacheDirectiveInfoProto)builder.Build());
		}

		public static CacheDirectiveInfo Convert(ClientNamenodeProtocolProtos.CacheDirectiveInfoProto
			 proto)
		{
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();
			if (proto.HasId())
			{
				builder.SetId(proto.GetId());
			}
			if (proto.HasPath())
			{
				builder.SetPath(new Path(proto.GetPath()));
			}
			if (proto.HasReplication())
			{
				builder.SetReplication(Shorts.CheckedCast(proto.GetReplication()));
			}
			if (proto.HasPool())
			{
				builder.SetPool(proto.GetPool());
			}
			if (proto.HasExpiration())
			{
				builder.SetExpiration(Convert(proto.GetExpiration()));
			}
			return builder.Build();
		}

		public static ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto Convert
			(CacheDirectiveInfo.Expiration expiration)
		{
			return ((ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto)ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto
				.NewBuilder().SetIsRelative(expiration.IsRelative()).SetMillis(expiration.GetMillis
				()).Build());
		}

		public static CacheDirectiveInfo.Expiration Convert(ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto
			 proto)
		{
			if (proto.GetIsRelative())
			{
				return CacheDirectiveInfo.Expiration.NewRelative(proto.GetMillis());
			}
			return CacheDirectiveInfo.Expiration.NewAbsolute(proto.GetMillis());
		}

		public static ClientNamenodeProtocolProtos.CacheDirectiveStatsProto Convert(CacheDirectiveStats
			 stats)
		{
			ClientNamenodeProtocolProtos.CacheDirectiveStatsProto.Builder builder = ClientNamenodeProtocolProtos.CacheDirectiveStatsProto
				.NewBuilder();
			builder.SetBytesNeeded(stats.GetBytesNeeded());
			builder.SetBytesCached(stats.GetBytesCached());
			builder.SetFilesNeeded(stats.GetFilesNeeded());
			builder.SetFilesCached(stats.GetFilesCached());
			builder.SetHasExpired(stats.HasExpired());
			return ((ClientNamenodeProtocolProtos.CacheDirectiveStatsProto)builder.Build());
		}

		public static CacheDirectiveStats Convert(ClientNamenodeProtocolProtos.CacheDirectiveStatsProto
			 proto)
		{
			CacheDirectiveStats.Builder builder = new CacheDirectiveStats.Builder();
			builder.SetBytesNeeded(proto.GetBytesNeeded());
			builder.SetBytesCached(proto.GetBytesCached());
			builder.SetFilesNeeded(proto.GetFilesNeeded());
			builder.SetFilesCached(proto.GetFilesCached());
			builder.SetHasExpired(proto.GetHasExpired());
			return builder.Build();
		}

		public static ClientNamenodeProtocolProtos.CacheDirectiveEntryProto Convert(CacheDirectiveEntry
			 entry)
		{
			ClientNamenodeProtocolProtos.CacheDirectiveEntryProto.Builder builder = ClientNamenodeProtocolProtos.CacheDirectiveEntryProto
				.NewBuilder();
			builder.SetInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(entry.GetInfo(
				)));
			builder.SetStats(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(entry.GetStats
				()));
			return ((ClientNamenodeProtocolProtos.CacheDirectiveEntryProto)builder.Build());
		}

		public static CacheDirectiveEntry Convert(ClientNamenodeProtocolProtos.CacheDirectiveEntryProto
			 proto)
		{
			CacheDirectiveInfo info = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto
				.GetInfo());
			CacheDirectiveStats stats = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto
				.GetStats());
			return new CacheDirectiveEntry(info, stats);
		}

		public static ClientNamenodeProtocolProtos.CachePoolInfoProto Convert(CachePoolInfo
			 info)
		{
			ClientNamenodeProtocolProtos.CachePoolInfoProto.Builder builder = ClientNamenodeProtocolProtos.CachePoolInfoProto
				.NewBuilder();
			builder.SetPoolName(info.GetPoolName());
			if (info.GetOwnerName() != null)
			{
				builder.SetOwnerName(info.GetOwnerName());
			}
			if (info.GetGroupName() != null)
			{
				builder.SetGroupName(info.GetGroupName());
			}
			if (info.GetMode() != null)
			{
				builder.SetMode(info.GetMode().ToShort());
			}
			if (info.GetLimit() != null)
			{
				builder.SetLimit(info.GetLimit());
			}
			if (info.GetMaxRelativeExpiryMs() != null)
			{
				builder.SetMaxRelativeExpiry(info.GetMaxRelativeExpiryMs());
			}
			return ((ClientNamenodeProtocolProtos.CachePoolInfoProto)builder.Build());
		}

		public static CachePoolInfo Convert(ClientNamenodeProtocolProtos.CachePoolInfoProto
			 proto)
		{
			// Pool name is a required field, the rest are optional
			string poolName = Preconditions.CheckNotNull(proto.GetPoolName());
			CachePoolInfo info = new CachePoolInfo(poolName);
			if (proto.HasOwnerName())
			{
				info.SetOwnerName(proto.GetOwnerName());
			}
			if (proto.HasGroupName())
			{
				info.SetGroupName(proto.GetGroupName());
			}
			if (proto.HasMode())
			{
				info.SetMode(new FsPermission((short)proto.GetMode()));
			}
			if (proto.HasLimit())
			{
				info.SetLimit(proto.GetLimit());
			}
			if (proto.HasMaxRelativeExpiry())
			{
				info.SetMaxRelativeExpiryMs(proto.GetMaxRelativeExpiry());
			}
			return info;
		}

		public static ClientNamenodeProtocolProtos.CachePoolStatsProto Convert(CachePoolStats
			 stats)
		{
			ClientNamenodeProtocolProtos.CachePoolStatsProto.Builder builder = ClientNamenodeProtocolProtos.CachePoolStatsProto
				.NewBuilder();
			builder.SetBytesNeeded(stats.GetBytesNeeded());
			builder.SetBytesCached(stats.GetBytesCached());
			builder.SetBytesOverlimit(stats.GetBytesOverlimit());
			builder.SetFilesNeeded(stats.GetFilesNeeded());
			builder.SetFilesCached(stats.GetFilesCached());
			return ((ClientNamenodeProtocolProtos.CachePoolStatsProto)builder.Build());
		}

		public static CachePoolStats Convert(ClientNamenodeProtocolProtos.CachePoolStatsProto
			 proto)
		{
			CachePoolStats.Builder builder = new CachePoolStats.Builder();
			builder.SetBytesNeeded(proto.GetBytesNeeded());
			builder.SetBytesCached(proto.GetBytesCached());
			builder.SetBytesOverlimit(proto.GetBytesOverlimit());
			builder.SetFilesNeeded(proto.GetFilesNeeded());
			builder.SetFilesCached(proto.GetFilesCached());
			return builder.Build();
		}

		public static ClientNamenodeProtocolProtos.CachePoolEntryProto Convert(CachePoolEntry
			 entry)
		{
			ClientNamenodeProtocolProtos.CachePoolEntryProto.Builder builder = ClientNamenodeProtocolProtos.CachePoolEntryProto
				.NewBuilder();
			builder.SetInfo(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(entry.GetInfo(
				)));
			builder.SetStats(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(entry.GetStats
				()));
			return ((ClientNamenodeProtocolProtos.CachePoolEntryProto)builder.Build());
		}

		public static CachePoolEntry Convert(ClientNamenodeProtocolProtos.CachePoolEntryProto
			 proto)
		{
			CachePoolInfo info = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetInfo
				());
			CachePoolStats stats = Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert(proto.GetStats
				());
			return new CachePoolEntry(info, stats);
		}

		public static HdfsProtos.ChecksumTypeProto Convert(DataChecksum.Type type)
		{
			return HdfsProtos.ChecksumTypeProto.ValueOf(type.id);
		}

		public static HdfsProtos.DatanodeLocalInfoProto Convert(DatanodeLocalInfo info)
		{
			HdfsProtos.DatanodeLocalInfoProto.Builder builder = HdfsProtos.DatanodeLocalInfoProto
				.NewBuilder();
			builder.SetSoftwareVersion(info.GetSoftwareVersion());
			builder.SetConfigVersion(info.GetConfigVersion());
			builder.SetUptime(info.GetUptime());
			return ((HdfsProtos.DatanodeLocalInfoProto)builder.Build());
		}

		public static DatanodeLocalInfo Convert(HdfsProtos.DatanodeLocalInfoProto proto)
		{
			return new DatanodeLocalInfo(proto.GetSoftwareVersion(), proto.GetConfigVersion()
				, proto.GetUptime());
		}

		/// <exception cref="System.IO.IOException"/>
		public static InputStream VintPrefixed(InputStream input)
		{
			int firstByte = input.Read();
			if (firstByte == -1)
			{
				throw new EOFException("Premature EOF: no length prefix available");
			}
			int size = CodedInputStream.ReadRawVarint32(firstByte, input);
			System.Diagnostics.Debug.Assert(size >= 0);
			return new ExactSizeInputStream(input, size);
		}

		private static AclProtos.AclEntryProto.AclEntryScopeProto Convert(AclEntryScope v
			)
		{
			return AclProtos.AclEntryProto.AclEntryScopeProto.ValueOf((int)(v));
		}

		private static AclEntryScope Convert(AclProtos.AclEntryProto.AclEntryScopeProto v
			)
		{
			return CastEnum(v, AclEntryScopeValues);
		}

		private static AclProtos.AclEntryProto.AclEntryTypeProto Convert(AclEntryType e)
		{
			return AclProtos.AclEntryProto.AclEntryTypeProto.ValueOf((int)(e));
		}

		private static AclEntryType Convert(AclProtos.AclEntryProto.AclEntryTypeProto v)
		{
			return CastEnum(v, AclEntryTypeValues);
		}

		private static XAttrProtos.XAttrProto.XAttrNamespaceProto Convert(XAttr.NameSpace
			 v)
		{
			return XAttrProtos.XAttrProto.XAttrNamespaceProto.ValueOf((int)(v));
		}

		private static XAttr.NameSpace Convert(XAttrProtos.XAttrProto.XAttrNamespaceProto
			 v)
		{
			return CastEnum(v, XattrNamespaceValues);
		}

		public static AclProtos.AclEntryProto.FsActionProto Convert(FsAction v)
		{
			return AclProtos.AclEntryProto.FsActionProto.ValueOf(v != null ? (int)(v) : 0);
		}

		public static FsAction Convert(AclProtos.AclEntryProto.FsActionProto v)
		{
			return CastEnum(v, FsactionValues);
		}

		public static IList<AclProtos.AclEntryProto> ConvertAclEntryProto(IList<AclEntry>
			 aclSpec)
		{
			AList<AclProtos.AclEntryProto> r = Lists.NewArrayListWithCapacity(aclSpec.Count);
			foreach (AclEntry e in aclSpec)
			{
				AclProtos.AclEntryProto.Builder builder = AclProtos.AclEntryProto.NewBuilder();
				builder.SetType(Convert(e.GetType()));
				builder.SetScope(Convert(e.GetScope()));
				builder.SetPermissions(Convert(e.GetPermission()));
				if (e.GetName() != null)
				{
					builder.SetName(e.GetName());
				}
				r.AddItem(((AclProtos.AclEntryProto)builder.Build()));
			}
			return r;
		}

		public static IList<AclEntry> ConvertAclEntry(IList<AclProtos.AclEntryProto> aclSpec
			)
		{
			AList<AclEntry> r = Lists.NewArrayListWithCapacity(aclSpec.Count);
			foreach (AclProtos.AclEntryProto e in aclSpec)
			{
				AclEntry.Builder builder = new AclEntry.Builder();
				builder.SetType(Convert(e.GetType()));
				builder.SetScope(Convert(e.GetScope()));
				builder.SetPermission(Convert(e.GetPermissions()));
				if (e.HasName())
				{
					builder.SetName(e.GetName());
				}
				r.AddItem(builder.Build());
			}
			return r;
		}

		public static AclStatus Convert(AclProtos.GetAclStatusResponseProto e)
		{
			AclProtos.AclStatusProto r = e.GetResult();
			AclStatus.Builder builder = new AclStatus.Builder();
			builder.Owner(r.GetOwner()).Group(r.GetGroup()).StickyBit(r.GetSticky()).AddEntries
				(ConvertAclEntry(r.GetEntriesList()));
			if (r.HasPermission())
			{
				builder.SetPermission(Convert(r.GetPermission()));
			}
			return builder.Build();
		}

		public static AclProtos.GetAclStatusResponseProto Convert(AclStatus e)
		{
			AclProtos.AclStatusProto.Builder builder = AclProtos.AclStatusProto.NewBuilder();
			builder.SetOwner(e.GetOwner()).SetGroup(e.GetGroup()).SetSticky(e.IsStickyBit()).
				AddAllEntries(ConvertAclEntryProto(e.GetEntries()));
			if (e.GetPermission() != null)
			{
				builder.SetPermission(Convert(e.GetPermission()));
			}
			AclProtos.AclStatusProto r = ((AclProtos.AclStatusProto)builder.Build());
			return ((AclProtos.GetAclStatusResponseProto)AclProtos.GetAclStatusResponseProto.
				NewBuilder().SetResult(r).Build());
		}

		public static XAttrProtos.XAttrProto ConvertXAttrProto(XAttr a)
		{
			XAttrProtos.XAttrProto.Builder builder = XAttrProtos.XAttrProto.NewBuilder();
			builder.SetNamespace(Convert(a.GetNameSpace()));
			if (a.GetName() != null)
			{
				builder.SetName(a.GetName());
			}
			if (a.GetValue() != null)
			{
				builder.SetValue(GetByteString(a.GetValue()));
			}
			return ((XAttrProtos.XAttrProto)builder.Build());
		}

		public static IList<XAttrProtos.XAttrProto> ConvertXAttrProto(IList<XAttr> xAttrSpec
			)
		{
			if (xAttrSpec == null)
			{
				return Lists.NewArrayListWithCapacity(0);
			}
			AList<XAttrProtos.XAttrProto> xAttrs = Lists.NewArrayListWithCapacity(xAttrSpec.Count
				);
			foreach (XAttr a in xAttrSpec)
			{
				XAttrProtos.XAttrProto.Builder builder = XAttrProtos.XAttrProto.NewBuilder();
				builder.SetNamespace(Convert(a.GetNameSpace()));
				if (a.GetName() != null)
				{
					builder.SetName(a.GetName());
				}
				if (a.GetValue() != null)
				{
					builder.SetValue(GetByteString(a.GetValue()));
				}
				xAttrs.AddItem(((XAttrProtos.XAttrProto)builder.Build()));
			}
			return xAttrs;
		}

		/// <summary>
		/// The flag field in PB is a bitmask whose values are the same a the
		/// emum values of XAttrSetFlag
		/// </summary>
		public static int Convert(EnumSet<XAttrSetFlag> flag)
		{
			int value = 0;
			if (flag.Contains(XAttrSetFlag.Create))
			{
				value |= XAttrProtos.XAttrSetFlagProto.XattrCreate.GetNumber();
			}
			if (flag.Contains(XAttrSetFlag.Replace))
			{
				value |= XAttrProtos.XAttrSetFlagProto.XattrReplace.GetNumber();
			}
			return value;
		}

		public static EnumSet<XAttrSetFlag> Convert(int flag)
		{
			EnumSet<XAttrSetFlag> result = EnumSet.NoneOf<XAttrSetFlag>();
			if ((flag & XAttrProtos.XAttrSetFlagProto.XattrCreateValue) == XAttrProtos.XAttrSetFlagProto
				.XattrCreateValue)
			{
				result.AddItem(XAttrSetFlag.Create);
			}
			if ((flag & XAttrProtos.XAttrSetFlagProto.XattrReplaceValue) == XAttrProtos.XAttrSetFlagProto
				.XattrReplaceValue)
			{
				result.AddItem(XAttrSetFlag.Replace);
			}
			return result;
		}

		public static XAttr ConvertXAttr(XAttrProtos.XAttrProto a)
		{
			XAttr.Builder builder = new XAttr.Builder();
			builder.SetNameSpace(Convert(a.GetNamespace()));
			if (a.HasName())
			{
				builder.SetName(a.GetName());
			}
			if (a.HasValue())
			{
				builder.SetValue(a.GetValue().ToByteArray());
			}
			return builder.Build();
		}

		public static IList<XAttr> ConvertXAttrs(IList<XAttrProtos.XAttrProto> xAttrSpec)
		{
			AList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(xAttrSpec.Count);
			foreach (XAttrProtos.XAttrProto a in xAttrSpec)
			{
				XAttr.Builder builder = new XAttr.Builder();
				builder.SetNameSpace(Convert(a.GetNamespace()));
				if (a.HasName())
				{
					builder.SetName(a.GetName());
				}
				if (a.HasValue())
				{
					builder.SetValue(a.GetValue().ToByteArray());
				}
				xAttrs.AddItem(builder.Build());
			}
			return xAttrs;
		}

		public static IList<XAttr> Convert(XAttrProtos.GetXAttrsResponseProto a)
		{
			IList<XAttrProtos.XAttrProto> xAttrs = a.GetXAttrsList();
			return ConvertXAttrs(xAttrs);
		}

		public static XAttrProtos.GetXAttrsResponseProto ConvertXAttrsResponse(IList<XAttr
			> xAttrs)
		{
			XAttrProtos.GetXAttrsResponseProto.Builder builder = XAttrProtos.GetXAttrsResponseProto
				.NewBuilder();
			if (xAttrs != null)
			{
				builder.AddAllXAttrs(ConvertXAttrProto(xAttrs));
			}
			return ((XAttrProtos.GetXAttrsResponseProto)builder.Build());
		}

		public static IList<XAttr> Convert(XAttrProtos.ListXAttrsResponseProto a)
		{
			IList<XAttrProtos.XAttrProto> xAttrs = a.GetXAttrsList();
			return ConvertXAttrs(xAttrs);
		}

		public static XAttrProtos.ListXAttrsResponseProto ConvertListXAttrsResponse(IList
			<XAttr> names)
		{
			XAttrProtos.ListXAttrsResponseProto.Builder builder = XAttrProtos.ListXAttrsResponseProto
				.NewBuilder();
			if (names != null)
			{
				builder.AddAllXAttrs(ConvertXAttrProto(names));
			}
			return ((XAttrProtos.ListXAttrsResponseProto)builder.Build());
		}

		public static EncryptionZonesProtos.EncryptionZoneProto Convert(EncryptionZone zone
			)
		{
			return ((EncryptionZonesProtos.EncryptionZoneProto)EncryptionZonesProtos.EncryptionZoneProto
				.NewBuilder().SetId(zone.GetId()).SetPath(zone.GetPath()).SetSuite(Convert(zone.
				GetSuite())).SetCryptoProtocolVersion(Convert(zone.GetVersion())).SetKeyName(zone
				.GetKeyName()).Build());
		}

		public static EncryptionZone Convert(EncryptionZonesProtos.EncryptionZoneProto proto
			)
		{
			return new EncryptionZone(proto.GetId(), proto.GetPath(), Convert(proto.GetSuite(
				)), Convert(proto.GetCryptoProtocolVersion()), proto.GetKeyName());
		}

		public static DataTransferProtos.ShortCircuitShmSlotProto Convert(ShortCircuitShm.SlotId
			 slotId)
		{
			return ((DataTransferProtos.ShortCircuitShmSlotProto)DataTransferProtos.ShortCircuitShmSlotProto
				.NewBuilder().SetShmId(Convert(slotId.GetShmId())).SetSlotIdx(slotId.GetSlotIdx(
				)).Build());
		}

		public static DataTransferProtos.ShortCircuitShmIdProto Convert(ShortCircuitShm.ShmId
			 shmId)
		{
			return ((DataTransferProtos.ShortCircuitShmIdProto)DataTransferProtos.ShortCircuitShmIdProto
				.NewBuilder().SetHi(shmId.GetHi()).SetLo(shmId.GetLo()).Build());
		}

		public static ShortCircuitShm.SlotId Convert(DataTransferProtos.ShortCircuitShmSlotProto
			 slotId)
		{
			return new ShortCircuitShm.SlotId(Org.Apache.Hadoop.Hdfs.ProtocolPB.PBHelper.Convert
				(slotId.GetShmId()), slotId.GetSlotIdx());
		}

		public static ShortCircuitShm.ShmId Convert(DataTransferProtos.ShortCircuitShmIdProto
			 shmId)
		{
			return new ShortCircuitShm.ShmId(shmId.GetHi(), shmId.GetLo());
		}

		private static Event.CreateEvent.INodeType CreateTypeConvert(InotifyProtos.INodeType
			 type)
		{
			switch (type)
			{
				case InotifyProtos.INodeType.ITypeDirectory:
				{
					return Event.CreateEvent.INodeType.Directory;
				}

				case InotifyProtos.INodeType.ITypeFile:
				{
					return Event.CreateEvent.INodeType.File;
				}

				case InotifyProtos.INodeType.ITypeSymlink:
				{
					return Event.CreateEvent.INodeType.Symlink;
				}

				default:
				{
					return null;
				}
			}
		}

		private static InotifyProtos.MetadataUpdateType MetadataUpdateTypeConvert(Event.MetadataUpdateEvent.MetadataType
			 type)
		{
			switch (type)
			{
				case Event.MetadataUpdateEvent.MetadataType.Times:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypeTimes;
				}

				case Event.MetadataUpdateEvent.MetadataType.Replication:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypeReplication;
				}

				case Event.MetadataUpdateEvent.MetadataType.Owner:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypeOwner;
				}

				case Event.MetadataUpdateEvent.MetadataType.Perms:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypePerms;
				}

				case Event.MetadataUpdateEvent.MetadataType.Acls:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypeAcls;
				}

				case Event.MetadataUpdateEvent.MetadataType.Xattrs:
				{
					return InotifyProtos.MetadataUpdateType.MetaTypeXattrs;
				}

				default:
				{
					return null;
				}
			}
		}

		private static Event.MetadataUpdateEvent.MetadataType MetadataUpdateTypeConvert(InotifyProtos.MetadataUpdateType
			 type)
		{
			switch (type)
			{
				case InotifyProtos.MetadataUpdateType.MetaTypeTimes:
				{
					return Event.MetadataUpdateEvent.MetadataType.Times;
				}

				case InotifyProtos.MetadataUpdateType.MetaTypeReplication:
				{
					return Event.MetadataUpdateEvent.MetadataType.Replication;
				}

				case InotifyProtos.MetadataUpdateType.MetaTypeOwner:
				{
					return Event.MetadataUpdateEvent.MetadataType.Owner;
				}

				case InotifyProtos.MetadataUpdateType.MetaTypePerms:
				{
					return Event.MetadataUpdateEvent.MetadataType.Perms;
				}

				case InotifyProtos.MetadataUpdateType.MetaTypeAcls:
				{
					return Event.MetadataUpdateEvent.MetadataType.Acls;
				}

				case InotifyProtos.MetadataUpdateType.MetaTypeXattrs:
				{
					return Event.MetadataUpdateEvent.MetadataType.Xattrs;
				}

				default:
				{
					return null;
				}
			}
		}

		private static InotifyProtos.INodeType CreateTypeConvert(Event.CreateEvent.INodeType
			 type)
		{
			switch (type)
			{
				case Event.CreateEvent.INodeType.Directory:
				{
					return InotifyProtos.INodeType.ITypeDirectory;
				}

				case Event.CreateEvent.INodeType.File:
				{
					return InotifyProtos.INodeType.ITypeFile;
				}

				case Event.CreateEvent.INodeType.Symlink:
				{
					return InotifyProtos.INodeType.ITypeSymlink;
				}

				default:
				{
					return null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static EventBatchList Convert(ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto
			 resp)
		{
			InotifyProtos.EventsListProto list = resp.GetEventsList();
			long firstTxid = list.GetFirstTxid();
			long lastTxid = list.GetLastTxid();
			IList<EventBatch> batches = Lists.NewArrayList();
			if (list.GetEventsList().Count > 0)
			{
				throw new IOException("Can't handle old inotify server response.");
			}
			foreach (InotifyProtos.EventBatchProto bp in list.GetBatchList())
			{
				long txid = bp.GetTxid();
				if ((txid != -1) && ((txid < firstTxid) || (txid > lastTxid)))
				{
					throw new IOException("Error converting TxidResponseProto: got a " + "transaction id "
						 + txid + " that was outside the range of [" + firstTxid + ", " + lastTxid + "]."
						);
				}
				IList<Event> events = Lists.NewArrayList();
				foreach (InotifyProtos.EventProto p in bp.GetEventsList())
				{
					switch (p.GetType())
					{
						case InotifyProtos.EventType.EventClose:
						{
							InotifyProtos.CloseEventProto close = InotifyProtos.CloseEventProto.ParseFrom(p.GetContents
								());
							events.AddItem(new Event.CloseEvent(close.GetPath(), close.GetFileSize(), close.GetTimestamp
								()));
							break;
						}

						case InotifyProtos.EventType.EventCreate:
						{
							InotifyProtos.CreateEventProto create = InotifyProtos.CreateEventProto.ParseFrom(
								p.GetContents());
							events.AddItem(new Event.CreateEvent.Builder().INodeType(CreateTypeConvert(create
								.GetType())).Path(create.GetPath()).Ctime(create.GetCtime()).OwnerName(create.GetOwnerName
								()).GroupName(create.GetGroupName()).Perms(Convert(create.GetPerms())).Replication
								(create.GetReplication()).SymlinkTarget(create.GetSymlinkTarget().IsEmpty() ? null
								 : create.GetSymlinkTarget()).DefaultBlockSize(create.GetDefaultBlockSize()).Overwrite
								(create.GetOverwrite()).Build());
							break;
						}

						case InotifyProtos.EventType.EventMetadata:
						{
							InotifyProtos.MetadataUpdateEventProto meta = InotifyProtos.MetadataUpdateEventProto
								.ParseFrom(p.GetContents());
							events.AddItem(new Event.MetadataUpdateEvent.Builder().Path(meta.GetPath()).MetadataType
								(MetadataUpdateTypeConvert(meta.GetType())).Mtime(meta.GetMtime()).Atime(meta.GetAtime
								()).Replication(meta.GetReplication()).OwnerName(meta.GetOwnerName().IsEmpty() ? 
								null : meta.GetOwnerName()).GroupName(meta.GetGroupName().IsEmpty() ? null : meta
								.GetGroupName()).Perms(meta.HasPerms() ? Convert(meta.GetPerms()) : null).Acls(meta
								.GetAclsList().IsEmpty() ? null : ConvertAclEntry(meta.GetAclsList())).XAttrs(meta
								.GetXAttrsList().IsEmpty() ? null : ConvertXAttrs(meta.GetXAttrsList())).XAttrsRemoved
								(meta.GetXAttrsRemoved()).Build());
							break;
						}

						case InotifyProtos.EventType.EventRename:
						{
							InotifyProtos.RenameEventProto rename = InotifyProtos.RenameEventProto.ParseFrom(
								p.GetContents());
							events.AddItem(new Event.RenameEvent.Builder().SrcPath(rename.GetSrcPath()).DstPath
								(rename.GetDestPath()).Timestamp(rename.GetTimestamp()).Build());
							break;
						}

						case InotifyProtos.EventType.EventAppend:
						{
							InotifyProtos.AppendEventProto append = InotifyProtos.AppendEventProto.ParseFrom(
								p.GetContents());
							events.AddItem(new Event.AppendEvent.Builder().Path(append.GetPath()).NewBlock(append
								.HasNewBlock() && append.GetNewBlock()).Build());
							break;
						}

						case InotifyProtos.EventType.EventUnlink:
						{
							InotifyProtos.UnlinkEventProto unlink = InotifyProtos.UnlinkEventProto.ParseFrom(
								p.GetContents());
							events.AddItem(new Event.UnlinkEvent.Builder().Path(unlink.GetPath()).Timestamp(unlink
								.GetTimestamp()).Build());
							break;
						}

						default:
						{
							throw new RuntimeException("Unexpected inotify event type: " + p.GetType());
						}
					}
				}
				batches.AddItem(new EventBatch(txid, Sharpen.Collections.ToArray(events, new Event
					[0])));
			}
			return new EventBatchList(batches, resp.GetEventsList().GetFirstTxid(), resp.GetEventsList
				().GetLastTxid(), resp.GetEventsList().GetSyncTxid());
		}

		public static ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto ConvertEditsResponse
			(EventBatchList el)
		{
			InotifyProtos.EventsListProto.Builder builder = InotifyProtos.EventsListProto.NewBuilder
				();
			foreach (EventBatch b in el.GetBatches())
			{
				IList<InotifyProtos.EventProto> events = Lists.NewArrayList();
				foreach (Event e in b.GetEvents())
				{
					switch (e.GetEventType())
					{
						case Event.EventType.Close:
						{
							Event.CloseEvent ce = (Event.CloseEvent)e;
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventClose).SetContents(((InotifyProtos.CloseEventProto
								)InotifyProtos.CloseEventProto.NewBuilder().SetPath(ce.GetPath()).SetFileSize(ce
								.GetFileSize()).SetTimestamp(ce.GetTimestamp()).Build()).ToByteString()).Build()
								));
							break;
						}

						case Event.EventType.Create:
						{
							Event.CreateEvent ce2 = (Event.CreateEvent)e;
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventCreate).SetContents(((InotifyProtos.CreateEventProto
								)InotifyProtos.CreateEventProto.NewBuilder().SetType(CreateTypeConvert(ce2.GetiNodeType
								())).SetPath(ce2.GetPath()).SetCtime(ce2.GetCtime()).SetOwnerName(ce2.GetOwnerName
								()).SetGroupName(ce2.GetGroupName()).SetPerms(Convert(ce2.GetPerms())).SetReplication
								(ce2.GetReplication()).SetSymlinkTarget(ce2.GetSymlinkTarget() == null ? string.Empty
								 : ce2.GetSymlinkTarget()).SetDefaultBlockSize(ce2.GetDefaultBlockSize()).SetOverwrite
								(ce2.GetOverwrite()).Build()).ToByteString()).Build()));
							break;
						}

						case Event.EventType.Metadata:
						{
							Event.MetadataUpdateEvent me = (Event.MetadataUpdateEvent)e;
							InotifyProtos.MetadataUpdateEventProto.Builder metaB = InotifyProtos.MetadataUpdateEventProto
								.NewBuilder().SetPath(me.GetPath()).SetType(MetadataUpdateTypeConvert(me.GetMetadataType
								())).SetMtime(me.GetMtime()).SetAtime(me.GetAtime()).SetReplication(me.GetReplication
								()).SetOwnerName(me.GetOwnerName() == null ? string.Empty : me.GetOwnerName()).SetGroupName
								(me.GetGroupName() == null ? string.Empty : me.GetGroupName()).AddAllAcls(me.GetAcls
								() == null ? Lists.NewArrayList<AclProtos.AclEntryProto>() : ConvertAclEntryProto
								(me.GetAcls())).AddAllXAttrs(me.GetxAttrs() == null ? Lists.NewArrayList<XAttrProtos.XAttrProto
								>() : ConvertXAttrProto(me.GetxAttrs())).SetXAttrsRemoved(me.IsxAttrsRemoved());
							if (me.GetPerms() != null)
							{
								metaB.SetPerms(Convert(me.GetPerms()));
							}
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventMetadata).SetContents(((InotifyProtos.MetadataUpdateEventProto
								)metaB.Build()).ToByteString()).Build()));
							break;
						}

						case Event.EventType.Rename:
						{
							Event.RenameEvent re = (Event.RenameEvent)e;
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventRename).SetContents(((InotifyProtos.RenameEventProto
								)InotifyProtos.RenameEventProto.NewBuilder().SetSrcPath(re.GetSrcPath()).SetDestPath
								(re.GetDstPath()).SetTimestamp(re.GetTimestamp()).Build()).ToByteString()).Build
								()));
							break;
						}

						case Event.EventType.Append:
						{
							Event.AppendEvent re2 = (Event.AppendEvent)e;
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventAppend).SetContents(((InotifyProtos.AppendEventProto
								)InotifyProtos.AppendEventProto.NewBuilder().SetPath(re2.GetPath()).SetNewBlock(
								re2.ToNewBlock()).Build()).ToByteString()).Build()));
							break;
						}

						case Event.EventType.Unlink:
						{
							Event.UnlinkEvent ue = (Event.UnlinkEvent)e;
							events.AddItem(((InotifyProtos.EventProto)InotifyProtos.EventProto.NewBuilder().SetType
								(InotifyProtos.EventType.EventUnlink).SetContents(((InotifyProtos.UnlinkEventProto
								)InotifyProtos.UnlinkEventProto.NewBuilder().SetPath(ue.GetPath()).SetTimestamp(
								ue.GetTimestamp()).Build()).ToByteString()).Build()));
							break;
						}

						default:
						{
							throw new RuntimeException("Unexpected inotify event: " + e);
						}
					}
				}
				builder.AddBatch(InotifyProtos.EventBatchProto.NewBuilder().SetTxid(b.GetTxid()).
					AddAllEvents(events));
			}
			builder.SetFirstTxid(el.GetFirstTxid());
			builder.SetLastTxid(el.GetLastTxid());
			builder.SetSyncTxid(el.GetSyncTxid());
			return ((ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto)ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto
				.NewBuilder().SetEventsList(((InotifyProtos.EventsListProto)builder.Build())).Build
				());
		}

		public static HdfsProtos.CipherOptionProto Convert(CipherOption option)
		{
			if (option != null)
			{
				HdfsProtos.CipherOptionProto.Builder builder = HdfsProtos.CipherOptionProto.NewBuilder
					();
				if (option.GetCipherSuite() != null)
				{
					builder.SetSuite(Convert(option.GetCipherSuite()));
				}
				if (option.GetInKey() != null)
				{
					builder.SetInKey(ByteString.CopyFrom(option.GetInKey()));
				}
				if (option.GetInIv() != null)
				{
					builder.SetInIv(ByteString.CopyFrom(option.GetInIv()));
				}
				if (option.GetOutKey() != null)
				{
					builder.SetOutKey(ByteString.CopyFrom(option.GetOutKey()));
				}
				if (option.GetOutIv() != null)
				{
					builder.SetOutIv(ByteString.CopyFrom(option.GetOutIv()));
				}
				return ((HdfsProtos.CipherOptionProto)builder.Build());
			}
			return null;
		}

		public static CipherOption Convert(HdfsProtos.CipherOptionProto proto)
		{
			if (proto != null)
			{
				CipherSuite suite = null;
				if (proto.GetSuite() != null)
				{
					suite = Convert(proto.GetSuite());
				}
				byte[] inKey = null;
				if (proto.GetInKey() != null)
				{
					inKey = proto.GetInKey().ToByteArray();
				}
				byte[] inIv = null;
				if (proto.GetInIv() != null)
				{
					inIv = proto.GetInIv().ToByteArray();
				}
				byte[] outKey = null;
				if (proto.GetOutKey() != null)
				{
					outKey = proto.GetOutKey().ToByteArray();
				}
				byte[] outIv = null;
				if (proto.GetOutIv() != null)
				{
					outIv = proto.GetOutIv().ToByteArray();
				}
				return new CipherOption(suite, inKey, inIv, outKey, outIv);
			}
			return null;
		}

		public static IList<HdfsProtos.CipherOptionProto> ConvertCipherOptions(IList<CipherOption
			> options)
		{
			if (options != null)
			{
				IList<HdfsProtos.CipherOptionProto> protos = Lists.NewArrayListWithCapacity(options
					.Count);
				foreach (CipherOption option in options)
				{
					protos.AddItem(Convert(option));
				}
				return protos;
			}
			return null;
		}

		public static IList<CipherOption> ConvertCipherOptionProtos(IList<HdfsProtos.CipherOptionProto
			> protos)
		{
			if (protos != null)
			{
				IList<CipherOption> options = Lists.NewArrayListWithCapacity(protos.Count);
				foreach (HdfsProtos.CipherOptionProto proto in protos)
				{
					options.AddItem(Convert(proto));
				}
				return options;
			}
			return null;
		}

		public static HdfsProtos.CipherSuiteProto Convert(CipherSuite suite)
		{
			switch (suite)
			{
				case CipherSuite.Unknown:
				{
					return HdfsProtos.CipherSuiteProto.Unknown;
				}

				case CipherSuite.AesCtrNopadding:
				{
					return HdfsProtos.CipherSuiteProto.AesCtrNopadding;
				}

				default:
				{
					return null;
				}
			}
		}

		public static CipherSuite Convert(HdfsProtos.CipherSuiteProto proto)
		{
			switch (proto)
			{
				case HdfsProtos.CipherSuiteProto.AesCtrNopadding:
				{
					return CipherSuite.AesCtrNopadding;
				}

				default:
				{
					// Set to UNKNOWN and stash the unknown enum value
					CipherSuite suite = CipherSuite.Unknown;
					suite.SetUnknownValue(proto.GetNumber());
					return suite;
				}
			}
		}

		public static IList<HdfsProtos.CryptoProtocolVersionProto> Convert(CryptoProtocolVersion
			[] versions)
		{
			IList<HdfsProtos.CryptoProtocolVersionProto> protos = Lists.NewArrayListWithCapacity
				(versions.Length);
			foreach (CryptoProtocolVersion v in versions)
			{
				protos.AddItem(Convert(v));
			}
			return protos;
		}

		public static CryptoProtocolVersion[] ConvertCryptoProtocolVersions(IList<HdfsProtos.CryptoProtocolVersionProto
			> protos)
		{
			IList<CryptoProtocolVersion> versions = Lists.NewArrayListWithCapacity(protos.Count
				);
			foreach (HdfsProtos.CryptoProtocolVersionProto p in protos)
			{
				versions.AddItem(Convert(p));
			}
			return Sharpen.Collections.ToArray(versions, new CryptoProtocolVersion[] {  });
		}

		public static CryptoProtocolVersion Convert(HdfsProtos.CryptoProtocolVersionProto
			 proto)
		{
			switch (proto)
			{
				case HdfsProtos.CryptoProtocolVersionProto.EncryptionZones:
				{
					return CryptoProtocolVersion.EncryptionZones;
				}

				default:
				{
					// Set to UNKNOWN and stash the unknown enum value
					CryptoProtocolVersion version = CryptoProtocolVersion.Unknown;
					version.SetUnknownValue(proto.GetNumber());
					return version;
				}
			}
		}

		public static HdfsProtos.CryptoProtocolVersionProto Convert(CryptoProtocolVersion
			 version)
		{
			switch (version)
			{
				case CryptoProtocolVersion.Unknown:
				{
					return HdfsProtos.CryptoProtocolVersionProto.UnknownProtocolVersion;
				}

				case CryptoProtocolVersion.EncryptionZones:
				{
					return HdfsProtos.CryptoProtocolVersionProto.EncryptionZones;
				}

				default:
				{
					return null;
				}
			}
		}

		public static HdfsProtos.FileEncryptionInfoProto Convert(FileEncryptionInfo info)
		{
			if (info == null)
			{
				return null;
			}
			return ((HdfsProtos.FileEncryptionInfoProto)HdfsProtos.FileEncryptionInfoProto.NewBuilder
				().SetSuite(Convert(info.GetCipherSuite())).SetCryptoProtocolVersion(Convert(info
				.GetCryptoProtocolVersion())).SetKey(GetByteString(info.GetEncryptedDataEncryptionKey
				())).SetIv(GetByteString(info.GetIV())).SetEzKeyVersionName(info.GetEzKeyVersionName
				()).SetKeyName(info.GetKeyName()).Build());
		}

		public static HdfsProtos.PerFileEncryptionInfoProto ConvertPerFileEncInfo(FileEncryptionInfo
			 info)
		{
			if (info == null)
			{
				return null;
			}
			return ((HdfsProtos.PerFileEncryptionInfoProto)HdfsProtos.PerFileEncryptionInfoProto
				.NewBuilder().SetKey(GetByteString(info.GetEncryptedDataEncryptionKey())).SetIv(
				GetByteString(info.GetIV())).SetEzKeyVersionName(info.GetEzKeyVersionName()).Build
				());
		}

		public static HdfsProtos.ZoneEncryptionInfoProto Convert(CipherSuite suite, CryptoProtocolVersion
			 version, string keyName)
		{
			if (suite == null || version == null || keyName == null)
			{
				return null;
			}
			return ((HdfsProtos.ZoneEncryptionInfoProto)HdfsProtos.ZoneEncryptionInfoProto.NewBuilder
				().SetSuite(Convert(suite)).SetCryptoProtocolVersion(Convert(version)).SetKeyName
				(keyName).Build());
		}

		public static FileEncryptionInfo Convert(HdfsProtos.FileEncryptionInfoProto proto
			)
		{
			if (proto == null)
			{
				return null;
			}
			CipherSuite suite = Convert(proto.GetSuite());
			CryptoProtocolVersion version = Convert(proto.GetCryptoProtocolVersion());
			byte[] key = proto.GetKey().ToByteArray();
			byte[] iv = proto.GetIv().ToByteArray();
			string ezKeyVersionName = proto.GetEzKeyVersionName();
			string keyName = proto.GetKeyName();
			return new FileEncryptionInfo(suite, version, key, iv, keyName, ezKeyVersionName);
		}

		public static FileEncryptionInfo Convert(HdfsProtos.PerFileEncryptionInfoProto fileProto
			, CipherSuite suite, CryptoProtocolVersion version, string keyName)
		{
			if (fileProto == null || suite == null || version == null || keyName == null)
			{
				return null;
			}
			byte[] key = fileProto.GetKey().ToByteArray();
			byte[] iv = fileProto.GetIv().ToByteArray();
			string ezKeyVersionName = fileProto.GetEzKeyVersionName();
			return new FileEncryptionInfo(suite, version, key, iv, keyName, ezKeyVersionName);
		}

		public static IList<bool> Convert(bool[] targetPinnings, int idx)
		{
			IList<bool> pinnings = new AList<bool>();
			if (targetPinnings == null)
			{
				pinnings.AddItem(false);
			}
			else
			{
				for (; idx < targetPinnings.Length; ++idx)
				{
					pinnings.AddItem(Sharpen.Extensions.ValueOf(targetPinnings[idx]));
				}
			}
			return pinnings;
		}

		public static bool[] ConvertBooleanList(IList<bool> targetPinningsList)
		{
			bool[] targetPinnings = new bool[targetPinningsList.Count];
			for (int i = 0; i < targetPinningsList.Count; i++)
			{
				targetPinnings[i] = targetPinningsList[i];
			}
			return targetPinnings;
		}

		public static BlockReportContext Convert(DatanodeProtocolProtos.BlockReportContextProto
			 proto)
		{
			return new BlockReportContext(proto.GetTotalRpcs(), proto.GetCurRpc(), proto.GetId
				());
		}

		public static DatanodeProtocolProtos.BlockReportContextProto Convert(BlockReportContext
			 context)
		{
			return ((DatanodeProtocolProtos.BlockReportContextProto)DatanodeProtocolProtos.BlockReportContextProto
				.NewBuilder().SetTotalRpcs(context.GetTotalRpcs()).SetCurRpc(context.GetCurRpc()
				).SetId(context.GetReportId()).Build());
		}
	}
}
