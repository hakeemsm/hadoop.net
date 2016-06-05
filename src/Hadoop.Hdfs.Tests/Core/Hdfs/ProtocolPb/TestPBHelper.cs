using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Tests for
	/// <see cref="PBHelper"/>
	/// </summary>
	public class TestPBHelper
	{
		/// <summary>Used for asserting equality on doubles.</summary>
		private const double Delta = 0.000001;

		[NUnit.Framework.Test]
		public virtual void TestConvertNamenodeRole()
		{
			NUnit.Framework.Assert.AreEqual(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto
				.Backup, PBHelper.Convert(HdfsServerConstants.NamenodeRole.Backup));
			NUnit.Framework.Assert.AreEqual(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto
				.Checkpoint, PBHelper.Convert(HdfsServerConstants.NamenodeRole.Checkpoint));
			NUnit.Framework.Assert.AreEqual(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto
				.Namenode, PBHelper.Convert(HdfsServerConstants.NamenodeRole.Namenode));
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.NamenodeRole.Backup, PBHelper
				.Convert(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Backup));
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.NamenodeRole.Checkpoint, PBHelper
				.Convert(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Checkpoint));
			NUnit.Framework.Assert.AreEqual(HdfsServerConstants.NamenodeRole.Namenode, PBHelper
				.Convert(HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto.Namenode));
		}

		private static StorageInfo GetStorageInfo(HdfsServerConstants.NodeType type)
		{
			return new StorageInfo(1, 2, "cid", 3, type);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertStoragInfo()
		{
			StorageInfo info = GetStorageInfo(HdfsServerConstants.NodeType.NameNode);
			HdfsProtos.StorageInfoProto infoProto = PBHelper.Convert(info);
			StorageInfo info2 = PBHelper.Convert(infoProto, HdfsServerConstants.NodeType.NameNode
				);
			NUnit.Framework.Assert.AreEqual(info.GetClusterID(), info2.GetClusterID());
			NUnit.Framework.Assert.AreEqual(info.GetCTime(), info2.GetCTime());
			NUnit.Framework.Assert.AreEqual(info.GetLayoutVersion(), info2.GetLayoutVersion()
				);
			NUnit.Framework.Assert.AreEqual(info.GetNamespaceID(), info2.GetNamespaceID());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertNamenodeRegistration()
		{
			StorageInfo info = GetStorageInfo(HdfsServerConstants.NodeType.NameNode);
			NamenodeRegistration reg = new NamenodeRegistration("address:999", "http:1000", info
				, HdfsServerConstants.NamenodeRole.Namenode);
			HdfsProtos.NamenodeRegistrationProto regProto = PBHelper.Convert(reg);
			NamenodeRegistration reg2 = PBHelper.Convert(regProto);
			NUnit.Framework.Assert.AreEqual(reg.GetAddress(), reg2.GetAddress());
			NUnit.Framework.Assert.AreEqual(reg.GetClusterID(), reg2.GetClusterID());
			NUnit.Framework.Assert.AreEqual(reg.GetCTime(), reg2.GetCTime());
			NUnit.Framework.Assert.AreEqual(reg.GetHttpAddress(), reg2.GetHttpAddress());
			NUnit.Framework.Assert.AreEqual(reg.GetLayoutVersion(), reg2.GetLayoutVersion());
			NUnit.Framework.Assert.AreEqual(reg.GetNamespaceID(), reg2.GetNamespaceID());
			NUnit.Framework.Assert.AreEqual(reg.GetRegistrationID(), reg2.GetRegistrationID()
				);
			NUnit.Framework.Assert.AreEqual(reg.GetRole(), reg2.GetRole());
			NUnit.Framework.Assert.AreEqual(reg.GetVersion(), reg2.GetVersion());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertDatanodeID()
		{
			DatanodeID dn = DFSTestUtil.GetLocalDatanodeID();
			HdfsProtos.DatanodeIDProto dnProto = PBHelper.Convert(dn);
			DatanodeID dn2 = PBHelper.Convert(dnProto);
			Compare(dn, dn2);
		}

		internal virtual void Compare(DatanodeID dn, DatanodeID dn2)
		{
			NUnit.Framework.Assert.AreEqual(dn.GetIpAddr(), dn2.GetIpAddr());
			NUnit.Framework.Assert.AreEqual(dn.GetHostName(), dn2.GetHostName());
			NUnit.Framework.Assert.AreEqual(dn.GetDatanodeUuid(), dn2.GetDatanodeUuid());
			NUnit.Framework.Assert.AreEqual(dn.GetXferPort(), dn2.GetXferPort());
			NUnit.Framework.Assert.AreEqual(dn.GetInfoPort(), dn2.GetInfoPort());
			NUnit.Framework.Assert.AreEqual(dn.GetIpcPort(), dn2.GetIpcPort());
		}

		internal virtual void Compare(DatanodeStorage dns1, DatanodeStorage dns2)
		{
			Assert.AssertThat(dns2.GetStorageID(), CoreMatchers.Is(dns1.GetStorageID()));
			Assert.AssertThat(dns2.GetState(), CoreMatchers.Is(dns1.GetState()));
			Assert.AssertThat(dns2.GetStorageType(), CoreMatchers.Is(dns1.GetStorageType()));
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlock()
		{
			Block b = new Block(1, 100, 3);
			HdfsProtos.BlockProto bProto = PBHelper.Convert(b);
			Block b2 = PBHelper.Convert(bProto);
			NUnit.Framework.Assert.AreEqual(b, b2);
		}

		private static BlocksWithLocations.BlockWithLocations GetBlockWithLocations(int bid
			)
		{
			string[] datanodeUuids = new string[] { "dn1", "dn2", "dn3" };
			string[] storageIDs = new string[] { "s1", "s2", "s3" };
			StorageType[] storageTypes = new StorageType[] { StorageType.Disk, StorageType.Disk
				, StorageType.Disk };
			return new BlocksWithLocations.BlockWithLocations(new Block(bid, 0, 1), datanodeUuids
				, storageIDs, storageTypes);
		}

		private void Compare(BlocksWithLocations.BlockWithLocations locs1, BlocksWithLocations.BlockWithLocations
			 locs2)
		{
			NUnit.Framework.Assert.AreEqual(locs1.GetBlock(), locs2.GetBlock());
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(locs1.GetStorageIDs(), locs2.GetStorageIDs
				()));
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlockWithLocations()
		{
			BlocksWithLocations.BlockWithLocations locs = GetBlockWithLocations(1);
			HdfsProtos.BlockWithLocationsProto locsProto = PBHelper.Convert(locs);
			BlocksWithLocations.BlockWithLocations locs2 = PBHelper.Convert(locsProto);
			Compare(locs, locs2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlocksWithLocations()
		{
			BlocksWithLocations.BlockWithLocations[] list = new BlocksWithLocations.BlockWithLocations
				[] { GetBlockWithLocations(1), GetBlockWithLocations(2) };
			BlocksWithLocations locs = new BlocksWithLocations(list);
			HdfsProtos.BlocksWithLocationsProto locsProto = PBHelper.Convert(locs);
			BlocksWithLocations locs2 = PBHelper.Convert(locsProto);
			BlocksWithLocations.BlockWithLocations[] blocks = locs.GetBlocks();
			BlocksWithLocations.BlockWithLocations[] blocks2 = locs2.GetBlocks();
			NUnit.Framework.Assert.AreEqual(blocks.Length, blocks2.Length);
			for (int i = 0; i < blocks.Length; i++)
			{
				Compare(blocks[i], blocks2[i]);
			}
		}

		private static BlockKey GetBlockKey(int keyId)
		{
			return new BlockKey(keyId, 10, Sharpen.Runtime.GetBytesForString("encodedKey"));
		}

		private void Compare(BlockKey k1, BlockKey k2)
		{
			NUnit.Framework.Assert.AreEqual(k1.GetExpiryDate(), k2.GetExpiryDate());
			NUnit.Framework.Assert.AreEqual(k1.GetKeyId(), k2.GetKeyId());
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(k1.GetEncodedKey(), k2.GetEncodedKey(
				)));
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlockKey()
		{
			BlockKey key = GetBlockKey(1);
			HdfsProtos.BlockKeyProto keyProto = PBHelper.Convert(key);
			BlockKey key1 = PBHelper.Convert(keyProto);
			Compare(key, key1);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertExportedBlockKeys()
		{
			BlockKey[] keys = new BlockKey[] { GetBlockKey(2), GetBlockKey(3) };
			ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10, GetBlockKey(1), keys
				);
			HdfsProtos.ExportedBlockKeysProto expKeysProto = PBHelper.Convert(expKeys);
			ExportedBlockKeys expKeys1 = PBHelper.Convert(expKeysProto);
			Compare(expKeys, expKeys1);
		}

		internal virtual void Compare(ExportedBlockKeys expKeys, ExportedBlockKeys expKeys1
			)
		{
			BlockKey[] allKeys = expKeys.GetAllKeys();
			BlockKey[] allKeys1 = expKeys1.GetAllKeys();
			NUnit.Framework.Assert.AreEqual(allKeys.Length, allKeys1.Length);
			for (int i = 0; i < allKeys.Length; i++)
			{
				Compare(allKeys[i], allKeys1[i]);
			}
			Compare(expKeys.GetCurrentKey(), expKeys1.GetCurrentKey());
			NUnit.Framework.Assert.AreEqual(expKeys.GetKeyUpdateInterval(), expKeys1.GetKeyUpdateInterval
				());
			NUnit.Framework.Assert.AreEqual(expKeys.GetTokenLifetime(), expKeys1.GetTokenLifetime
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertCheckpointSignature()
		{
			CheckpointSignature s = new CheckpointSignature(GetStorageInfo(HdfsServerConstants.NodeType
				.NameNode), "bpid", 100, 1);
			HdfsProtos.CheckpointSignatureProto sProto = PBHelper.Convert(s);
			CheckpointSignature s1 = PBHelper.Convert(sProto);
			NUnit.Framework.Assert.AreEqual(s.GetBlockpoolID(), s1.GetBlockpoolID());
			NUnit.Framework.Assert.AreEqual(s.GetClusterID(), s1.GetClusterID());
			NUnit.Framework.Assert.AreEqual(s.GetCTime(), s1.GetCTime());
			NUnit.Framework.Assert.AreEqual(s.GetCurSegmentTxId(), s1.GetCurSegmentTxId());
			NUnit.Framework.Assert.AreEqual(s.GetLayoutVersion(), s1.GetLayoutVersion());
			NUnit.Framework.Assert.AreEqual(s.GetMostRecentCheckpointTxId(), s1.GetMostRecentCheckpointTxId
				());
			NUnit.Framework.Assert.AreEqual(s.GetNamespaceID(), s1.GetNamespaceID());
		}

		private static void Compare(RemoteEditLog l1, RemoteEditLog l2)
		{
			NUnit.Framework.Assert.AreEqual(l1.GetEndTxId(), l2.GetEndTxId());
			NUnit.Framework.Assert.AreEqual(l1.GetStartTxId(), l2.GetStartTxId());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertRemoteEditLog()
		{
			RemoteEditLog l = new RemoteEditLog(1, 100);
			HdfsProtos.RemoteEditLogProto lProto = PBHelper.Convert(l);
			RemoteEditLog l1 = PBHelper.Convert(lProto);
			Compare(l, l1);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertRemoteEditLogManifest()
		{
			IList<RemoteEditLog> logs = new AList<RemoteEditLog>();
			logs.AddItem(new RemoteEditLog(1, 10));
			logs.AddItem(new RemoteEditLog(11, 20));
			RemoteEditLogManifest m = new RemoteEditLogManifest(logs);
			HdfsProtos.RemoteEditLogManifestProto mProto = PBHelper.Convert(m);
			RemoteEditLogManifest m1 = PBHelper.Convert(mProto);
			IList<RemoteEditLog> logs1 = m1.GetLogs();
			NUnit.Framework.Assert.AreEqual(logs.Count, logs1.Count);
			for (int i = 0; i < logs.Count; i++)
			{
				Compare(logs[i], logs1[i]);
			}
		}

		public virtual ExtendedBlock GetExtendedBlock()
		{
			return GetExtendedBlock(1);
		}

		public virtual ExtendedBlock GetExtendedBlock(long blkid)
		{
			return new ExtendedBlock("bpid", blkid, 100, 2);
		}

		private void Compare(DatanodeInfo dn1, DatanodeInfo dn2)
		{
			NUnit.Framework.Assert.AreEqual(dn1.GetAdminState(), dn2.GetAdminState());
			NUnit.Framework.Assert.AreEqual(dn1.GetBlockPoolUsed(), dn2.GetBlockPoolUsed());
			NUnit.Framework.Assert.AreEqual(dn1.GetBlockPoolUsedPercent(), dn2.GetBlockPoolUsedPercent
				(), Delta);
			NUnit.Framework.Assert.AreEqual(dn1.GetCapacity(), dn2.GetCapacity());
			NUnit.Framework.Assert.AreEqual(dn1.GetDatanodeReport(), dn2.GetDatanodeReport());
			NUnit.Framework.Assert.AreEqual(dn1.GetDfsUsed(), dn1.GetDfsUsed());
			NUnit.Framework.Assert.AreEqual(dn1.GetDfsUsedPercent(), dn1.GetDfsUsedPercent(), 
				Delta);
			NUnit.Framework.Assert.AreEqual(dn1.GetIpAddr(), dn2.GetIpAddr());
			NUnit.Framework.Assert.AreEqual(dn1.GetHostName(), dn2.GetHostName());
			NUnit.Framework.Assert.AreEqual(dn1.GetInfoPort(), dn2.GetInfoPort());
			NUnit.Framework.Assert.AreEqual(dn1.GetIpcPort(), dn2.GetIpcPort());
			NUnit.Framework.Assert.AreEqual(dn1.GetLastUpdate(), dn2.GetLastUpdate());
			NUnit.Framework.Assert.AreEqual(dn1.GetLevel(), dn2.GetLevel());
			NUnit.Framework.Assert.AreEqual(dn1.GetNetworkLocation(), dn2.GetNetworkLocation(
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertExtendedBlock()
		{
			ExtendedBlock b = GetExtendedBlock();
			HdfsProtos.ExtendedBlockProto bProto = PBHelper.Convert(b);
			ExtendedBlock b1 = PBHelper.Convert(bProto);
			NUnit.Framework.Assert.AreEqual(b, b1);
			b.SetBlockId(-1);
			bProto = PBHelper.Convert(b);
			b1 = PBHelper.Convert(bProto);
			NUnit.Framework.Assert.AreEqual(b, b1);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertRecoveringBlock()
		{
			DatanodeInfo di1 = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo di2 = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };
			BlockRecoveryCommand.RecoveringBlock b = new BlockRecoveryCommand.RecoveringBlock
				(GetExtendedBlock(), dnInfo, 3);
			HdfsProtos.RecoveringBlockProto bProto = PBHelper.Convert(b);
			BlockRecoveryCommand.RecoveringBlock b1 = PBHelper.Convert(bProto);
			NUnit.Framework.Assert.AreEqual(b.GetBlock(), b1.GetBlock());
			DatanodeInfo[] dnInfo1 = b1.GetLocations();
			NUnit.Framework.Assert.AreEqual(dnInfo.Length, dnInfo1.Length);
			for (int i = 0; i < dnInfo.Length; i++)
			{
				Compare(dnInfo[0], dnInfo1[0]);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlockRecoveryCommand()
		{
			DatanodeInfo di1 = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo di2 = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo[] dnInfo = new DatanodeInfo[] { di1, di2 };
			IList<BlockRecoveryCommand.RecoveringBlock> blks = ImmutableList.Of(new BlockRecoveryCommand.RecoveringBlock
				(GetExtendedBlock(1), dnInfo, 3), new BlockRecoveryCommand.RecoveringBlock(GetExtendedBlock
				(2), dnInfo, 3));
			BlockRecoveryCommand cmd = new BlockRecoveryCommand(blks);
			DatanodeProtocolProtos.BlockRecoveryCommandProto proto = PBHelper.Convert(cmd);
			NUnit.Framework.Assert.AreEqual(1, proto.GetBlocks(0).GetBlock().GetB().GetBlockId
				());
			NUnit.Framework.Assert.AreEqual(2, proto.GetBlocks(1).GetBlock().GetB().GetBlockId
				());
			BlockRecoveryCommand cmd2 = PBHelper.Convert(proto);
			IList<BlockRecoveryCommand.RecoveringBlock> cmd2Blks = Lists.NewArrayList(cmd2.GetRecoveringBlocks
				());
			NUnit.Framework.Assert.AreEqual(blks[0].GetBlock(), cmd2Blks[0].GetBlock());
			NUnit.Framework.Assert.AreEqual(blks[1].GetBlock(), cmd2Blks[1].GetBlock());
			NUnit.Framework.Assert.AreEqual(Joiner.On(",").Join(blks), Joiner.On(",").Join(cmd2Blks
				));
			NUnit.Framework.Assert.AreEqual(cmd.ToString(), cmd2.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertText()
		{
			Text t = new Text(Sharpen.Runtime.GetBytesForString("abc"));
			string s = t.ToString();
			Text t1 = new Text(s);
			NUnit.Framework.Assert.AreEqual(t, t1);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlockToken()
		{
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
				("password"), new Text("kind"), new Text("service"));
			SecurityProtos.TokenProto tokenProto = PBHelper.Convert(token);
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token2 = PBHelper.Convert
				(tokenProto);
			Compare(token, token2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertNamespaceInfo()
		{
			NamespaceInfo info = new NamespaceInfo(37, "clusterID", "bpID", 2300);
			HdfsProtos.NamespaceInfoProto proto = PBHelper.Convert(info);
			NamespaceInfo info2 = PBHelper.Convert(proto);
			Compare(info, info2);
			//Compare the StorageInfo
			NUnit.Framework.Assert.AreEqual(info.GetBlockPoolID(), info2.GetBlockPoolID());
			NUnit.Framework.Assert.AreEqual(info.GetBuildVersion(), info2.GetBuildVersion());
		}

		private void Compare(StorageInfo expected, StorageInfo actual)
		{
			NUnit.Framework.Assert.AreEqual(expected.clusterID, actual.clusterID);
			NUnit.Framework.Assert.AreEqual(expected.namespaceID, actual.namespaceID);
			NUnit.Framework.Assert.AreEqual(expected.cTime, actual.cTime);
			NUnit.Framework.Assert.AreEqual(expected.layoutVersion, actual.layoutVersion);
		}

		private void Compare(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>
			 expected, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> actual)
		{
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expected.GetIdentifier(), actual.GetIdentifier
				()));
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expected.GetPassword(), actual.GetPassword
				()));
			NUnit.Framework.Assert.AreEqual(expected.GetKind(), actual.GetKind());
			NUnit.Framework.Assert.AreEqual(expected.GetService(), actual.GetService());
		}

		private void Compare(LocatedBlock expected, LocatedBlock actual)
		{
			NUnit.Framework.Assert.AreEqual(expected.GetBlock(), actual.GetBlock());
			Compare(expected.GetBlockToken(), actual.GetBlockToken());
			NUnit.Framework.Assert.AreEqual(expected.GetStartOffset(), actual.GetStartOffset(
				));
			NUnit.Framework.Assert.AreEqual(expected.IsCorrupt(), actual.IsCorrupt());
			DatanodeInfo[] ei = expected.GetLocations();
			DatanodeInfo[] ai = actual.GetLocations();
			NUnit.Framework.Assert.AreEqual(ei.Length, ai.Length);
			for (int i = 0; i < ei.Length; i++)
			{
				Compare(ei[i], ai[i]);
			}
		}

		private LocatedBlock CreateLocatedBlock()
		{
			DatanodeInfo[] dnInfos = new DatanodeInfo[] { DFSTestUtil.GetLocalDatanodeInfo("127.0.0.1"
				, "h1", DatanodeInfo.AdminStates.DecommissionInprogress), DFSTestUtil.GetLocalDatanodeInfo
				("127.0.0.1", "h2", DatanodeInfo.AdminStates.Decommissioned), DFSTestUtil.GetLocalDatanodeInfo
				("127.0.0.1", "h3", DatanodeInfo.AdminStates.Normal), DFSTestUtil.GetLocalDatanodeInfo
				("127.0.0.1", "h4", DatanodeInfo.AdminStates.Normal) };
			string[] storageIDs = new string[] { "s1", "s2", "s3", "s4" };
			StorageType[] media = new StorageType[] { StorageType.Disk, StorageType.Ssd, StorageType
				.Disk, StorageType.RamDisk };
			LocatedBlock lb = new LocatedBlock(new ExtendedBlock("bp12", 12345, 10, 53), dnInfos
				, storageIDs, media, 5, false, new DatanodeInfo[] {  });
			lb.SetBlockToken(new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>
				(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
				("password"), new Text("kind"), new Text("service")));
			return lb;
		}

		private LocatedBlock CreateLocatedBlockNoStorageMedia()
		{
			DatanodeInfo[] dnInfos = new DatanodeInfo[] { DFSTestUtil.GetLocalDatanodeInfo("127.0.0.1"
				, "h1", DatanodeInfo.AdminStates.DecommissionInprogress), DFSTestUtil.GetLocalDatanodeInfo
				("127.0.0.1", "h2", DatanodeInfo.AdminStates.Decommissioned), DFSTestUtil.GetLocalDatanodeInfo
				("127.0.0.1", "h3", DatanodeInfo.AdminStates.Normal) };
			LocatedBlock lb = new LocatedBlock(new ExtendedBlock("bp12", 12345, 10, 53), dnInfos
				, 5, false);
			lb.SetBlockToken(new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>
				(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
				("password"), new Text("kind"), new Text("service")));
			return lb;
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertLocatedBlock()
		{
			LocatedBlock lb = CreateLocatedBlock();
			HdfsProtos.LocatedBlockProto lbProto = PBHelper.Convert(lb);
			LocatedBlock lb2 = PBHelper.Convert(lbProto);
			Compare(lb, lb2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertLocatedBlockNoStorageMedia()
		{
			LocatedBlock lb = CreateLocatedBlockNoStorageMedia();
			HdfsProtos.LocatedBlockProto lbProto = PBHelper.Convert(lb);
			LocatedBlock lb2 = PBHelper.Convert(lbProto);
			Compare(lb, lb2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertLocatedBlockList()
		{
			AList<LocatedBlock> lbl = new AList<LocatedBlock>();
			for (int i = 0; i < 3; i++)
			{
				lbl.AddItem(CreateLocatedBlock());
			}
			IList<HdfsProtos.LocatedBlockProto> lbpl = PBHelper.ConvertLocatedBlock2(lbl);
			IList<LocatedBlock> lbl2 = PBHelper.ConvertLocatedBlock(lbpl);
			NUnit.Framework.Assert.AreEqual(lbl.Count, lbl2.Count);
			for (int i_1 = 0; i_1 < lbl.Count; i_1++)
			{
				Compare(lbl[i_1], lbl2[2]);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertLocatedBlockArray()
		{
			LocatedBlock[] lbl = new LocatedBlock[3];
			for (int i = 0; i < 3; i++)
			{
				lbl[i] = CreateLocatedBlock();
			}
			HdfsProtos.LocatedBlockProto[] lbpl = PBHelper.ConvertLocatedBlock(lbl);
			LocatedBlock[] lbl2 = PBHelper.ConvertLocatedBlock(lbpl);
			NUnit.Framework.Assert.AreEqual(lbl.Length, lbl2.Length);
			for (int i_1 = 0; i_1 < lbl.Length; i_1++)
			{
				Compare(lbl[i_1], lbl2[i_1]);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertDatanodeRegistration()
		{
			DatanodeID dnId = DFSTestUtil.GetLocalDatanodeID();
			BlockKey[] keys = new BlockKey[] { GetBlockKey(2), GetBlockKey(3) };
			ExportedBlockKeys expKeys = new ExportedBlockKeys(true, 9, 10, GetBlockKey(1), keys
				);
			DatanodeRegistration reg = new DatanodeRegistration(dnId, new StorageInfo(HdfsServerConstants.NodeType
				.DataNode), expKeys, "3.0.0");
			DatanodeProtocolProtos.DatanodeRegistrationProto proto = PBHelper.Convert(reg);
			DatanodeRegistration reg2 = PBHelper.Convert(proto);
			Compare(reg.GetStorageInfo(), reg2.GetStorageInfo());
			Compare(reg.GetExportedKeys(), reg2.GetExportedKeys());
			Compare(reg, reg2);
			NUnit.Framework.Assert.AreEqual(reg.GetSoftwareVersion(), reg2.GetSoftwareVersion
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertDatanodeStorage()
		{
			DatanodeStorage dns1 = new DatanodeStorage("id1", DatanodeStorage.State.Normal, StorageType
				.Ssd);
			HdfsProtos.DatanodeStorageProto proto = PBHelper.Convert(dns1);
			DatanodeStorage dns2 = PBHelper.Convert(proto);
			Compare(dns1, dns2);
		}

		[NUnit.Framework.Test]
		public virtual void TestConvertBlockCommand()
		{
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks = new Org.Apache.Hadoop.Hdfs.Protocol.Block
				[] { new Org.Apache.Hadoop.Hdfs.Protocol.Block(21), new Org.Apache.Hadoop.Hdfs.Protocol.Block
				(22) };
			DatanodeInfo[][] dnInfos = new DatanodeInfo[][] { new DatanodeInfo[1], new DatanodeInfo
				[2] };
			dnInfos[0][0] = DFSTestUtil.GetLocalDatanodeInfo();
			dnInfos[1][0] = DFSTestUtil.GetLocalDatanodeInfo();
			dnInfos[1][1] = DFSTestUtil.GetLocalDatanodeInfo();
			string[][] storageIDs = new string[][] { new string[] { "s00" }, new string[] { "s10"
				, "s11" } };
			StorageType[][] storageTypes = new StorageType[][] { new StorageType[] { StorageType
				.Default }, new StorageType[] { StorageType.Default, StorageType.Default } };
			BlockCommand bc = new BlockCommand(DatanodeProtocol.DnaTransfer, "bp1", blocks, dnInfos
				, storageTypes, storageIDs);
			DatanodeProtocolProtos.BlockCommandProto bcProto = PBHelper.Convert(bc);
			BlockCommand bc2 = PBHelper.Convert(bcProto);
			NUnit.Framework.Assert.AreEqual(bc.GetAction(), bc2.GetAction());
			NUnit.Framework.Assert.AreEqual(bc.GetBlocks().Length, bc2.GetBlocks().Length);
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks2 = bc2.GetBlocks();
			for (int i = 0; i < blocks.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(blocks[i], blocks2[i]);
			}
			DatanodeInfo[][] dnInfos2 = bc2.GetTargets();
			NUnit.Framework.Assert.AreEqual(dnInfos.Length, dnInfos2.Length);
			for (int i_1 = 0; i_1 < dnInfos.Length; i_1++)
			{
				DatanodeInfo[] d1 = dnInfos[i_1];
				DatanodeInfo[] d2 = dnInfos2[i_1];
				NUnit.Framework.Assert.AreEqual(d1.Length, d2.Length);
				for (int j = 0; j < d1.Length; j++)
				{
					Compare(d1[j], d2[j]);
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestChecksumTypeProto()
		{
			NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Null, PBHelper.Convert(HdfsProtos.ChecksumTypeProto
				.ChecksumNull));
			NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Crc32, PBHelper.Convert(HdfsProtos.ChecksumTypeProto
				.ChecksumCrc32));
			NUnit.Framework.Assert.AreEqual(DataChecksum.Type.Crc32c, PBHelper.Convert(HdfsProtos.ChecksumTypeProto
				.ChecksumCrc32c));
			NUnit.Framework.Assert.AreEqual(PBHelper.Convert(DataChecksum.Type.Null), HdfsProtos.ChecksumTypeProto
				.ChecksumNull);
			NUnit.Framework.Assert.AreEqual(PBHelper.Convert(DataChecksum.Type.Crc32), HdfsProtos.ChecksumTypeProto
				.ChecksumCrc32);
			NUnit.Framework.Assert.AreEqual(PBHelper.Convert(DataChecksum.Type.Crc32c), HdfsProtos.ChecksumTypeProto
				.ChecksumCrc32c);
		}

		[NUnit.Framework.Test]
		public virtual void TestAclEntryProto()
		{
			// All fields populated.
			AclEntry e1 = new AclEntry.Builder().SetName("test").SetPermission(FsAction.ReadExecute
				).SetScope(AclEntryScope.Default).SetType(AclEntryType.Other).Build();
			// No name.
			AclEntry e2 = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType(AclEntryType
				.User).SetPermission(FsAction.All).Build();
			// No permission, which will default to the 0'th enum element.
			AclEntry e3 = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType(AclEntryType
				.User).SetName("test").Build();
			AclEntry[] expected = new AclEntry[] { e1, e2, new AclEntry.Builder().SetScope(e3
				.GetScope()).SetType(e3.GetType()).SetName(e3.GetName()).SetPermission(FsAction.
				None).Build() };
			AclEntry[] actual = Sharpen.Collections.ToArray(Lists.NewArrayList(PBHelper.ConvertAclEntry
				(PBHelper.ConvertAclEntryProto(Lists.NewArrayList(e1, e2, e3)))), new AclEntry[0
				]);
			Assert.AssertArrayEquals(expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void TestAclStatusProto()
		{
			AclEntry e = new AclEntry.Builder().SetName("test").SetPermission(FsAction.ReadExecute
				).SetScope(AclEntryScope.Default).SetType(AclEntryType.Other).Build();
			AclStatus s = new AclStatus.Builder().Owner("foo").Group("bar").AddEntry(e).Build
				();
			NUnit.Framework.Assert.AreEqual(s, PBHelper.Convert(PBHelper.Convert(s)));
		}
	}
}
