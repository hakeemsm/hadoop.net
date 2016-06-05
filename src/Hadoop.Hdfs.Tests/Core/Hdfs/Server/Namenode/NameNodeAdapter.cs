using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This is a utility class to expose NameNode functionality for unit tests.
	/// 	</summary>
	public class NameNodeAdapter
	{
		/// <summary>Get the namesystem from the namenode</summary>
		public static FSNamesystem GetNamesystem(NameNode namenode)
		{
			return namenode.GetNamesystem();
		}

		/// <summary>Get block locations within the specified range.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static LocatedBlocks GetBlockLocations(NameNode namenode, string src, long
			 offset, long length)
		{
			return namenode.GetNamesystem().GetBlockLocations("foo", src, offset, length);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		public static HdfsFileStatus GetFileInfo(NameNode namenode, string src, bool resolveLink
			)
		{
			return FSDirStatAndListingOp.GetFileInfo(namenode.GetNamesystem().GetFSDirectory(
				), src, resolveLink);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		public static bool Mkdirs(NameNode namenode, string src, PermissionStatus permissions
			, bool createParent)
		{
			return namenode.GetNamesystem().Mkdirs(src, permissions, createParent);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void SaveNamespace(NameNode namenode)
		{
			namenode.GetNamesystem().SaveNamespace();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void EnterSafeMode(NameNode namenode, bool resourcesLow)
		{
			namenode.GetNamesystem().EnterSafeMode(resourcesLow);
		}

		public static void LeaveSafeMode(NameNode namenode)
		{
			namenode.GetNamesystem().LeaveSafeMode();
		}

		public static void AbortEditLogs(NameNode nn)
		{
			FSEditLog el = nn.GetFSImage().GetEditLog();
			el.AbortCurrentLogSegment();
		}

		/// <summary>Get the internal RPC server instance.</summary>
		/// <returns>rpc server</returns>
		public static Org.Apache.Hadoop.Ipc.Server GetRpcServer(NameNode namenode)
		{
			return ((NameNodeRpcServer)namenode.GetRpcServer()).clientRpcServer;
		}

		public static DelegationTokenSecretManager GetDtSecretManager(FSNamesystem ns)
		{
			return ns.GetDelegationTokenSecretManager();
		}

		/// <exception cref="System.IO.IOException"/>
		public static HeartbeatResponse SendHeartBeat(DatanodeRegistration nodeReg, DatanodeDescriptor
			 dd, FSNamesystem namesystem)
		{
			return namesystem.HandleHeartbeat(nodeReg, BlockManagerTestUtil.GetStorageReportsForDatanode
				(dd), dd.GetCacheCapacity(), dd.GetCacheRemaining(), 0, 0, 0, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool SetReplication(FSNamesystem ns, string src, short replication)
		{
			return ns.SetReplication(src, replication);
		}

		public static LeaseManager GetLeaseManager(FSNamesystem ns)
		{
			return ns.leaseManager;
		}

		/// <summary>Set the softLimit and hardLimit of client lease periods.</summary>
		public static void SetLeasePeriod(FSNamesystem namesystem, long soft, long hard)
		{
			GetLeaseManager(namesystem).SetLeasePeriod(soft, hard);
			namesystem.leaseManager.TriggerMonitorCheckNow();
		}

		public static string GetLeaseHolderForPath(NameNode namenode, string path)
		{
			LeaseManager.Lease l = namenode.GetNamesystem().leaseManager.GetLeaseByPath(path);
			return l == null ? null : l.GetHolder();
		}

		/// <returns>
		/// the timestamp of the last renewal of the given lease,
		/// or -1 in the case that the lease doesn't exist.
		/// </returns>
		public static long GetLeaseRenewalTime(NameNode nn, string path)
		{
			LeaseManager lm = nn.GetNamesystem().leaseManager;
			LeaseManager.Lease l = lm.GetLeaseByPath(path);
			if (l == null)
			{
				return -1;
			}
			return l.GetLastUpdate();
		}

		/// <summary>Return the datanode descriptor for the given datanode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static DatanodeDescriptor GetDatanode(FSNamesystem ns, DatanodeID id)
		{
			ns.ReadLock();
			try
			{
				return ns.GetBlockManager().GetDatanodeManager().GetDatanode(id);
			}
			finally
			{
				ns.ReadUnlock();
			}
		}

		/// <summary>Return the FSNamesystem stats</summary>
		public static long[] GetStats(FSNamesystem fsn)
		{
			return fsn.GetStats();
		}

		public static ReentrantReadWriteLock SpyOnFsLock(FSNamesystem fsn)
		{
			ReentrantReadWriteLock spy = Org.Mockito.Mockito.Spy(fsn.GetFsLockForTests());
			fsn.SetFsLockForTests(spy);
			return spy;
		}

		public static FSImage SpyOnFsImage(NameNode nn1)
		{
			FSNamesystem fsn = nn1.GetNamesystem();
			FSImage spy = Org.Mockito.Mockito.Spy(fsn.GetFSImage());
			Whitebox.SetInternalState(fsn, "fsImage", spy);
			return spy;
		}

		public static FSEditLog SpyOnEditLog(NameNode nn)
		{
			FSEditLog spyEditLog = Org.Mockito.Mockito.Spy(nn.GetNamesystem().GetFSImage().GetEditLog
				());
			DFSTestUtil.SetEditLogForTesting(nn.GetNamesystem(), spyEditLog);
			EditLogTailer tailer = nn.GetNamesystem().GetEditLogTailer();
			if (tailer != null)
			{
				tailer.SetEditLog(spyEditLog);
			}
			return spyEditLog;
		}

		public static JournalSet SpyOnJournalSet(NameNode nn)
		{
			FSEditLog editLog = nn.GetFSImage().GetEditLog();
			JournalSet js = Org.Mockito.Mockito.Spy(editLog.GetJournalSet());
			editLog.SetJournalSetForTesting(js);
			return js;
		}

		public static string GetMkdirOpPath(FSEditLogOp op)
		{
			if (op.opCode == FSEditLogOpCodes.OpMkdir)
			{
				return ((FSEditLogOp.MkdirOp)op).path;
			}
			else
			{
				return null;
			}
		}

		public static FSEditLogOp CreateMkdirOp(string path)
		{
			FSEditLogOp.MkdirOp op = FSEditLogOp.MkdirOp.GetInstance(new FSEditLogOp.OpInstanceCache
				()).SetPath(path).SetTimestamp(0).SetPermissionStatus(new PermissionStatus("testuser"
				, "testgroup", FsPermission.GetDefault()));
			return op;
		}

		/// <returns>
		/// the number of blocks marked safe by safemode, or -1
		/// if safemode is not running.
		/// </returns>
		public static int GetSafeModeSafeBlocks(NameNode nn)
		{
			FSNamesystem.SafeModeInfo smi = nn.GetNamesystem().GetSafeModeInfoForTests();
			if (smi == null)
			{
				return -1;
			}
			return smi.blockSafe;
		}

		/// <returns>Replication queue initialization status</returns>
		public static bool SafeModeInitializedReplQueues(NameNode nn)
		{
			return nn.GetNamesystem().IsPopulatingReplQueues();
		}

		public static FilePath GetInProgressEditsFile(Storage.StorageDirectory sd, long startTxId
			)
		{
			return NNStorage.GetInProgressEditsFile(sd, startTxId);
		}

		/// <exception cref="System.IO.IOException"/>
		public static NamenodeCommand StartCheckpoint(NameNode nn, NamenodeRegistration backupNode
			, NamenodeRegistration activeNamenode)
		{
			return nn.GetNamesystem().StartCheckpoint(backupNode, activeNamenode);
		}
	}
}
