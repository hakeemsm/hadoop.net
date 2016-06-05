using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// One instance per block-pool/namespace on the DN, which handles the
	/// heartbeats to the active and standby NNs for that namespace.
	/// </summary>
	/// <remarks>
	/// One instance per block-pool/namespace on the DN, which handles the
	/// heartbeats to the active and standby NNs for that namespace.
	/// This class manages an instance of
	/// <see cref="BPServiceActor"/>
	/// for each NN,
	/// and delegates calls to both NNs.
	/// It also maintains the state about which of the NNs is considered active.
	/// </remarks>
	internal class BPOfferService
	{
		internal static readonly Log Log = DataNode.Log;

		/// <summary>
		/// Information about the namespace that this service
		/// is registering with.
		/// </summary>
		/// <remarks>
		/// Information about the namespace that this service
		/// is registering with. This is assigned after
		/// the first phase of the handshake.
		/// </remarks>
		internal NamespaceInfo bpNSInfo;

		/// <summary>The registration information for this block pool.</summary>
		/// <remarks>
		/// The registration information for this block pool.
		/// This is assigned after the second phase of the
		/// handshake.
		/// </remarks>
		internal volatile DatanodeRegistration bpRegistration;

		private readonly DataNode dn;

		/// <summary>
		/// A reference to the BPServiceActor associated with the currently
		/// ACTIVE NN.
		/// </summary>
		/// <remarks>
		/// A reference to the BPServiceActor associated with the currently
		/// ACTIVE NN. In the case that all NameNodes are in STANDBY mode,
		/// this can be null. If non-null, this must always refer to a member
		/// of the
		/// <see cref="bpServices"/>
		/// list.
		/// </remarks>
		private BPServiceActor bpServiceToActive = null;

		/// <summary>
		/// The list of all actors for namenodes in this nameservice, regardless
		/// of their active or standby states.
		/// </summary>
		private readonly IList<BPServiceActor> bpServices = new CopyOnWriteArrayList<BPServiceActor
			>();

		/// <summary>
		/// Each time we receive a heartbeat from a NN claiming to be ACTIVE,
		/// we record that NN's most recent transaction ID here, so long as it
		/// is more recent than the previous value.
		/// </summary>
		/// <remarks>
		/// Each time we receive a heartbeat from a NN claiming to be ACTIVE,
		/// we record that NN's most recent transaction ID here, so long as it
		/// is more recent than the previous value. This allows us to detect
		/// split-brain scenarios in which a prior NN is still asserting its
		/// ACTIVE state but with a too-low transaction ID. See HDFS-2627
		/// for details.
		/// </remarks>
		private long lastActiveClaimTxId = -1;

		private readonly ReentrantReadWriteLock mReadWriteLock = new ReentrantReadWriteLock
			();

		private readonly Lock mReadLock = mReadWriteLock.ReadLock();

		private readonly Lock mWriteLock = mReadWriteLock.WriteLock();

		// utility methods to acquire and release read lock and write lock
		internal virtual void ReadLock()
		{
			mReadLock.Lock();
		}

		internal virtual void ReadUnlock()
		{
			mReadLock.Unlock();
		}

		internal virtual void WriteLock()
		{
			mWriteLock.Lock();
		}

		internal virtual void WriteUnlock()
		{
			mWriteLock.Unlock();
		}

		internal BPOfferService(IList<IPEndPoint> nnAddrs, DataNode dn)
		{
			Preconditions.CheckArgument(!nnAddrs.IsEmpty(), "Must pass at least one NN.");
			this.dn = dn;
			foreach (IPEndPoint addr in nnAddrs)
			{
				this.bpServices.AddItem(new BPServiceActor(addr, this));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RefreshNNList(AList<IPEndPoint> addrs)
		{
			ICollection<IPEndPoint> oldAddrs = Sets.NewHashSet();
			foreach (BPServiceActor actor in bpServices)
			{
				oldAddrs.AddItem(actor.GetNNSocketAddress());
			}
			ICollection<IPEndPoint> newAddrs = Sets.NewHashSet(addrs);
			if (!Sets.SymmetricDifference(oldAddrs, newAddrs).IsEmpty())
			{
				// Keep things simple for now -- we can implement this at a later date.
				throw new IOException("HA does not currently support adding a new standby to a running DN. "
					 + "Please do a rolling restart of DNs to reconfigure the list of NNs.");
			}
		}

		/// <returns>true if the service has registered with at least one NameNode.</returns>
		internal virtual bool IsInitialized()
		{
			return bpRegistration != null;
		}

		/// <returns>
		/// true if there is at least one actor thread running which is
		/// talking to a NameNode.
		/// </returns>
		internal virtual bool IsAlive()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				if (actor.IsAlive())
				{
					return true;
				}
			}
			return false;
		}

		internal virtual string GetBlockPoolId()
		{
			ReadLock();
			try
			{
				if (bpNSInfo != null)
				{
					return bpNSInfo.GetBlockPoolID();
				}
				else
				{
					Log.Warn("Block pool ID needed, but service not yet registered with NN", new Exception
						("trace"));
					return null;
				}
			}
			finally
			{
				ReadUnlock();
			}
		}

		internal virtual bool HasBlockPoolId()
		{
			return GetNamespaceInfo() != null;
		}

		internal virtual NamespaceInfo GetNamespaceInfo()
		{
			ReadLock();
			try
			{
				return bpNSInfo;
			}
			finally
			{
				ReadUnlock();
			}
		}

		public override string ToString()
		{
			ReadLock();
			try
			{
				if (bpNSInfo == null)
				{
					// If we haven't yet connected to our NN, we don't yet know our
					// own block pool ID.
					// If _none_ of the block pools have connected yet, we don't even
					// know the DatanodeID ID of this DN.
					string datanodeUuid = dn.GetDatanodeUuid();
					if (datanodeUuid == null || datanodeUuid.IsEmpty())
					{
						datanodeUuid = "unassigned";
					}
					return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
				}
				else
				{
					return "Block pool " + GetBlockPoolId() + " (Datanode Uuid " + dn.GetDatanodeUuid
						() + ")";
				}
			}
			finally
			{
				ReadUnlock();
			}
		}

		internal virtual void ReportBadBlocks(ExtendedBlock block, string storageUuid, StorageType
			 storageType)
		{
			CheckBlock(block);
			foreach (BPServiceActor actor in bpServices)
			{
				ReportBadBlockAction rbbAction = new ReportBadBlockAction(block, storageUuid, storageType
					);
				actor.BpThreadEnqueue(rbbAction);
			}
		}

		/*
		* Informing the name node could take a long long time! Should we wait
		* till namenode is informed before responding with success to the
		* client? For now we don't.
		*/
		internal virtual void NotifyNamenodeReceivedBlock(ExtendedBlock block, string delHint
			, string storageUuid)
		{
			CheckBlock(block);
			ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(block.GetLocalBlock
				(), ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, delHint);
			foreach (BPServiceActor actor in bpServices)
			{
				actor.NotifyNamenodeBlock(bInfo, storageUuid, true);
			}
		}

		private void CheckBlock(ExtendedBlock block)
		{
			Preconditions.CheckArgument(block != null, "block is null");
			Preconditions.CheckArgument(block.GetBlockPoolId().Equals(GetBlockPoolId()), "block belongs to BP %s instead of BP %s"
				, block.GetBlockPoolId(), GetBlockPoolId());
		}

		internal virtual void NotifyNamenodeDeletedBlock(ExtendedBlock block, string storageUuid
			)
		{
			CheckBlock(block);
			ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(block.GetLocalBlock
				(), ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock, null);
			foreach (BPServiceActor actor in bpServices)
			{
				actor.NotifyNamenodeDeletedBlock(bInfo, storageUuid);
			}
		}

		internal virtual void NotifyNamenodeReceivingBlock(ExtendedBlock block, string storageUuid
			)
		{
			CheckBlock(block);
			ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(block.GetLocalBlock
				(), ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock, null);
			foreach (BPServiceActor actor in bpServices)
			{
				actor.NotifyNamenodeBlock(bInfo, storageUuid, false);
			}
		}

		//This must be called only by blockPoolManager
		internal virtual void Start()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.Start();
			}
		}

		//This must be called only by blockPoolManager.
		internal virtual void Stop()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.Stop();
			}
		}

		//This must be called only by blockPoolManager
		internal virtual void Join()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.Join();
			}
		}

		internal virtual DataNode GetDataNode()
		{
			return dn;
		}

		/// <summary>Called by the BPServiceActors when they handshake to a NN.</summary>
		/// <remarks>
		/// Called by the BPServiceActors when they handshake to a NN.
		/// If this is the first NN connection, this sets the namespace info
		/// for this BPOfferService. If it's a connection to a new NN, it
		/// verifies that this namespace matches (eg to prevent a misconfiguration
		/// where a StandbyNode from a different cluster is specified)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void VerifyAndSetNamespaceInfo(NamespaceInfo nsInfo)
		{
			WriteLock();
			try
			{
				if (this.bpNSInfo == null)
				{
					this.bpNSInfo = nsInfo;
					bool success = false;
					// Now that we know the namespace ID, etc, we can pass this to the DN.
					// The DN can now initialize its local storage if we are the
					// first BP to handshake, etc.
					try
					{
						dn.InitBlockPool(this);
						success = true;
					}
					finally
					{
						if (!success)
						{
							// The datanode failed to initialize the BP. We need to reset
							// the namespace info so that other BPService actors still have
							// a chance to set it, and re-initialize the datanode.
							this.bpNSInfo = null;
						}
					}
				}
				else
				{
					CheckNSEquality(bpNSInfo.GetBlockPoolID(), nsInfo.GetBlockPoolID(), "Blockpool ID"
						);
					CheckNSEquality(bpNSInfo.GetNamespaceID(), nsInfo.GetNamespaceID(), "Namespace ID"
						);
					CheckNSEquality(bpNSInfo.GetClusterID(), nsInfo.GetClusterID(), "Cluster ID");
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// After one of the BPServiceActors registers successfully with the
		/// NN, it calls this function to verify that the NN it connected to
		/// is consistent with other NNs serving the block-pool.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RegistrationSucceeded(BPServiceActor bpServiceActor, DatanodeRegistration
			 reg)
		{
			WriteLock();
			try
			{
				if (bpRegistration != null)
				{
					CheckNSEquality(bpRegistration.GetStorageInfo().GetNamespaceID(), reg.GetStorageInfo
						().GetNamespaceID(), "namespace ID");
					CheckNSEquality(bpRegistration.GetStorageInfo().GetClusterID(), reg.GetStorageInfo
						().GetClusterID(), "cluster ID");
				}
				bpRegistration = reg;
				dn.BpRegistrationSucceeded(bpRegistration, GetBlockPoolId());
				// Add the initial block token secret keys to the DN's secret manager.
				if (dn.isBlockTokenEnabled)
				{
					dn.blockPoolTokenSecretManager.AddKeys(GetBlockPoolId(), reg.GetExportedKeys());
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// Verify equality of two namespace-related fields, throwing
		/// an exception if they are unequal.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void CheckNSEquality(object ourID, object theirID, string idHelpText
			)
		{
			if (!ourID.Equals(theirID))
			{
				throw new IOException(idHelpText + " mismatch: " + "previously connected to " + idHelpText
					 + " " + ourID + " but now connected to " + idHelpText + " " + theirID);
			}
		}

		internal virtual DatanodeRegistration CreateRegistration()
		{
			WriteLock();
			try
			{
				Preconditions.CheckState(bpNSInfo != null, "getRegistration() can only be called after initial handshake"
					);
				return dn.CreateBPRegistration(bpNSInfo);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Called when an actor shuts down.</summary>
		/// <remarks>
		/// Called when an actor shuts down. If this is the last actor
		/// to shut down, shuts down the whole blockpool in the DN.
		/// </remarks>
		internal virtual void ShutdownActor(BPServiceActor actor)
		{
			WriteLock();
			try
			{
				if (bpServiceToActive == actor)
				{
					bpServiceToActive = null;
				}
				bpServices.Remove(actor);
				if (bpServices.IsEmpty())
				{
					dn.ShutdownBlockPool(this);
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Called by the DN to report an error to the NNs.</summary>
		internal virtual void TrySendErrorReport(int errCode, string errMsg)
		{
			foreach (BPServiceActor actor in bpServices)
			{
				ErrorReportAction errorReportAction = new ErrorReportAction(errCode, errMsg);
				actor.BpThreadEnqueue(errorReportAction);
			}
		}

		/// <summary>
		/// Ask each of the actors to schedule a block report after
		/// the specified delay.
		/// </summary>
		internal virtual void ScheduleBlockReport(long delay)
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.GetScheduler().ScheduleBlockReport(delay);
			}
		}

		/// <summary>Ask each of the actors to report a bad block hosted on another DN.</summary>
		internal virtual void ReportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block
			)
		{
			foreach (BPServiceActor actor in bpServices)
			{
				try
				{
					actor.ReportRemoteBadBlock(dnInfo, block);
				}
				catch (IOException e)
				{
					Log.Warn("Couldn't report bad block " + block + " to " + actor, e);
				}
			}
		}

		/// <returns>
		/// a proxy to the active NN, or null if the BPOS has not
		/// acknowledged any NN as active yet.
		/// </returns>
		internal virtual DatanodeProtocolClientSideTranslatorPB GetActiveNN()
		{
			ReadLock();
			try
			{
				if (bpServiceToActive != null)
				{
					return bpServiceToActive.bpNamenode;
				}
				else
				{
					return null;
				}
			}
			finally
			{
				ReadUnlock();
			}
		}

		[VisibleForTesting]
		internal virtual IList<BPServiceActor> GetBPServiceActors()
		{
			return Lists.NewArrayList(bpServices);
		}

		/// <summary>Signal the current rolling upgrade status as indicated by the NN.</summary>
		/// <param name="rollingUpgradeStatus">rolling upgrade status</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SignalRollingUpgrade(RollingUpgradeStatus rollingUpgradeStatus
			)
		{
			if (rollingUpgradeStatus == null)
			{
				return;
			}
			string bpid = GetBlockPoolId();
			if (!rollingUpgradeStatus.IsFinalized())
			{
				dn.GetFSDataset().EnableTrash(bpid);
				dn.GetFSDataset().SetRollingUpgradeMarker(bpid);
			}
			else
			{
				dn.GetFSDataset().ClearTrash(bpid);
				dn.GetFSDataset().ClearRollingUpgradeMarker(bpid);
			}
		}

		/// <summary>
		/// Update the BPOS's view of which NN is active, based on a heartbeat
		/// response from one of the actors.
		/// </summary>
		/// <param name="actor">the actor which received the heartbeat</param>
		/// <param name="nnHaState">the HA-related heartbeat contents</param>
		internal virtual void UpdateActorStatesFromHeartbeat(BPServiceActor actor, NNHAStatusHeartbeat
			 nnHaState)
		{
			WriteLock();
			try
			{
				long txid = nnHaState.GetTxId();
				bool nnClaimsActive = nnHaState.GetState() == HAServiceProtocol.HAServiceState.Active;
				bool bposThinksActive = bpServiceToActive == actor;
				bool isMoreRecentClaim = txid > lastActiveClaimTxId;
				if (nnClaimsActive && !bposThinksActive)
				{
					Log.Info("Namenode " + actor + " trying to claim ACTIVE state with " + "txid=" + 
						txid);
					if (!isMoreRecentClaim)
					{
						// Split-brain scenario - an NN is trying to claim active
						// state when a different NN has already claimed it with a higher
						// txid.
						Log.Warn("NN " + actor + " tried to claim ACTIVE state at txid=" + txid + " but there was already a more recent claim at txid="
							 + lastActiveClaimTxId);
						return;
					}
					else
					{
						if (bpServiceToActive == null)
						{
							Log.Info("Acknowledging ACTIVE Namenode " + actor);
						}
						else
						{
							Log.Info("Namenode " + actor + " taking over ACTIVE state from " + bpServiceToActive
								 + " at higher txid=" + txid);
						}
						bpServiceToActive = actor;
					}
				}
				else
				{
					if (!nnClaimsActive && bposThinksActive)
					{
						Log.Info("Namenode " + actor + " relinquishing ACTIVE state with " + "txid=" + nnHaState
							.GetTxId());
						bpServiceToActive = null;
					}
				}
				if (bpServiceToActive == actor)
				{
					System.Diagnostics.Debug.Assert(txid >= lastActiveClaimTxId);
					lastActiveClaimTxId = txid;
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <returns>
		/// true if the given NN address is one of the NNs for this
		/// block pool
		/// </returns>
		internal virtual bool ContainsNN(IPEndPoint addr)
		{
			foreach (BPServiceActor actor in bpServices)
			{
				if (actor.GetNNSocketAddress().Equals(addr))
				{
					return true;
				}
			}
			return false;
		}

		[VisibleForTesting]
		internal virtual int CountNameNodes()
		{
			return bpServices.Count;
		}

		/// <summary>Run an immediate block report on this thread.</summary>
		/// <remarks>Run an immediate block report on this thread. Used by tests.</remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void TriggerBlockReportForTests()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.TriggerBlockReportForTests();
			}
		}

		/// <summary>Run an immediate deletion report on this thread.</summary>
		/// <remarks>Run an immediate deletion report on this thread. Used by tests.</remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void TriggerDeletionReportForTests()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.TriggerDeletionReportForTests();
			}
		}

		/// <summary>Run an immediate heartbeat from all actors.</summary>
		/// <remarks>Run an immediate heartbeat from all actors. Used by tests.</remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void TriggerHeartbeatForTests()
		{
			foreach (BPServiceActor actor in bpServices)
			{
				actor.TriggerHeartbeatForTests();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool ProcessCommandFromActor(DatanodeCommand cmd, BPServiceActor
			 actor)
		{
			System.Diagnostics.Debug.Assert(bpServices.Contains(actor));
			if (cmd == null)
			{
				return true;
			}
			/*
			* Datanode Registration can be done asynchronously here. No need to hold
			* the lock. for more info refer HDFS-5014
			*/
			if (DatanodeProtocol.DnaRegister == cmd.GetAction())
			{
				// namenode requested a registration - at start or if NN lost contact
				// Just logging the claiming state is OK here instead of checking the
				// actor state by obtaining the lock
				Log.Info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr + " with " 
					+ actor.state + " state");
				actor.ReRegister();
				return false;
			}
			WriteLock();
			try
			{
				if (actor == bpServiceToActive)
				{
					return ProcessCommandFromActive(cmd, actor);
				}
				else
				{
					return ProcessCommandFromStandby(cmd, actor);
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		private string BlockIdArrayToString(long[] ids)
		{
			long maxNumberOfBlocksToLog = dn.GetMaxNumberOfBlocksToLog();
			StringBuilder bld = new StringBuilder();
			string prefix = string.Empty;
			for (int i = 0; i < ids.Length; i++)
			{
				if (i >= maxNumberOfBlocksToLog)
				{
					bld.Append("...");
					break;
				}
				bld.Append(prefix).Append(ids[i]);
				prefix = ", ";
			}
			return bld.ToString();
		}

		/// <summary>
		/// This method should handle all commands from Active namenode except
		/// DNA_REGISTER which should be handled earlier itself.
		/// </summary>
		/// <param name="cmd"/>
		/// <returns>true if further processing may be required or false otherwise.</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool ProcessCommandFromActive(DatanodeCommand cmd, BPServiceActor actor)
		{
			BlockCommand bcmd = cmd is BlockCommand ? (BlockCommand)cmd : null;
			BlockIdCommand blockIdCmd = cmd is BlockIdCommand ? (BlockIdCommand)cmd : null;
			switch (cmd.GetAction())
			{
				case DatanodeProtocol.DnaTransfer:
				{
					// Send a copy of a block to another datanode
					dn.TransferBlocks(bcmd.GetBlockPoolId(), bcmd.GetBlocks(), bcmd.GetTargets(), bcmd
						.GetTargetStorageTypes());
					dn.metrics.IncrBlocksReplicated(bcmd.GetBlocks().Length);
					break;
				}

				case DatanodeProtocol.DnaInvalidate:
				{
					//
					// Some local block(s) are obsolete and can be 
					// safely garbage-collected.
					//
					Block[] toDelete = bcmd.GetBlocks();
					try
					{
						// using global fsdataset
						dn.GetFSDataset().Invalidate(bcmd.GetBlockPoolId(), toDelete);
					}
					catch (IOException e)
					{
						// Exceptions caught here are not expected to be disk-related.
						throw;
					}
					dn.metrics.IncrBlocksRemoved(toDelete.Length);
					break;
				}

				case DatanodeProtocol.DnaCache:
				{
					Log.Info("DatanodeCommand action: DNA_CACHE for " + blockIdCmd.GetBlockPoolId() +
						 " of [" + BlockIdArrayToString(blockIdCmd.GetBlockIds()) + "]");
					dn.GetFSDataset().Cache(blockIdCmd.GetBlockPoolId(), blockIdCmd.GetBlockIds());
					break;
				}

				case DatanodeProtocol.DnaUncache:
				{
					Log.Info("DatanodeCommand action: DNA_UNCACHE for " + blockIdCmd.GetBlockPoolId()
						 + " of [" + BlockIdArrayToString(blockIdCmd.GetBlockIds()) + "]");
					dn.GetFSDataset().Uncache(blockIdCmd.GetBlockPoolId(), blockIdCmd.GetBlockIds());
					break;
				}

				case DatanodeProtocol.DnaShutdown:
				{
					// TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
					// See HDFS-2987.
					throw new NotSupportedException("Received unimplemented DNA_SHUTDOWN");
				}

				case DatanodeProtocol.DnaFinalize:
				{
					string bp = ((FinalizeCommand)cmd).GetBlockPoolId();
					Log.Info("Got finalize command for block pool " + bp);
					System.Diagnostics.Debug.Assert(GetBlockPoolId().Equals(bp), "BP " + GetBlockPoolId
						() + " received DNA_FINALIZE " + "for other block pool " + bp);
					dn.FinalizeUpgradeForPool(bp);
					break;
				}

				case DatanodeProtocol.DnaRecoverblock:
				{
					string who = "NameNode at " + actor.GetNNSocketAddress();
					dn.RecoverBlocks(who, ((BlockRecoveryCommand)cmd).GetRecoveringBlocks());
					break;
				}

				case DatanodeProtocol.DnaAccesskeyupdate:
				{
					Log.Info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
					if (dn.isBlockTokenEnabled)
					{
						dn.blockPoolTokenSecretManager.AddKeys(GetBlockPoolId(), ((KeyUpdateCommand)cmd).
							GetExportedKeys());
					}
					break;
				}

				case DatanodeProtocol.DnaBalancerbandwidthupdate:
				{
					Log.Info("DatanodeCommand action: DNA_BALANCERBANDWIDTHUPDATE");
					long bandwidth = ((BalancerBandwidthCommand)cmd).GetBalancerBandwidthValue();
					if (bandwidth > 0)
					{
						DataXceiverServer dxcs = (DataXceiverServer)dn.dataXceiverServer.GetRunnable();
						Log.Info("Updating balance throttler bandwidth from " + dxcs.balanceThrottler.GetBandwidth
							() + " bytes/s " + "to: " + bandwidth + " bytes/s.");
						dxcs.balanceThrottler.SetBandwidth(bandwidth);
					}
					break;
				}

				default:
				{
					Log.Warn("Unknown DatanodeCommand action: " + cmd.GetAction());
					break;
				}
			}
			return true;
		}

		/// <summary>
		/// This method should handle commands from Standby namenode except
		/// DNA_REGISTER which should be handled earlier itself.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private bool ProcessCommandFromStandby(DatanodeCommand cmd, BPServiceActor actor)
		{
			switch (cmd.GetAction())
			{
				case DatanodeProtocol.DnaAccesskeyupdate:
				{
					Log.Info("DatanodeCommand action from standby: DNA_ACCESSKEYUPDATE");
					if (dn.isBlockTokenEnabled)
					{
						dn.blockPoolTokenSecretManager.AddKeys(GetBlockPoolId(), ((KeyUpdateCommand)cmd).
							GetExportedKeys());
					}
					break;
				}

				case DatanodeProtocol.DnaTransfer:
				case DatanodeProtocol.DnaInvalidate:
				case DatanodeProtocol.DnaShutdown:
				case DatanodeProtocol.DnaFinalize:
				case DatanodeProtocol.DnaRecoverblock:
				case DatanodeProtocol.DnaBalancerbandwidthupdate:
				case DatanodeProtocol.DnaCache:
				case DatanodeProtocol.DnaUncache:
				{
					Log.Warn("Got a command from standby NN - ignoring command:" + cmd.GetAction());
					break;
				}

				default:
				{
					Log.Warn("Unknown DatanodeCommand action: " + cmd.GetAction());
					break;
				}
			}
			return true;
		}

		/*
		* Let the actor retry for initialization until all namenodes of cluster have
		* failed.
		*/
		internal virtual bool ShouldRetryInit()
		{
			if (HasBlockPoolId())
			{
				// One of the namenode registered successfully. lets continue retry for
				// other.
				return true;
			}
			return IsAlive();
		}
	}
}
