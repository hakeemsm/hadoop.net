using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// A thread per active or standby namenode to perform:
	/// <ul>
	/// <li> Pre-registration handshake with namenode</li>
	/// <li> Registration with namenode</li>
	/// <li> Send periodic heartbeats to the namenode</li>
	/// <li> Handle commands received from the namenode</li>
	/// </ul>
	/// </summary>
	internal class BPServiceActor : Runnable
	{
		internal static readonly Log Log = DataNode.Log;

		internal readonly IPEndPoint nnAddr;

		internal HAServiceProtocol.HAServiceState state;

		internal readonly BPOfferService bpos;

		internal volatile long lastDeletedReport = 0;

		internal volatile long lastCacheReport = 0;

		private readonly BPServiceActor.Scheduler scheduler;

		internal Sharpen.Thread bpThread;

		internal DatanodeProtocolClientSideTranslatorPB bpNamenode;

		internal enum RunningState
		{
			Connecting,
			InitFailed,
			Running,
			Exited,
			Failed
		}

		private volatile BPServiceActor.RunningState runningState = BPServiceActor.RunningState
			.Connecting;

		/// <summary>
		/// Between block reports (which happen on the order of once an hour) the
		/// DN reports smaller incremental changes to its block list.
		/// </summary>
		/// <remarks>
		/// Between block reports (which happen on the order of once an hour) the
		/// DN reports smaller incremental changes to its block list. This map,
		/// keyed by block ID, contains the pending changes which have yet to be
		/// reported to the NN. Access should be synchronized on this object.
		/// </remarks>
		private readonly IDictionary<DatanodeStorage, BPServiceActor.PerStoragePendingIncrementalBR
			> pendingIncrementalBRperStorage = Maps.NewHashMap();

		private volatile bool sendImmediateIBR = false;

		private volatile bool shouldServiceRun = true;

		private readonly DataNode dn;

		private readonly DNConf dnConf;

		private DatanodeRegistration bpRegistration;

		internal readonly List<BPServiceActorAction> bpThreadQueue = new List<BPServiceActorAction
			>();

		internal BPServiceActor(IPEndPoint nnAddr, BPOfferService bpos)
		{
			// IBR = Incremental Block Report. If this flag is set then an IBR will be
			// sent immediately by the actor thread without waiting for the IBR timer
			// to elapse.
			this.bpos = bpos;
			this.dn = bpos.GetDataNode();
			this.nnAddr = nnAddr;
			this.dnConf = dn.GetDnConf();
			scheduler = new BPServiceActor.Scheduler(dnConf.heartBeatInterval, dnConf.blockReportInterval
				);
		}

		internal virtual bool IsAlive()
		{
			if (!shouldServiceRun || !bpThread.IsAlive())
			{
				return false;
			}
			return runningState == BPServiceActor.RunningState.Running || runningState == BPServiceActor.RunningState
				.Connecting;
		}

		public override string ToString()
		{
			return bpos.ToString() + " service to " + nnAddr;
		}

		internal virtual IPEndPoint GetNNSocketAddress()
		{
			return nnAddr;
		}

		/// <summary>Used to inject a spy NN in the unit tests.</summary>
		[VisibleForTesting]
		internal virtual void SetNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol
			)
		{
			bpNamenode = dnProtocol;
		}

		[VisibleForTesting]
		internal virtual DatanodeProtocolClientSideTranslatorPB GetNameNodeProxy()
		{
			return bpNamenode;
		}

		/// <summary>Perform the first part of the handshake with the NameNode.</summary>
		/// <remarks>
		/// Perform the first part of the handshake with the NameNode.
		/// This calls <code>versionRequest</code> to determine the NN's
		/// namespace and version info. It automatically retries until
		/// the NN responds or the DN is shutting down.
		/// </remarks>
		/// <returns>the NamespaceInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual NamespaceInfo RetrieveNamespaceInfo()
		{
			NamespaceInfo nsInfo = null;
			while (ShouldRun())
			{
				try
				{
					nsInfo = bpNamenode.VersionRequest();
					Log.Debug(this + " received versionRequest response: " + nsInfo);
					break;
				}
				catch (SocketTimeoutException)
				{
					// namenode is busy
					Log.Warn("Problem connecting to server: " + nnAddr);
				}
				catch (IOException)
				{
					// namenode is not available
					Log.Warn("Problem connecting to server: " + nnAddr);
				}
				// try again in a second
				SleepAndLogInterrupts(5000, "requesting version info from NN");
			}
			if (nsInfo != null)
			{
				CheckNNVersion(nsInfo);
			}
			else
			{
				throw new IOException("DN shut down before block pool connected");
			}
			return nsInfo;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.IncorrectVersionException"/
		/// 	>
		private void CheckNNVersion(NamespaceInfo nsInfo)
		{
			// build and layout versions should match
			string nnVersion = nsInfo.GetSoftwareVersion();
			string minimumNameNodeVersion = dnConf.GetMinimumNameNodeVersion();
			if (VersionUtil.CompareVersions(nnVersion, minimumNameNodeVersion) < 0)
			{
				IncorrectVersionException ive = new IncorrectVersionException(minimumNameNodeVersion
					, nnVersion, "NameNode", "DataNode");
				Log.Warn(ive.Message);
				throw ive;
			}
			string dnVersion = VersionInfo.GetVersion();
			if (!nnVersion.Equals(dnVersion))
			{
				Log.Info("Reported NameNode version '" + nnVersion + "' does not match " + "DataNode version '"
					 + dnVersion + "' but is within acceptable " + "limits. Note: This is normal during a rolling upgrade."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ConnectToNNAndHandshake()
		{
			// get NN proxy
			bpNamenode = dn.ConnectToNN(nnAddr);
			// First phase of the handshake with NN - get the namespace
			// info.
			NamespaceInfo nsInfo = RetrieveNamespaceInfo();
			// Verify that this matches the other NN in this HA pair.
			// This also initializes our block pool in the DN if we are
			// the first NN connection for this BP.
			bpos.VerifyAndSetNamespaceInfo(nsInfo);
			// Second phase of the handshake with the NN.
			Register(nsInfo);
		}

		/// <summary>
		/// Report received blocks and delete hints to the Namenode for each
		/// storage.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void ReportReceivedDeletedBlocks()
		{
			// Generate a list of the pending reports for each storage under the lock
			AList<StorageReceivedDeletedBlocks> reports = new AList<StorageReceivedDeletedBlocks
				>(pendingIncrementalBRperStorage.Count);
			lock (pendingIncrementalBRperStorage)
			{
				foreach (KeyValuePair<DatanodeStorage, BPServiceActor.PerStoragePendingIncrementalBR
					> entry in pendingIncrementalBRperStorage)
				{
					DatanodeStorage storage = entry.Key;
					BPServiceActor.PerStoragePendingIncrementalBR perStorageMap = entry.Value;
					if (perStorageMap.GetBlockInfoCount() > 0)
					{
						// Send newly-received and deleted blockids to namenode
						ReceivedDeletedBlockInfo[] rdbi = perStorageMap.DequeueBlockInfos();
						reports.AddItem(new StorageReceivedDeletedBlocks(storage, rdbi));
					}
				}
				sendImmediateIBR = false;
			}
			if (reports.Count == 0)
			{
				// Nothing new to report.
				return;
			}
			// Send incremental block reports to the Namenode outside the lock
			bool success = false;
			long startTime = Time.MonotonicNow();
			try
			{
				bpNamenode.BlockReceivedAndDeleted(bpRegistration, bpos.GetBlockPoolId(), Sharpen.Collections.ToArray
					(reports, new StorageReceivedDeletedBlocks[reports.Count]));
				success = true;
			}
			finally
			{
				dn.GetMetrics().AddIncrementalBlockReport(Time.MonotonicNow() - startTime);
				if (!success)
				{
					lock (pendingIncrementalBRperStorage)
					{
						foreach (StorageReceivedDeletedBlocks report in reports)
						{
							// If we didn't succeed in sending the report, put all of the
							// blocks back onto our queue, but only in the case where we
							// didn't put something newer in the meantime.
							BPServiceActor.PerStoragePendingIncrementalBR perStorageMap = pendingIncrementalBRperStorage
								[report.GetStorage()];
							perStorageMap.PutMissingBlockInfos(report.GetBlocks());
							sendImmediateIBR = true;
						}
					}
				}
			}
		}

		/// <returns>
		/// pending incremental block report for given
		/// <paramref name="storage"/>
		/// </returns>
		private BPServiceActor.PerStoragePendingIncrementalBR GetIncrementalBRMapForStorage
			(DatanodeStorage storage)
		{
			BPServiceActor.PerStoragePendingIncrementalBR mapForStorage = pendingIncrementalBRperStorage
				[storage];
			if (mapForStorage == null)
			{
				// This is the first time we are adding incremental BR state for
				// this storage so create a new map. This is required once per
				// storage, per service actor.
				mapForStorage = new BPServiceActor.PerStoragePendingIncrementalBR();
				pendingIncrementalBRperStorage[storage] = mapForStorage;
			}
			return mapForStorage;
		}

		/// <summary>Add a blockInfo for notification to NameNode.</summary>
		/// <remarks>
		/// Add a blockInfo for notification to NameNode. If another entry
		/// exists for the same block it is removed.
		/// Caller must synchronize access using pendingIncrementalBRperStorage.
		/// </remarks>
		internal virtual void AddPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo
			, DatanodeStorage storage)
		{
			// Make sure another entry for the same block is first removed.
			// There may only be one such entry.
			foreach (KeyValuePair<DatanodeStorage, BPServiceActor.PerStoragePendingIncrementalBR
				> entry in pendingIncrementalBRperStorage)
			{
				if (entry.Value.RemoveBlockInfo(bInfo))
				{
					break;
				}
			}
			GetIncrementalBRMapForStorage(storage).PutBlockInfo(bInfo);
		}

		/*
		* Informing the name node could take a long long time! Should we wait
		* till namenode is informed before responding with success to the
		* client? For now we don't.
		*/
		internal virtual void NotifyNamenodeBlock(ReceivedDeletedBlockInfo bInfo, string 
			storageUuid, bool now)
		{
			lock (pendingIncrementalBRperStorage)
			{
				AddPendingReplicationBlockInfo(bInfo, dn.GetFSDataset().GetStorage(storageUuid));
				sendImmediateIBR = true;
				// If now is true, the report is sent right away.
				// Otherwise, it will be sent out in the next heartbeat.
				if (now)
				{
					Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				}
			}
		}

		internal virtual void NotifyNamenodeDeletedBlock(ReceivedDeletedBlockInfo bInfo, 
			string storageUuid)
		{
			lock (pendingIncrementalBRperStorage)
			{
				AddPendingReplicationBlockInfo(bInfo, dn.GetFSDataset().GetStorage(storageUuid));
			}
		}

		/// <summary>Run an immediate block report on this thread.</summary>
		/// <remarks>Run an immediate block report on this thread. Used by tests.</remarks>
		[VisibleForTesting]
		internal virtual void TriggerBlockReportForTests()
		{
			lock (pendingIncrementalBRperStorage)
			{
				scheduler.ScheduleHeartbeat();
				long nextBlockReportTime = scheduler.ScheduleBlockReport(0);
				Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				while (nextBlockReportTime - scheduler.nextBlockReportTime >= 0)
				{
					try
					{
						Sharpen.Runtime.Wait(pendingIncrementalBRperStorage, 100);
					}
					catch (Exception)
					{
						return;
					}
				}
			}
		}

		[VisibleForTesting]
		internal virtual void TriggerHeartbeatForTests()
		{
			lock (pendingIncrementalBRperStorage)
			{
				long nextHeartbeatTime = scheduler.ScheduleHeartbeat();
				Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				while (nextHeartbeatTime - scheduler.nextHeartbeatTime >= 0)
				{
					try
					{
						Sharpen.Runtime.Wait(pendingIncrementalBRperStorage, 100);
					}
					catch (Exception)
					{
						return;
					}
				}
			}
		}

		[VisibleForTesting]
		internal virtual void TriggerDeletionReportForTests()
		{
			lock (pendingIncrementalBRperStorage)
			{
				lastDeletedReport = 0;
				Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				while (lastDeletedReport == 0)
				{
					try
					{
						Sharpen.Runtime.Wait(pendingIncrementalBRperStorage, 100);
					}
					catch (Exception)
					{
						return;
					}
				}
			}
		}

		[VisibleForTesting]
		internal virtual bool HasPendingIBR()
		{
			return sendImmediateIBR;
		}

		private long prevBlockReportId = 0;

		private long GenerateUniqueBlockReportId()
		{
			long id = Runtime.NanoTime();
			if (id <= prevBlockReportId)
			{
				id = prevBlockReportId + 1;
			}
			prevBlockReportId = id;
			return id;
		}

		/// <summary>Report the list blocks to the Namenode</summary>
		/// <returns>DatanodeCommands returned by the NN. May be null.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<DatanodeCommand> BlockReport()
		{
			// send block report if timer has expired.
			long startTime = scheduler.MonotonicNow();
			if (!scheduler.IsBlockReportDue())
			{
				return null;
			}
			AList<DatanodeCommand> cmds = new AList<DatanodeCommand>();
			// Flush any block information that precedes the block report. Otherwise
			// we have a chance that we will miss the delHint information
			// or we will report an RBW replica after the BlockReport already reports
			// a FINALIZED one.
			ReportReceivedDeletedBlocks();
			lastDeletedReport = startTime;
			long brCreateStartTime = Time.MonotonicNow();
			IDictionary<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = dn.GetFSDataset
				().GetBlockReports(bpos.GetBlockPoolId());
			// Convert the reports to the format expected by the NN.
			int i = 0;
			int totalBlockCount = 0;
			StorageBlockReport[] reports = new StorageBlockReport[perVolumeBlockLists.Count];
			foreach (KeyValuePair<DatanodeStorage, BlockListAsLongs> kvPair in perVolumeBlockLists)
			{
				BlockListAsLongs blockList = kvPair.Value;
				reports[i++] = new StorageBlockReport(kvPair.Key, blockList);
				totalBlockCount += blockList.GetNumberOfBlocks();
			}
			// Send the reports to the NN.
			int numReportsSent = 0;
			int numRPCs = 0;
			bool success = false;
			long brSendStartTime = Time.MonotonicNow();
			long reportId = GenerateUniqueBlockReportId();
			try
			{
				if (totalBlockCount < dnConf.blockReportSplitThreshold)
				{
					// Below split threshold, send all reports in a single message.
					DatanodeCommand cmd = bpNamenode.BlockReport(bpRegistration, bpos.GetBlockPoolId(
						), reports, new BlockReportContext(1, 0, reportId));
					numRPCs = 1;
					numReportsSent = reports.Length;
					if (cmd != null)
					{
						cmds.AddItem(cmd);
					}
				}
				else
				{
					// Send one block report per message.
					for (int r = 0; r < reports.Length; r++)
					{
						StorageBlockReport[] singleReport = new StorageBlockReport[] { reports[r] };
						DatanodeCommand cmd = bpNamenode.BlockReport(bpRegistration, bpos.GetBlockPoolId(
							), singleReport, new BlockReportContext(reports.Length, r, reportId));
						numReportsSent++;
						numRPCs++;
						if (cmd != null)
						{
							cmds.AddItem(cmd);
						}
					}
				}
				success = true;
			}
			finally
			{
				// Log the block report processing stats from Datanode perspective
				long brSendCost = Time.MonotonicNow() - brSendStartTime;
				long brCreateCost = brSendStartTime - brCreateStartTime;
				dn.GetMetrics().AddBlockReport(brSendCost);
				int nCmds = cmds.Count;
				Log.Info((success ? "S" : "Uns") + "uccessfully sent block report 0x" + long.ToHexString
					(reportId) + ",  containing " + reports.Length + " storage report(s), of which we sent "
					 + numReportsSent + "." + " The reports had " + totalBlockCount + " total blocks and used "
					 + numRPCs + " RPC(s). This took " + brCreateCost + " msec to generate and " + brSendCost
					 + " msecs for RPC and NN processing." + " Got back " + ((nCmds == 0) ? "no commands"
					 : ((nCmds == 1) ? "one command: " + cmds[0] : (nCmds + " commands: " + Joiner.On
					("; ").Join(cmds)))) + ".");
			}
			scheduler.ScheduleNextBlockReport();
			return cmds.Count == 0 ? null : cmds;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual DatanodeCommand CacheReport()
		{
			// If caching is disabled, do not send a cache report
			if (dn.GetFSDataset().GetCacheCapacity() == 0)
			{
				return null;
			}
			// send cache report if timer has expired.
			DatanodeCommand cmd = null;
			long startTime = Time.MonotonicNow();
			if (startTime - lastCacheReport > dnConf.cacheReportInterval)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Sending cacheReport from service actor: " + this);
				}
				lastCacheReport = startTime;
				string bpid = bpos.GetBlockPoolId();
				IList<long> blockIds = dn.GetFSDataset().GetCacheReport(bpid);
				long createTime = Time.MonotonicNow();
				cmd = bpNamenode.CacheReport(bpRegistration, bpid, blockIds);
				long sendTime = Time.MonotonicNow();
				long createCost = createTime - startTime;
				long sendCost = sendTime - createTime;
				dn.GetMetrics().AddCacheReport(sendCost);
				Log.Debug("CacheReport of " + blockIds.Count + " block(s) took " + createCost + " msec to generate and "
					 + sendCost + " msecs for RPC and NN processing");
			}
			return cmd;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual HeartbeatResponse SendHeartBeat()
		{
			scheduler.ScheduleNextHeartbeat();
			StorageReport[] reports = dn.GetFSDataset().GetStorageReports(bpos.GetBlockPoolId
				());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Sending heartbeat with " + reports.Length + " storage reports from service actor: "
					 + this);
			}
			VolumeFailureSummary volumeFailureSummary = dn.GetFSDataset().GetVolumeFailureSummary
				();
			int numFailedVolumes = volumeFailureSummary != null ? volumeFailureSummary.GetFailedStorageLocations
				().Length : 0;
			return bpNamenode.SendHeartbeat(bpRegistration, reports, dn.GetFSDataset().GetCacheCapacity
				(), dn.GetFSDataset().GetCacheUsed(), dn.GetXmitsInProgress(), dn.GetXceiverCount
				(), numFailedVolumes, volumeFailureSummary);
		}

		//This must be called only by BPOfferService
		internal virtual void Start()
		{
			if ((bpThread != null) && (bpThread.IsAlive()))
			{
				//Thread is started already
				return;
			}
			bpThread = new Sharpen.Thread(this, FormatThreadName());
			bpThread.SetDaemon(true);
			// needed for JUnit testing
			bpThread.Start();
		}

		private string FormatThreadName()
		{
			ICollection<StorageLocation> dataDirs = DataNode.GetStorageLocations(dn.GetConf()
				);
			return "DataNode: [" + dataDirs.ToString() + "] " + " heartbeating to " + nnAddr;
		}

		//This must be called only by blockPoolManager.
		internal virtual void Stop()
		{
			shouldServiceRun = false;
			if (bpThread != null)
			{
				bpThread.Interrupt();
			}
		}

		//This must be called only by blockPoolManager
		internal virtual void Join()
		{
			try
			{
				if (bpThread != null)
				{
					bpThread.Join();
				}
			}
			catch (Exception)
			{
			}
		}

		//Cleanup method to be called by current thread before exiting.
		private void CleanUp()
		{
			lock (this)
			{
				shouldServiceRun = false;
				IOUtils.Cleanup(Log, bpNamenode);
				bpos.ShutdownActor(this);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void HandleRollingUpgradeStatus(HeartbeatResponse resp)
		{
			RollingUpgradeStatus rollingUpgradeStatus = resp.GetRollingUpdateStatus();
			if (rollingUpgradeStatus != null && string.CompareOrdinal(rollingUpgradeStatus.GetBlockPoolId
				(), bpos.GetBlockPoolId()) != 0)
			{
				// Can this ever occur?
				Log.Error("Invalid BlockPoolId " + rollingUpgradeStatus.GetBlockPoolId() + " in HeartbeatResponse. Expected "
					 + bpos.GetBlockPoolId());
			}
			else
			{
				bpos.SignalRollingUpgrade(rollingUpgradeStatus);
			}
		}

		/// <summary>Main loop for each BP thread.</summary>
		/// <remarks>
		/// Main loop for each BP thread. Run until shutdown,
		/// forever calling remote NameNode functions.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private void OfferService()
		{
			Log.Info("For namenode " + nnAddr + " using" + " DELETEREPORT_INTERVAL of " + dnConf
				.deleteReportInterval + " msec " + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval
				 + "msec" + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec" + 
				" Initial delay: " + dnConf.initialBlockReportDelay + "msec" + "; heartBeatInterval="
				 + dnConf.heartBeatInterval);
			//
			// Now loop for a long time....
			//
			while (ShouldRun())
			{
				try
				{
					long startTime = scheduler.MonotonicNow();
					//
					// Every so often, send heartbeat or block-report
					//
					bool sendHeartbeat = scheduler.IsHeartbeatDue(startTime);
					if (sendHeartbeat)
					{
						//
						// All heartbeat messages include following info:
						// -- Datanode name
						// -- data transfer port
						// -- Total capacity
						// -- Bytes remaining
						//
						if (!dn.AreHeartbeatsDisabledForTests())
						{
							HeartbeatResponse resp = SendHeartBeat();
							System.Diagnostics.Debug.Assert(resp != null);
							dn.GetMetrics().AddHeartbeat(scheduler.MonotonicNow() - startTime);
							// If the state of this NN has changed (eg STANDBY->ACTIVE)
							// then let the BPOfferService update itself.
							//
							// Important that this happens before processCommand below,
							// since the first heartbeat to a new active might have commands
							// that we should actually process.
							bpos.UpdateActorStatesFromHeartbeat(this, resp.GetNameNodeHaState());
							state = resp.GetNameNodeHaState().GetState();
							if (state == HAServiceProtocol.HAServiceState.Active)
							{
								HandleRollingUpgradeStatus(resp);
							}
							long startProcessCommands = Time.MonotonicNow();
							if (!ProcessCommand(resp.GetCommands()))
							{
								continue;
							}
							long endProcessCommands = Time.MonotonicNow();
							if (endProcessCommands - startProcessCommands > 2000)
							{
								Log.Info("Took " + (endProcessCommands - startProcessCommands) + "ms to process "
									 + resp.GetCommands().Length + " commands from NN");
							}
						}
					}
					if (sendImmediateIBR || (startTime - lastDeletedReport > dnConf.deleteReportInterval
						))
					{
						ReportReceivedDeletedBlocks();
						lastDeletedReport = startTime;
					}
					IList<DatanodeCommand> cmds = BlockReport();
					ProcessCommand(cmds == null ? null : Sharpen.Collections.ToArray(cmds, new DatanodeCommand
						[cmds.Count]));
					DatanodeCommand cmd = CacheReport();
					ProcessCommand(new DatanodeCommand[] { cmd });
					//
					// There is no work to do;  sleep until hearbeat timer elapses, 
					// or work arrives, and then iterate again.
					//
					long waitTime = scheduler.GetHeartbeatWaitTime();
					lock (pendingIncrementalBRperStorage)
					{
						if (waitTime > 0 && !sendImmediateIBR)
						{
							try
							{
								Sharpen.Runtime.Wait(pendingIncrementalBRperStorage, waitTime);
							}
							catch (Exception)
							{
								Log.Warn("BPOfferService for " + this + " interrupted");
							}
						}
					}
				}
				catch (RemoteException re)
				{
					// synchronized
					string reClass = re.GetClassName();
					if (typeof(UnregisteredNodeException).FullName.Equals(reClass) || typeof(DisallowedDatanodeException
						).FullName.Equals(reClass) || typeof(IncorrectVersionException).FullName.Equals(
						reClass))
					{
						Log.Warn(this + " is shutting down", re);
						shouldServiceRun = false;
						return;
					}
					Log.Warn("RemoteException in offerService", re);
					try
					{
						long sleepTime = Math.Min(1000, dnConf.heartBeatInterval);
						Sharpen.Thread.Sleep(sleepTime);
					}
					catch (Exception)
					{
						Sharpen.Thread.CurrentThread().Interrupt();
					}
				}
				catch (IOException e)
				{
					Log.Warn("IOException in offerService", e);
				}
				ProcessQueueMessages();
			}
		}

		// while (shouldRun())
		// offerService
		/// <summary>
		/// Register one bp with the corresponding NameNode
		/// <p>
		/// The bpDatanode needs to register with the namenode on startup in order
		/// 1) to report which storage it is serving now and
		/// 2) to receive a registrationID
		/// issued by the namenode to recognize registered datanodes.
		/// </summary>
		/// <param name="nsInfo">current NamespaceInfo</param>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.RegisterDatanode(Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Register(NamespaceInfo nsInfo)
		{
			// The handshake() phase loaded the block pool storage
			// off disk - so update the bpRegistration object from that info
			DatanodeRegistration newBpRegistration = bpos.CreateRegistration();
			Log.Info(this + " beginning handshake with NN");
			while (ShouldRun())
			{
				try
				{
					// Use returned registration from namenode with updated fields
					newBpRegistration = bpNamenode.RegisterDatanode(newBpRegistration);
					newBpRegistration.SetNamespaceInfo(nsInfo);
					bpRegistration = newBpRegistration;
					break;
				}
				catch (EOFException e)
				{
					// namenode might have just restarted
					Log.Info("Problem connecting to server: " + nnAddr + " :" + e.GetLocalizedMessage
						());
					SleepAndLogInterrupts(1000, "connecting to server");
				}
				catch (SocketTimeoutException)
				{
					// namenode is busy
					Log.Info("Problem connecting to server: " + nnAddr);
					SleepAndLogInterrupts(1000, "connecting to server");
				}
			}
			Log.Info("Block pool " + this + " successfully registered with NN");
			bpos.RegistrationSucceeded(this, bpRegistration);
			// random short delay - helps scatter the BR from all DNs
			scheduler.ScheduleBlockReport(dnConf.initialBlockReportDelay);
		}

		private void SleepAndLogInterrupts(int millis, string stateString)
		{
			try
			{
				Sharpen.Thread.Sleep(millis);
			}
			catch (Exception)
			{
				Log.Info("BPOfferService " + this + " interrupted while " + stateString);
			}
		}

		/// <summary>No matter what kind of exception we get, keep retrying to offerService().
		/// 	</summary>
		/// <remarks>
		/// No matter what kind of exception we get, keep retrying to offerService().
		/// That's the loop that connects to the NameNode and provides basic DataNode
		/// functionality.
		/// Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
		/// happen either at shutdown or due to refreshNamenodes.
		/// </remarks>
		public virtual void Run()
		{
			Log.Info(this + " starting to offer service");
			try
			{
				while (true)
				{
					// init stuff
					try
					{
						// setup storage
						ConnectToNNAndHandshake();
						break;
					}
					catch (IOException ioe)
					{
						// Initial handshake, storage recovery or registration failed
						runningState = BPServiceActor.RunningState.InitFailed;
						if (ShouldRetryInit())
						{
							// Retry until all namenode's of BPOS failed initialization
							Log.Error("Initialization failed for " + this + " " + ioe.GetLocalizedMessage());
							SleepAndLogInterrupts(5000, "initializing");
						}
						else
						{
							runningState = BPServiceActor.RunningState.Failed;
							Log.Fatal("Initialization failed for " + this + ". Exiting. ", ioe);
							return;
						}
					}
				}
				runningState = BPServiceActor.RunningState.Running;
				while (ShouldRun())
				{
					try
					{
						OfferService();
					}
					catch (Exception ex)
					{
						Log.Error("Exception in BPOfferService for " + this, ex);
						SleepAndLogInterrupts(5000, "offering service");
					}
				}
				runningState = BPServiceActor.RunningState.Exited;
			}
			catch (Exception ex)
			{
				Log.Warn("Unexpected exception in block pool " + this, ex);
				runningState = BPServiceActor.RunningState.Failed;
			}
			finally
			{
				Log.Warn("Ending block pool service for: " + this);
				CleanUp();
			}
		}

		private bool ShouldRetryInit()
		{
			return ShouldRun() && bpos.ShouldRetryInit();
		}

		private bool ShouldRun()
		{
			return shouldServiceRun && dn.ShouldRun();
		}

		/// <summary>Process an array of datanode commands</summary>
		/// <param name="cmds">an array of datanode commands</param>
		/// <returns>true if further processing may be required or false otherwise.</returns>
		internal virtual bool ProcessCommand(DatanodeCommand[] cmds)
		{
			if (cmds != null)
			{
				foreach (DatanodeCommand cmd in cmds)
				{
					try
					{
						if (bpos.ProcessCommandFromActor(cmd, this) == false)
						{
							return false;
						}
					}
					catch (IOException ioe)
					{
						Log.Warn("Error processing datanode Command", ioe);
					}
				}
			}
			return true;
		}

		/// <summary>Report a bad block from another DN in this cluster.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block
			)
		{
			LocatedBlock lb = new LocatedBlock(block, new DatanodeInfo[] { dnInfo });
			bpNamenode.ReportBadBlocks(new LocatedBlock[] { lb });
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReRegister()
		{
			if (ShouldRun())
			{
				// re-retrieve namespace info to make sure that, if the NN
				// was restarted, we still match its version (HDFS-2120)
				NamespaceInfo nsInfo = RetrieveNamespaceInfo();
				// and re-register
				Register(nsInfo);
				scheduler.ScheduleHeartbeat();
			}
		}

		private class PerStoragePendingIncrementalBR
		{
			private readonly IDictionary<long, ReceivedDeletedBlockInfo> pendingIncrementalBR
				 = Maps.NewHashMap();

			/// <summary>
			/// Return the number of blocks on this storage that have pending
			/// incremental block reports.
			/// </summary>
			/// <returns/>
			internal virtual int GetBlockInfoCount()
			{
				return pendingIncrementalBR.Count;
			}

			/// <summary>Dequeue and return all pending incremental block report state.</summary>
			/// <returns/>
			internal virtual ReceivedDeletedBlockInfo[] DequeueBlockInfos()
			{
				ReceivedDeletedBlockInfo[] blockInfos = Sharpen.Collections.ToArray(pendingIncrementalBR
					.Values, new ReceivedDeletedBlockInfo[GetBlockInfoCount()]);
				pendingIncrementalBR.Clear();
				return blockInfos;
			}

			/// <summary>
			/// Add blocks from blockArray to pendingIncrementalBR, unless the
			/// block already exists in pendingIncrementalBR.
			/// </summary>
			/// <param name="blockArray">list of blocks to add.</param>
			/// <returns>the number of missing blocks that we added.</returns>
			internal virtual int PutMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray)
			{
				int blocksPut = 0;
				foreach (ReceivedDeletedBlockInfo rdbi in blockArray)
				{
					if (!pendingIncrementalBR.Contains(rdbi.GetBlock().GetBlockId()))
					{
						pendingIncrementalBR[rdbi.GetBlock().GetBlockId()] = rdbi;
						++blocksPut;
					}
				}
				return blocksPut;
			}

			/// <summary>Add pending incremental block report for a single block.</summary>
			/// <param name="blockInfo"/>
			internal virtual void PutBlockInfo(ReceivedDeletedBlockInfo blockInfo)
			{
				pendingIncrementalBR[blockInfo.GetBlock().GetBlockId()] = blockInfo;
			}

			/// <summary>
			/// Remove pending incremental block report for a single block if it
			/// exists.
			/// </summary>
			/// <param name="blockInfo"/>
			/// <returns>
			/// true if a report was removed, false if no report existed for
			/// the given block.
			/// </returns>
			internal virtual bool RemoveBlockInfo(ReceivedDeletedBlockInfo blockInfo)
			{
				return (Sharpen.Collections.Remove(pendingIncrementalBR, blockInfo.GetBlock().GetBlockId
					()) != null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void TriggerBlockReport(BlockReportOptions options)
		{
			if (options.IsIncremental())
			{
				Log.Info(bpos.ToString() + ": scheduling an incremental block report.");
				lock (pendingIncrementalBRperStorage)
				{
					sendImmediateIBR = true;
					Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				}
			}
			else
			{
				Log.Info(bpos.ToString() + ": scheduling a full block report.");
				lock (pendingIncrementalBRperStorage)
				{
					scheduler.ScheduleBlockReport(0);
					Sharpen.Runtime.NotifyAll(pendingIncrementalBRperStorage);
				}
			}
		}

		public virtual void BpThreadEnqueue(BPServiceActorAction action)
		{
			lock (bpThreadQueue)
			{
				if (!bpThreadQueue.Contains(action))
				{
					bpThreadQueue.AddItem(action);
				}
			}
		}

		private void ProcessQueueMessages()
		{
			List<BPServiceActorAction> duplicateQueue;
			lock (bpThreadQueue)
			{
				duplicateQueue = new List<BPServiceActorAction>(bpThreadQueue);
				bpThreadQueue.Clear();
			}
			while (!duplicateQueue.IsEmpty())
			{
				BPServiceActorAction actionItem = duplicateQueue.Remove();
				try
				{
					actionItem.ReportTo(bpNamenode, bpRegistration);
				}
				catch (BPServiceActorActionException baae)
				{
					Log.Warn(baae.Message + nnAddr, baae);
					// Adding it back to the queue if not present
					BpThreadEnqueue(actionItem);
				}
			}
		}

		internal virtual BPServiceActor.Scheduler GetScheduler()
		{
			return scheduler;
		}

		/// <summary>
		/// Utility class that wraps the timestamp computations for scheduling
		/// heartbeats and block reports.
		/// </summary>
		internal class Scheduler
		{
			[VisibleForTesting]
			internal volatile long nextBlockReportTime;

			[VisibleForTesting]
			internal volatile long nextHeartbeatTime;

			[VisibleForTesting]
			internal bool resetBlockReportTime = true;

			private readonly long heartbeatIntervalMs;

			private readonly long blockReportIntervalMs;

			internal Scheduler(long heartbeatIntervalMs, long blockReportIntervalMs)
			{
				nextBlockReportTime = MonotonicNow();
				nextHeartbeatTime = MonotonicNow();
				// nextBlockReportTime and nextHeartbeatTime may be assigned/read
				// by testing threads (through BPServiceActor#triggerXXX), while also
				// assigned/read by the actor thread.
				this.heartbeatIntervalMs = heartbeatIntervalMs;
				this.blockReportIntervalMs = blockReportIntervalMs;
			}

			// This is useful to make sure NN gets Heartbeat before Blockreport
			// upon NN restart while DN keeps retrying Otherwise,
			// 1. NN restarts.
			// 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
			// 3. After reregistration completes, DN will send Blockreport first.
			// 4. Given NN receives Blockreport after Heartbeat, it won't mark
			//    DatanodeStorageInfo#blockContentsStale to false until the next
			//    Blockreport.
			internal virtual long ScheduleHeartbeat()
			{
				nextHeartbeatTime = MonotonicNow();
				return nextHeartbeatTime;
			}

			internal virtual long ScheduleNextHeartbeat()
			{
				// Numerical overflow is possible here and is okay.
				nextHeartbeatTime = MonotonicNow() + heartbeatIntervalMs;
				return nextHeartbeatTime;
			}

			internal virtual bool IsHeartbeatDue(long startTime)
			{
				return (nextHeartbeatTime - startTime <= 0);
			}

			internal virtual bool IsBlockReportDue()
			{
				return nextBlockReportTime - MonotonicNow() <= 0;
			}

			/// <summary>
			/// This methods  arranges for the data node to send the block report at
			/// the next heartbeat.
			/// </summary>
			internal virtual long ScheduleBlockReport(long delay)
			{
				if (delay > 0)
				{
					// send BR after random delay
					// Numerical overflow is possible here and is okay.
					nextBlockReportTime = MonotonicNow() + DFSUtil.GetRandom().Next((int)(delay));
				}
				else
				{
					// send at next heartbeat
					nextBlockReportTime = MonotonicNow();
				}
				resetBlockReportTime = true;
				// reset future BRs for randomness
				return nextBlockReportTime;
			}

			/// <summary>Schedule the next block report after the block report interval.</summary>
			/// <remarks>
			/// Schedule the next block report after the block report interval. If the
			/// current block report was delayed then the next block report is sent per
			/// the original schedule.
			/// Numerical overflow is possible here.
			/// </remarks>
			internal virtual void ScheduleNextBlockReport()
			{
				// If we have sent the first set of block reports, then wait a random
				// time before we start the periodic block reports.
				if (resetBlockReportTime)
				{
					nextBlockReportTime = MonotonicNow() + DFSUtil.GetRandom().Next((int)(blockReportIntervalMs
						));
					resetBlockReportTime = false;
				}
				else
				{
					/* say the last block report was at 8:20:14. The current report
					* should have started around 9:20:14 (default 1 hour interval).
					* If current time is :
					*   1) normal like 9:20:18, next report should be at 10:20:14
					*   2) unexpected like 11:35:43, next report should be at 12:20:14
					*/
					nextBlockReportTime += (((MonotonicNow() - nextBlockReportTime + blockReportIntervalMs
						) / blockReportIntervalMs)) * blockReportIntervalMs;
				}
			}

			internal virtual long GetHeartbeatWaitTime()
			{
				return nextHeartbeatTime - MonotonicNow();
			}

			/// <summary>Wrapped for testing.</summary>
			/// <returns/>
			[VisibleForTesting]
			public virtual long MonotonicNow()
			{
				return Time.MonotonicNow();
			}
		}
	}
}
