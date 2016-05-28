using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class FSEditLogLoader
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLogLoader
			).FullName);

		internal const long ReplayTransactionLogInterval = 1000;

		private readonly FSNamesystem fsNamesys;

		private long lastAppliedTxId;

		/// <summary>Total number of end transactions loaded.</summary>
		private int totalEdits = 0;

		public FSEditLogLoader(FSNamesystem fsNamesys, long lastAppliedTxId)
		{
			// 1sec
			this.fsNamesys = fsNamesys;
			this.lastAppliedTxId = lastAppliedTxId;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long LoadFSEdits(EditLogInputStream edits, long expectedStartingTxId
			)
		{
			return LoadFSEdits(edits, expectedStartingTxId, null, null);
		}

		/// <summary>
		/// Load an edit log, and apply the changes to the in-memory structure
		/// This is where we apply edits that we've been writing to disk all
		/// along.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long LoadFSEdits(EditLogInputStream edits, long expectedStartingTxId
			, HdfsServerConstants.StartupOption startOpt, MetaRecoveryContext recovery)
		{
			StartupProgress prog = NameNode.GetStartupProgress();
			Step step = CreateStartupProgressStep(edits);
			prog.BeginStep(Phase.LoadingEdits, step);
			fsNamesys.WriteLock();
			try
			{
				long startTime = Time.MonotonicNow();
				FSImage.Log.Info("Start loading edits file " + edits.GetName());
				long numEdits = LoadEditRecords(edits, false, expectedStartingTxId, startOpt, recovery
					);
				FSImage.Log.Info("Edits file " + edits.GetName() + " of size " + edits.Length() +
					 " edits # " + numEdits + " loaded in " + (Time.MonotonicNow() - startTime) / 1000
					 + " seconds");
				return numEdits;
			}
			finally
			{
				edits.Close();
				fsNamesys.WriteUnlock();
				prog.EndStep(Phase.LoadingEdits, step);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long LoadEditRecords(EditLogInputStream @in, bool closeOnExit, long
			 expectedStartingTxId, HdfsServerConstants.StartupOption startOpt, MetaRecoveryContext
			 recovery)
		{
			FSDirectory fsDir = fsNamesys.dir;
			EnumMap<FSEditLogOpCodes, Holder<int>> opCounts = new EnumMap<FSEditLogOpCodes, Holder
				<int>>(typeof(FSEditLogOpCodes));
			if (Log.IsTraceEnabled())
			{
				Log.Trace("Acquiring write lock to replay edit log");
			}
			fsNamesys.WriteLock();
			fsDir.WriteLock();
			long[] recentOpcodeOffsets = new long[4];
			Arrays.Fill(recentOpcodeOffsets, -1);
			long expectedTxId = expectedStartingTxId;
			long numEdits = 0;
			long lastTxId = @in.GetLastTxId();
			long numTxns = (lastTxId - expectedStartingTxId) + 1;
			StartupProgress prog = NameNode.GetStartupProgress();
			Step step = CreateStartupProgressStep(@in);
			prog.SetTotal(Phase.LoadingEdits, step, numTxns);
			StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingEdits, step);
			long lastLogTime = Time.MonotonicNow();
			long lastInodeId = fsNamesys.dir.GetLastInodeId();
			try
			{
				while (true)
				{
					try
					{
						FSEditLogOp op;
						try
						{
							op = @in.ReadOp();
							if (op == null)
							{
								break;
							}
						}
						catch (Exception e)
						{
							// Handle a problem with our input
							Check203UpgradeFailure(@in.GetVersion(true), e);
							string errorMessage = FormatEditLogReplayError(@in, recentOpcodeOffsets, expectedTxId
								);
							FSImage.Log.Error(errorMessage, e);
							if (recovery == null)
							{
								// We will only try to skip over problematic opcodes when in
								// recovery mode.
								throw new EditLogInputException(errorMessage, e, numEdits);
							}
							MetaRecoveryContext.EditLogLoaderPrompt("We failed to read txId " + expectedTxId, 
								recovery, "skipping the bad section in the log");
							@in.Resync();
							continue;
						}
						recentOpcodeOffsets[(int)(numEdits % recentOpcodeOffsets.Length)] = @in.GetPosition
							();
						if (op.HasTransactionId())
						{
							if (op.GetTransactionId() > expectedTxId)
							{
								MetaRecoveryContext.EditLogLoaderPrompt("There appears " + "to be a gap in the edit log.  We expected txid "
									 + expectedTxId + ", but got txid " + op.GetTransactionId() + ".", recovery, "ignoring missing "
									 + " transaction IDs");
							}
							else
							{
								if (op.GetTransactionId() < expectedTxId)
								{
									MetaRecoveryContext.EditLogLoaderPrompt("There appears " + "to be an out-of-order edit in the edit log.  We "
										 + "expected txid " + expectedTxId + ", but got txid " + op.GetTransactionId() +
										 ".", recovery, "skipping the out-of-order edit");
									continue;
								}
							}
						}
						try
						{
							if (Log.IsTraceEnabled())
							{
								Log.Trace("op=" + op + ", startOpt=" + startOpt + ", numEdits=" + numEdits + ", totalEdits="
									 + totalEdits);
							}
							long inodeId = ApplyEditLogOp(op, fsDir, startOpt, @in.GetVersion(true), lastInodeId
								);
							if (lastInodeId < inodeId)
							{
								lastInodeId = inodeId;
							}
						}
						catch (FSEditLogOp.RollingUpgradeOp.RollbackException e)
						{
							throw;
						}
						catch (Exception e)
						{
							Log.Error("Encountered exception on operation " + op, e);
							if (recovery == null)
							{
								throw e is IOException ? (IOException)e : new IOException(e);
							}
							MetaRecoveryContext.EditLogLoaderPrompt("Failed to " + "apply edit log operation "
								 + op + ": error " + e.Message, recovery, "applying edits");
						}
						// Now that the operation has been successfully decoded and
						// applied, update our bookkeeping.
						IncrOpCount(op.opCode, opCounts, step, counter);
						if (op.HasTransactionId())
						{
							lastAppliedTxId = op.GetTransactionId();
							expectedTxId = lastAppliedTxId + 1;
						}
						else
						{
							expectedTxId = lastAppliedTxId = expectedStartingTxId;
						}
						// log progress
						if (op.HasTransactionId())
						{
							long now = Time.MonotonicNow();
							if (now - lastLogTime > ReplayTransactionLogInterval)
							{
								long deltaTxId = lastAppliedTxId - expectedStartingTxId + 1;
								int percent = Math.Round((float)deltaTxId / numTxns * 100);
								Log.Info("replaying edit log: " + deltaTxId + "/" + numTxns + " transactions completed. ("
									 + percent + "%)");
								lastLogTime = now;
							}
						}
						numEdits++;
						totalEdits++;
					}
					catch (FSEditLogOp.RollingUpgradeOp.RollbackException)
					{
						Log.Info("Stopped at OP_START_ROLLING_UPGRADE for rollback.");
						break;
					}
					catch (MetaRecoveryContext.RequestStopException)
					{
						MetaRecoveryContext.Log.Warn("Stopped reading edit log at " + @in.GetPosition() +
							 "/" + @in.Length());
						break;
					}
				}
			}
			finally
			{
				fsNamesys.dir.ResetLastInodeId(lastInodeId);
				if (closeOnExit)
				{
					@in.Close();
				}
				fsDir.WriteUnlock();
				fsNamesys.WriteUnlock();
				if (Log.IsTraceEnabled())
				{
					Log.Trace("replaying edit log finished");
				}
				if (FSImage.Log.IsDebugEnabled())
				{
					DumpOpCounts(opCounts);
				}
			}
			return numEdits;
		}

		// allocate and update last allocated inode id
		/// <exception cref="System.IO.IOException"/>
		private long GetAndUpdateLastInodeId(long inodeIdFromOp, int logVersion, long lastInodeId
			)
		{
			long inodeId = inodeIdFromOp;
			if (inodeId == INodeId.GrandfatherInodeId)
			{
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddInodeId, logVersion))
				{
					throw new IOException("The layout version " + logVersion + " supports inodeId but gave bogus inodeId"
						);
				}
				inodeId = fsNamesys.dir.AllocateNewInodeId();
			}
			else
			{
				// need to reset lastInodeId. fsnamesys gets lastInodeId firstly from
				// fsimage but editlog captures more recent inodeId allocations
				if (inodeId > lastInodeId)
				{
					fsNamesys.dir.ResetLastInodeId(inodeId);
				}
			}
			return inodeId;
		}

		/// <exception cref="System.IO.IOException"/>
		private long ApplyEditLogOp(FSEditLogOp op, FSDirectory fsDir, HdfsServerConstants.StartupOption
			 startOpt, int logVersion, long lastInodeId)
		{
			long inodeId = INodeId.GrandfatherInodeId;
			if (Log.IsTraceEnabled())
			{
				Log.Trace("replaying edit log: " + op);
			}
			bool toAddRetryCache = fsNamesys.HasRetryCache() && op.HasRpcIds();
			switch (op.opCode)
			{
				case FSEditLogOpCodes.OpAdd:
				{
					FSEditLogOp.AddCloseOp addCloseOp = (FSEditLogOp.AddCloseOp)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(addCloseOp.path, logVersion
						);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug(op.opCode + ": " + path + " numblocks : " + addCloseOp.blocks
							.Length + " clientHolder " + addCloseOp.clientName + " clientMachine " + addCloseOp
							.clientMachine);
					}
					// There are 3 cases here:
					// 1. OP_ADD to create a new file
					// 2. OP_ADD to update file blocks
					// 3. OP_ADD to open file for append (old append)
					// See if the file already exists (persistBlocks call)
					INodesInPath iip = fsDir.GetINodesInPath(path, true);
					INodeFile oldFile = INodeFile.ValueOf(iip.GetLastINode(), path, true);
					if (oldFile != null && addCloseOp.overwrite)
					{
						// This is OP_ADD with overwrite
						FSDirDeleteOp.DeleteForEditLog(fsDir, path, addCloseOp.mtime);
						iip = INodesInPath.Replace(iip, iip.Length() - 1, null);
						oldFile = null;
					}
					INodeFile newFile = oldFile;
					if (oldFile == null)
					{
						// this is OP_ADD on a new file (case 1)
						// versions > 0 support per file replication
						// get name and replication
						short replication = fsNamesys.GetBlockManager().AdjustReplication(addCloseOp.replication
							);
						System.Diagnostics.Debug.Assert(addCloseOp.blocks.Length == 0);
						// add to the file tree
						inodeId = GetAndUpdateLastInodeId(addCloseOp.inodeId, logVersion, lastInodeId);
						newFile = fsDir.AddFileForEditLog(inodeId, iip.GetExistingINodes(), iip.GetLastLocalName
							(), addCloseOp.permissions, addCloseOp.aclEntries, addCloseOp.xAttrs, replication
							, addCloseOp.mtime, addCloseOp.atime, addCloseOp.blockSize, true, addCloseOp.clientName
							, addCloseOp.clientMachine, addCloseOp.storagePolicyId);
						iip = INodesInPath.Replace(iip, iip.Length() - 1, newFile);
						fsNamesys.leaseManager.AddLease(addCloseOp.clientName, path);
						// add the op into retry cache if necessary
						if (toAddRetryCache)
						{
							HdfsFileStatus stat = FSDirStatAndListingOp.CreateFileStatus(fsNamesys.dir, path, 
								HdfsFileStatus.EmptyName, newFile, BlockStoragePolicySuite.IdUnspecified, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
								.CurrentStateId, false, iip);
							fsNamesys.AddCacheEntryWithPayload(addCloseOp.rpcClientId, addCloseOp.rpcCallId, 
								stat);
						}
					}
					else
					{
						// This is OP_ADD on an existing file (old append)
						if (!oldFile.IsUnderConstruction())
						{
							// This is case 3: a call to append() on an already-closed file.
							if (FSNamesystem.Log.IsDebugEnabled())
							{
								FSNamesystem.Log.Debug("Reopening an already-closed file " + "for append");
							}
							LocatedBlock lb = fsNamesys.PrepareFileForAppend(path, iip, addCloseOp.clientName
								, addCloseOp.clientMachine, false, false, false);
							// add the op into retry cache if necessary
							if (toAddRetryCache)
							{
								HdfsFileStatus stat = FSDirStatAndListingOp.CreateFileStatus(fsNamesys.dir, path, 
									HdfsFileStatus.EmptyName, newFile, BlockStoragePolicySuite.IdUnspecified, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
									.CurrentStateId, false, iip);
								fsNamesys.AddCacheEntryWithPayload(addCloseOp.rpcClientId, addCloseOp.rpcCallId, 
									new LastBlockWithStatus(lb, stat));
							}
						}
					}
					// Fall-through for case 2.
					// Regardless of whether it's a new file or an updated file,
					// update the block list.
					// Update the salient file attributes.
					newFile.SetAccessTime(addCloseOp.atime, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					newFile.SetModificationTime(addCloseOp.mtime, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					UpdateBlocks(fsDir, addCloseOp, iip, newFile);
					break;
				}

				case FSEditLogOpCodes.OpClose:
				{
					FSEditLogOp.AddCloseOp addCloseOp = (FSEditLogOp.AddCloseOp)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(addCloseOp.path, logVersion
						);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug(op.opCode + ": " + path + " numblocks : " + addCloseOp.blocks
							.Length + " clientHolder " + addCloseOp.clientName + " clientMachine " + addCloseOp
							.clientMachine);
					}
					INodesInPath iip = fsDir.GetINodesInPath(path, true);
					INodeFile file = INodeFile.ValueOf(iip.GetLastINode(), path);
					// Update the salient file attributes.
					file.SetAccessTime(addCloseOp.atime, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					file.SetModificationTime(addCloseOp.mtime, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					UpdateBlocks(fsDir, addCloseOp, iip, file);
					// Now close the file
					if (!file.IsUnderConstruction() && logVersion <= LayoutVersion.BugfixHdfs2991Version)
					{
						// There was a bug (HDFS-2991) in hadoop < 0.23.1 where OP_CLOSE
						// could show up twice in a row. But after that version, this
						// should be fixed, so we should treat it as an error.
						throw new IOException("File is not under construction: " + path);
					}
					// One might expect that you could use removeLease(holder, path) here,
					// but OP_CLOSE doesn't serialize the holder. So, remove by path.
					if (file.IsUnderConstruction())
					{
						fsNamesys.leaseManager.RemoveLeaseWithPrefixPath(path);
						file.ToCompleteFile(file.GetModificationTime());
					}
					break;
				}

				case FSEditLogOpCodes.OpAppend:
				{
					FSEditLogOp.AppendOp appendOp = (FSEditLogOp.AppendOp)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(appendOp.path, logVersion
						);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug(op.opCode + ": " + path + " clientName " + appendOp.clientName
							 + " clientMachine " + appendOp.clientMachine + " newBlock " + appendOp.newBlock
							);
					}
					INodesInPath iip = fsDir.GetINodesInPath4Write(path);
					INodeFile file = INodeFile.ValueOf(iip.GetLastINode(), path);
					if (!file.IsUnderConstruction())
					{
						LocatedBlock lb = fsNamesys.PrepareFileForAppend(path, iip, appendOp.clientName, 
							appendOp.clientMachine, appendOp.newBlock, false, false);
						// add the op into retry cache if necessary
						if (toAddRetryCache)
						{
							HdfsFileStatus stat = FSDirStatAndListingOp.CreateFileStatus(fsNamesys.dir, path, 
								HdfsFileStatus.EmptyName, file, BlockStoragePolicySuite.IdUnspecified, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
								.CurrentStateId, false, iip);
							fsNamesys.AddCacheEntryWithPayload(appendOp.rpcClientId, appendOp.rpcCallId, new 
								LastBlockWithStatus(lb, stat));
						}
					}
					break;
				}

				case FSEditLogOpCodes.OpUpdateBlocks:
				{
					FSEditLogOp.UpdateBlocksOp updateOp = (FSEditLogOp.UpdateBlocksOp)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(updateOp.path, logVersion
						);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug(op.opCode + ": " + path + " numblocks : " + updateOp.blocks
							.Length);
					}
					INodesInPath iip = fsDir.GetINodesInPath(path, true);
					INodeFile oldFile = INodeFile.ValueOf(iip.GetLastINode(), path);
					// Update in-memory data structures
					UpdateBlocks(fsDir, updateOp, iip, oldFile);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(updateOp.rpcClientId, updateOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpAddBlock:
				{
					FSEditLogOp.AddBlockOp addBlockOp = (FSEditLogOp.AddBlockOp)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(addBlockOp.GetPath(), logVersion
						);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug(op.opCode + ": " + path + " new block id : " + addBlockOp.
							GetLastBlock().GetBlockId());
					}
					INodeFile oldFile = INodeFile.ValueOf(fsDir.GetINode(path), path);
					// add the new block to the INodeFile
					AddNewBlock(fsDir, addBlockOp, oldFile);
					break;
				}

				case FSEditLogOpCodes.OpSetReplication:
				{
					FSEditLogOp.SetReplicationOp setReplicationOp = (FSEditLogOp.SetReplicationOp)op;
					short replication = fsNamesys.GetBlockManager().AdjustReplication(setReplicationOp
						.replication);
					FSDirAttrOp.UnprotectedSetReplication(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setReplicationOp.path, logVersion), replication, null);
					break;
				}

				case FSEditLogOpCodes.OpConcatDelete:
				{
					FSEditLogOp.ConcatDeleteOp concatDeleteOp = (FSEditLogOp.ConcatDeleteOp)op;
					string trg = FSImageFormat.RenameReservedPathsOnUpgrade(concatDeleteOp.trg, logVersion
						);
					string[] srcs = new string[concatDeleteOp.srcs.Length];
					for (int i = 0; i < srcs.Length; i++)
					{
						srcs[i] = FSImageFormat.RenameReservedPathsOnUpgrade(concatDeleteOp.srcs[i], logVersion
							);
					}
					INodesInPath targetIIP = fsDir.GetINodesInPath4Write(trg);
					INodeFile[] srcFiles = new INodeFile[srcs.Length];
					for (int i_1 = 0; i_1 < srcs.Length; i_1++)
					{
						INodesInPath srcIIP = fsDir.GetINodesInPath4Write(srcs[i_1]);
						srcFiles[i_1] = srcIIP.GetLastINode().AsFile();
					}
					FSDirConcatOp.UnprotectedConcat(fsDir, targetIIP, srcFiles, concatDeleteOp.timestamp
						);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(concatDeleteOp.rpcClientId, concatDeleteOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRenameOld:
				{
					FSEditLogOp.RenameOldOp renameOp = (FSEditLogOp.RenameOldOp)op;
					string src = FSImageFormat.RenameReservedPathsOnUpgrade(renameOp.src, logVersion);
					string dst = FSImageFormat.RenameReservedPathsOnUpgrade(renameOp.dst, logVersion);
					FSDirRenameOp.RenameForEditLog(fsDir, src, dst, renameOp.timestamp);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpDelete:
				{
					FSEditLogOp.DeleteOp deleteOp = (FSEditLogOp.DeleteOp)op;
					FSDirDeleteOp.DeleteForEditLog(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade(
						deleteOp.path, logVersion), deleteOp.timestamp);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(deleteOp.rpcClientId, deleteOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpMkdir:
				{
					FSEditLogOp.MkdirOp mkdirOp = (FSEditLogOp.MkdirOp)op;
					inodeId = GetAndUpdateLastInodeId(mkdirOp.inodeId, logVersion, lastInodeId);
					FSDirMkdirOp.MkdirForEditLog(fsDir, inodeId, FSImageFormat.RenameReservedPathsOnUpgrade
						(mkdirOp.path, logVersion), mkdirOp.permissions, mkdirOp.aclEntries, mkdirOp.timestamp
						);
					break;
				}

				case FSEditLogOpCodes.OpSetGenstampV1:
				{
					FSEditLogOp.SetGenstampV1Op setGenstampV1Op = (FSEditLogOp.SetGenstampV1Op)op;
					fsNamesys.GetBlockIdManager().SetGenerationStampV1(setGenstampV1Op.genStampV1);
					break;
				}

				case FSEditLogOpCodes.OpSetPermissions:
				{
					FSEditLogOp.SetPermissionsOp setPermissionsOp = (FSEditLogOp.SetPermissionsOp)op;
					FSDirAttrOp.UnprotectedSetPermission(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setPermissionsOp.src, logVersion), setPermissionsOp.permissions);
					break;
				}

				case FSEditLogOpCodes.OpSetOwner:
				{
					FSEditLogOp.SetOwnerOp setOwnerOp = (FSEditLogOp.SetOwnerOp)op;
					FSDirAttrOp.UnprotectedSetOwner(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setOwnerOp.src, logVersion), setOwnerOp.username, setOwnerOp.groupname);
					break;
				}

				case FSEditLogOpCodes.OpSetNsQuota:
				{
					FSEditLogOp.SetNSQuotaOp setNSQuotaOp = (FSEditLogOp.SetNSQuotaOp)op;
					FSDirAttrOp.UnprotectedSetQuota(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setNSQuotaOp.src, logVersion), setNSQuotaOp.nsQuota, HdfsConstants.QuotaDontSet
						, null);
					break;
				}

				case FSEditLogOpCodes.OpClearNsQuota:
				{
					FSEditLogOp.ClearNSQuotaOp clearNSQuotaOp = (FSEditLogOp.ClearNSQuotaOp)op;
					FSDirAttrOp.UnprotectedSetQuota(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(clearNSQuotaOp.src, logVersion), HdfsConstants.QuotaReset, HdfsConstants.QuotaDontSet
						, null);
					break;
				}

				case FSEditLogOpCodes.OpSetQuota:
				{
					FSEditLogOp.SetQuotaOp setQuotaOp = (FSEditLogOp.SetQuotaOp)op;
					FSDirAttrOp.UnprotectedSetQuota(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setQuotaOp.src, logVersion), setQuotaOp.nsQuota, setQuotaOp.dsQuota, null);
					break;
				}

				case FSEditLogOpCodes.OpSetQuotaByStoragetype:
				{
					FSEditLogOp.SetQuotaByStorageTypeOp setQuotaByStorageTypeOp = (FSEditLogOp.SetQuotaByStorageTypeOp
						)op;
					FSDirAttrOp.UnprotectedSetQuota(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(setQuotaByStorageTypeOp.src, logVersion), HdfsConstants.QuotaDontSet, setQuotaByStorageTypeOp
						.dsQuota, setQuotaByStorageTypeOp.type);
					break;
				}

				case FSEditLogOpCodes.OpTimes:
				{
					FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp)op;
					FSDirAttrOp.UnprotectedSetTimes(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade
						(timesOp.path, logVersion), timesOp.mtime, timesOp.atime, true);
					break;
				}

				case FSEditLogOpCodes.OpSymlink:
				{
					if (!FileSystem.AreSymlinksEnabled())
					{
						throw new IOException("Symlinks not supported - please remove symlink before upgrading to this version of HDFS"
							);
					}
					FSEditLogOp.SymlinkOp symlinkOp = (FSEditLogOp.SymlinkOp)op;
					inodeId = GetAndUpdateLastInodeId(symlinkOp.inodeId, logVersion, lastInodeId);
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(symlinkOp.path, logVersion
						);
					INodesInPath iip = fsDir.GetINodesInPath(path, false);
					FSDirSymlinkOp.UnprotectedAddSymlink(fsDir, iip.GetExistingINodes(), iip.GetLastLocalName
						(), inodeId, symlinkOp.value, symlinkOp.mtime, symlinkOp.atime, symlinkOp.permissionStatus
						);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(symlinkOp.rpcClientId, symlinkOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRename:
				{
					FSEditLogOp.RenameOp renameOp = (FSEditLogOp.RenameOp)op;
					FSDirRenameOp.RenameForEditLog(fsDir, FSImageFormat.RenameReservedPathsOnUpgrade(
						renameOp.src, logVersion), FSImageFormat.RenameReservedPathsOnUpgrade(renameOp.dst
						, logVersion), renameOp.timestamp, renameOp.options);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpGetDelegationToken:
				{
					FSEditLogOp.GetDelegationTokenOp getDelegationTokenOp = (FSEditLogOp.GetDelegationTokenOp
						)op;
					fsNamesys.GetDelegationTokenSecretManager().AddPersistedDelegationToken(getDelegationTokenOp
						.token, getDelegationTokenOp.expiryTime);
					break;
				}

				case FSEditLogOpCodes.OpRenewDelegationToken:
				{
					FSEditLogOp.RenewDelegationTokenOp renewDelegationTokenOp = (FSEditLogOp.RenewDelegationTokenOp
						)op;
					fsNamesys.GetDelegationTokenSecretManager().UpdatePersistedTokenRenewal(renewDelegationTokenOp
						.token, renewDelegationTokenOp.expiryTime);
					break;
				}

				case FSEditLogOpCodes.OpCancelDelegationToken:
				{
					FSEditLogOp.CancelDelegationTokenOp cancelDelegationTokenOp = (FSEditLogOp.CancelDelegationTokenOp
						)op;
					fsNamesys.GetDelegationTokenSecretManager().UpdatePersistedTokenCancellation(cancelDelegationTokenOp
						.token);
					break;
				}

				case FSEditLogOpCodes.OpUpdateMasterKey:
				{
					FSEditLogOp.UpdateMasterKeyOp updateMasterKeyOp = (FSEditLogOp.UpdateMasterKeyOp)
						op;
					fsNamesys.GetDelegationTokenSecretManager().UpdatePersistedMasterKey(updateMasterKeyOp
						.key);
					break;
				}

				case FSEditLogOpCodes.OpReassignLease:
				{
					FSEditLogOp.ReassignLeaseOp reassignLeaseOp = (FSEditLogOp.ReassignLeaseOp)op;
					LeaseManager.Lease lease = fsNamesys.leaseManager.GetLease(reassignLeaseOp.leaseHolder
						);
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(reassignLeaseOp.path, logVersion
						);
					INodeFile pendingFile = fsDir.GetINode(path).AsFile();
					Preconditions.CheckState(pendingFile.IsUnderConstruction());
					fsNamesys.ReassignLeaseInternal(lease, path, reassignLeaseOp.newHolder, pendingFile
						);
					break;
				}

				case FSEditLogOpCodes.OpStartLogSegment:
				case FSEditLogOpCodes.OpEndLogSegment:
				{
					// no data in here currently.
					break;
				}

				case FSEditLogOpCodes.OpCreateSnapshot:
				{
					FSEditLogOp.CreateSnapshotOp createSnapshotOp = (FSEditLogOp.CreateSnapshotOp)op;
					string snapshotRoot = FSImageFormat.RenameReservedPathsOnUpgrade(createSnapshotOp
						.snapshotRoot, logVersion);
					INodesInPath iip = fsDir.GetINodesInPath4Write(snapshotRoot);
					string path = fsNamesys.GetSnapshotManager().CreateSnapshot(iip, snapshotRoot, createSnapshotOp
						.snapshotName);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntryWithPayload(createSnapshotOp.rpcClientId, createSnapshotOp
							.rpcCallId, path);
					}
					break;
				}

				case FSEditLogOpCodes.OpDeleteSnapshot:
				{
					FSEditLogOp.DeleteSnapshotOp deleteSnapshotOp = (FSEditLogOp.DeleteSnapshotOp)op;
					INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
					IList<INode> removedINodes = new ChunkedArrayList<INode>();
					string snapshotRoot = FSImageFormat.RenameReservedPathsOnUpgrade(deleteSnapshotOp
						.snapshotRoot, logVersion);
					INodesInPath iip = fsDir.GetINodesInPath4Write(snapshotRoot);
					fsNamesys.GetSnapshotManager().DeleteSnapshot(iip, deleteSnapshotOp.snapshotName, 
						collectedBlocks, removedINodes);
					fsNamesys.RemoveBlocksAndUpdateSafemodeTotal(collectedBlocks);
					collectedBlocks.Clear();
					fsNamesys.dir.RemoveFromInodeMap(removedINodes);
					removedINodes.Clear();
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(deleteSnapshotOp.rpcClientId, deleteSnapshotOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRenameSnapshot:
				{
					FSEditLogOp.RenameSnapshotOp renameSnapshotOp = (FSEditLogOp.RenameSnapshotOp)op;
					string snapshotRoot = FSImageFormat.RenameReservedPathsOnUpgrade(renameSnapshotOp
						.snapshotRoot, logVersion);
					INodesInPath iip = fsDir.GetINodesInPath4Write(snapshotRoot);
					fsNamesys.GetSnapshotManager().RenameSnapshot(iip, snapshotRoot, renameSnapshotOp
						.snapshotOldName, renameSnapshotOp.snapshotNewName);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(renameSnapshotOp.rpcClientId, renameSnapshotOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpAllowSnapshot:
				{
					FSEditLogOp.AllowSnapshotOp allowSnapshotOp = (FSEditLogOp.AllowSnapshotOp)op;
					string snapshotRoot = FSImageFormat.RenameReservedPathsOnUpgrade(allowSnapshotOp.
						snapshotRoot, logVersion);
					fsNamesys.GetSnapshotManager().SetSnapshottable(snapshotRoot, false);
					break;
				}

				case FSEditLogOpCodes.OpDisallowSnapshot:
				{
					FSEditLogOp.DisallowSnapshotOp disallowSnapshotOp = (FSEditLogOp.DisallowSnapshotOp
						)op;
					string snapshotRoot = FSImageFormat.RenameReservedPathsOnUpgrade(disallowSnapshotOp
						.snapshotRoot, logVersion);
					fsNamesys.GetSnapshotManager().ResetSnapshottable(snapshotRoot);
					break;
				}

				case FSEditLogOpCodes.OpSetGenstampV2:
				{
					FSEditLogOp.SetGenstampV2Op setGenstampV2Op = (FSEditLogOp.SetGenstampV2Op)op;
					fsNamesys.GetBlockIdManager().SetGenerationStampV2(setGenstampV2Op.genStampV2);
					break;
				}

				case FSEditLogOpCodes.OpAllocateBlockId:
				{
					FSEditLogOp.AllocateBlockIdOp allocateBlockIdOp = (FSEditLogOp.AllocateBlockIdOp)
						op;
					fsNamesys.GetBlockIdManager().SetLastAllocatedBlockId(allocateBlockIdOp.blockId);
					break;
				}

				case FSEditLogOpCodes.OpRollingUpgradeStart:
				{
					if (startOpt == HdfsServerConstants.StartupOption.Rollingupgrade)
					{
						HdfsServerConstants.RollingUpgradeStartupOption rollingUpgradeOpt = startOpt.GetRollingUpgradeStartupOption
							();
						if (rollingUpgradeOpt == HdfsServerConstants.RollingUpgradeStartupOption.Rollback)
						{
							throw new FSEditLogOp.RollingUpgradeOp.RollbackException();
						}
						else
						{
							if (rollingUpgradeOpt == HdfsServerConstants.RollingUpgradeStartupOption.Downgrade)
							{
								//ignore upgrade marker
								break;
							}
						}
					}
					// start rolling upgrade
					long startTime = ((FSEditLogOp.RollingUpgradeOp)op).GetTime();
					fsNamesys.StartRollingUpgradeInternal(startTime);
					fsNamesys.TriggerRollbackCheckpoint();
					break;
				}

				case FSEditLogOpCodes.OpRollingUpgradeFinalize:
				{
					long finalizeTime = ((FSEditLogOp.RollingUpgradeOp)op).GetTime();
					if (fsNamesys.IsRollingUpgrade())
					{
						// Only do it when NN is actually doing rolling upgrade.
						// We can get FINALIZE without corresponding START, if NN is restarted
						// before this op is consumed and a new checkpoint is created.
						fsNamesys.FinalizeRollingUpgradeInternal(finalizeTime);
					}
					fsNamesys.GetFSImage().UpdateStorageVersion();
					fsNamesys.GetFSImage().RenameCheckpoint(NNStorage.NameNodeFile.ImageRollback, NNStorage.NameNodeFile
						.Image);
					break;
				}

				case FSEditLogOpCodes.OpAddCacheDirective:
				{
					FSEditLogOp.AddCacheDirectiveInfoOp addOp = (FSEditLogOp.AddCacheDirectiveInfoOp)
						op;
					CacheDirectiveInfo result = fsNamesys.GetCacheManager().AddDirectiveFromEditLog(addOp
						.directive);
					if (toAddRetryCache)
					{
						long id = result.GetId();
						fsNamesys.AddCacheEntryWithPayload(op.rpcClientId, op.rpcCallId, id);
					}
					break;
				}

				case FSEditLogOpCodes.OpModifyCacheDirective:
				{
					FSEditLogOp.ModifyCacheDirectiveInfoOp modifyOp = (FSEditLogOp.ModifyCacheDirectiveInfoOp
						)op;
					fsNamesys.GetCacheManager().ModifyDirectiveFromEditLog(modifyOp.directive);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(op.rpcClientId, op.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRemoveCacheDirective:
				{
					FSEditLogOp.RemoveCacheDirectiveInfoOp removeOp = (FSEditLogOp.RemoveCacheDirectiveInfoOp
						)op;
					fsNamesys.GetCacheManager().RemoveDirective(removeOp.id, null);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(op.rpcClientId, op.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpAddCachePool:
				{
					FSEditLogOp.AddCachePoolOp addOp = (FSEditLogOp.AddCachePoolOp)op;
					fsNamesys.GetCacheManager().AddCachePool(addOp.info);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(op.rpcClientId, op.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpModifyCachePool:
				{
					FSEditLogOp.ModifyCachePoolOp modifyOp = (FSEditLogOp.ModifyCachePoolOp)op;
					fsNamesys.GetCacheManager().ModifyCachePool(modifyOp.info);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(op.rpcClientId, op.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRemoveCachePool:
				{
					FSEditLogOp.RemoveCachePoolOp removeOp = (FSEditLogOp.RemoveCachePoolOp)op;
					fsNamesys.GetCacheManager().RemoveCachePool(removeOp.poolName);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(op.rpcClientId, op.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpSetAcl:
				{
					FSEditLogOp.SetAclOp setAclOp = (FSEditLogOp.SetAclOp)op;
					FSDirAclOp.UnprotectedSetAcl(fsDir, setAclOp.src, setAclOp.aclEntries, true);
					break;
				}

				case FSEditLogOpCodes.OpSetXattr:
				{
					FSEditLogOp.SetXAttrOp setXAttrOp = (FSEditLogOp.SetXAttrOp)op;
					FSDirXAttrOp.UnprotectedSetXAttrs(fsDir, setXAttrOp.src, setXAttrOp.xAttrs, EnumSet
						.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace));
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(setXAttrOp.rpcClientId, setXAttrOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpRemoveXattr:
				{
					FSEditLogOp.RemoveXAttrOp removeXAttrOp = (FSEditLogOp.RemoveXAttrOp)op;
					FSDirXAttrOp.UnprotectedRemoveXAttrs(fsDir, removeXAttrOp.src, removeXAttrOp.xAttrs
						);
					if (toAddRetryCache)
					{
						fsNamesys.AddCacheEntry(removeXAttrOp.rpcClientId, removeXAttrOp.rpcCallId);
					}
					break;
				}

				case FSEditLogOpCodes.OpTruncate:
				{
					FSEditLogOp.TruncateOp truncateOp = (FSEditLogOp.TruncateOp)op;
					fsDir.UnprotectedTruncate(truncateOp.src, truncateOp.clientName, truncateOp.clientMachine
						, truncateOp.newLength, truncateOp.timestamp, truncateOp.truncateBlock);
					break;
				}

				case FSEditLogOpCodes.OpSetStoragePolicy:
				{
					FSEditLogOp.SetStoragePolicyOp setStoragePolicyOp = (FSEditLogOp.SetStoragePolicyOp
						)op;
					string path = FSImageFormat.RenameReservedPathsOnUpgrade(setStoragePolicyOp.path, 
						logVersion);
					INodesInPath iip = fsDir.GetINodesInPath4Write(path);
					FSDirAttrOp.UnprotectedSetStoragePolicy(fsDir, fsNamesys.GetBlockManager(), iip, 
						setStoragePolicyOp.policyId);
					break;
				}

				default:
				{
					throw new IOException("Invalid operation read " + op.opCode);
				}
			}
			return inodeId;
		}

		private static string FormatEditLogReplayError(EditLogInputStream @in, long[] recentOpcodeOffsets
			, long txid)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Error replaying edit log at offset " + @in.GetPosition());
			sb.Append(".  Expected transaction ID was ").Append(txid);
			if (recentOpcodeOffsets[0] != -1)
			{
				Arrays.Sort(recentOpcodeOffsets);
				sb.Append("\nRecent opcode offsets:");
				foreach (long offset in recentOpcodeOffsets)
				{
					if (offset != -1)
					{
						sb.Append(' ').Append(offset);
					}
				}
			}
			return sb.ToString();
		}

		/// <summary>Add a new block into the given INodeFile</summary>
		/// <exception cref="System.IO.IOException"/>
		private void AddNewBlock(FSDirectory fsDir, FSEditLogOp.AddBlockOp op, INodeFile 
			file)
		{
			BlockInfoContiguous[] oldBlocks = file.GetBlocks();
			Block pBlock = op.GetPenultimateBlock();
			Block newBlock = op.GetLastBlock();
			if (pBlock != null)
			{
				// the penultimate block is not null
				Preconditions.CheckState(oldBlocks != null && oldBlocks.Length > 0);
				// compare pBlock with the last block of oldBlocks
				Block oldLastBlock = oldBlocks[oldBlocks.Length - 1];
				if (oldLastBlock.GetBlockId() != pBlock.GetBlockId() || oldLastBlock.GetGenerationStamp
					() != pBlock.GetGenerationStamp())
				{
					throw new IOException("Mismatched block IDs or generation stamps for the old last block of file "
						 + op.GetPath() + ", the old last block is " + oldLastBlock + ", and the block read from editlog is "
						 + pBlock);
				}
				oldLastBlock.SetNumBytes(pBlock.GetNumBytes());
				if (oldLastBlock is BlockInfoContiguousUnderConstruction)
				{
					fsNamesys.GetBlockManager().ForceCompleteBlock(file, (BlockInfoContiguousUnderConstruction
						)oldLastBlock);
					fsNamesys.GetBlockManager().ProcessQueuedMessagesForBlock(pBlock);
				}
			}
			else
			{
				// the penultimate block is null
				Preconditions.CheckState(oldBlocks == null || oldBlocks.Length == 0);
			}
			// add the new block
			BlockInfoContiguous newBI = new BlockInfoContiguousUnderConstruction(newBlock, file
				.GetBlockReplication());
			fsNamesys.GetBlockManager().AddBlockCollection(newBI, file);
			file.AddBlock(newBI);
			fsNamesys.GetBlockManager().ProcessQueuedMessagesForBlock(newBlock);
		}

		/// <summary>Update in-memory data structures with new block information.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void UpdateBlocks(FSDirectory fsDir, FSEditLogOp.BlockListUpdatingOp op, 
			INodesInPath iip, INodeFile file)
		{
			// Update its block list
			BlockInfoContiguous[] oldBlocks = file.GetBlocks();
			Block[] newBlocks = op.GetBlocks();
			string path = op.GetPath();
			// Are we only updating the last block's gen stamp.
			bool isGenStampUpdate = oldBlocks.Length == newBlocks.Length;
			// First, update blocks in common
			for (int i = 0; i < oldBlocks.Length && i < newBlocks.Length; i++)
			{
				BlockInfoContiguous oldBlock = oldBlocks[i];
				Block newBlock = newBlocks[i];
				bool isLastBlock = i == newBlocks.Length - 1;
				if (oldBlock.GetBlockId() != newBlock.GetBlockId() || (oldBlock.GetGenerationStamp
					() != newBlock.GetGenerationStamp() && !(isGenStampUpdate && isLastBlock)))
				{
					throw new IOException("Mismatched block IDs or generation stamps, " + "attempting to replace block "
						 + oldBlock + " with " + newBlock + " as block # " + i + "/" + newBlocks.Length 
						+ " of " + path);
				}
				oldBlock.SetNumBytes(newBlock.GetNumBytes());
				bool changeMade = oldBlock.GetGenerationStamp() != newBlock.GetGenerationStamp();
				oldBlock.SetGenerationStamp(newBlock.GetGenerationStamp());
				if (oldBlock is BlockInfoContiguousUnderConstruction && (!isLastBlock || op.ShouldCompleteLastBlock
					()))
				{
					changeMade = true;
					fsNamesys.GetBlockManager().ForceCompleteBlock(file, (BlockInfoContiguousUnderConstruction
						)oldBlock);
				}
				if (changeMade)
				{
					// The state or gen-stamp of the block has changed. So, we may be
					// able to process some messages from datanodes that we previously
					// were unable to process.
					fsNamesys.GetBlockManager().ProcessQueuedMessagesForBlock(newBlock);
				}
			}
			if (newBlocks.Length < oldBlocks.Length)
			{
				// We're removing a block from the file, e.g. abandonBlock(...)
				if (!file.IsUnderConstruction())
				{
					throw new IOException("Trying to remove a block from file " + path + " which is not under construction."
						);
				}
				if (newBlocks.Length != oldBlocks.Length - 1)
				{
					throw new IOException("Trying to remove more than one block from file " + path);
				}
				Block oldBlock = oldBlocks[oldBlocks.Length - 1];
				bool removed = fsDir.UnprotectedRemoveBlock(path, iip, file, oldBlock);
				if (!removed && !(op is FSEditLogOp.UpdateBlocksOp))
				{
					throw new IOException("Trying to delete non-existant block " + oldBlock);
				}
			}
			else
			{
				if (newBlocks.Length > oldBlocks.Length)
				{
					// We're adding blocks
					for (int i_1 = oldBlocks.Length; i_1 < newBlocks.Length; i_1++)
					{
						Block newBlock = newBlocks[i_1];
						BlockInfoContiguous newBI;
						if (!op.ShouldCompleteLastBlock())
						{
							// TODO: shouldn't this only be true for the last block?
							// what about an old-version fsync() where fsync isn't called
							// until several blocks in?
							newBI = new BlockInfoContiguousUnderConstruction(newBlock, file.GetBlockReplication
								());
						}
						else
						{
							// OP_CLOSE should add finalized blocks. This code path
							// is only executed when loading edits written by prior
							// versions of Hadoop. Current versions always log
							// OP_ADD operations as each block is allocated.
							newBI = new BlockInfoContiguous(newBlock, file.GetBlockReplication());
						}
						fsNamesys.GetBlockManager().AddBlockCollection(newBI, file);
						file.AddBlock(newBI);
						fsNamesys.GetBlockManager().ProcessQueuedMessagesForBlock(newBlock);
					}
				}
			}
		}

		private static void DumpOpCounts(EnumMap<FSEditLogOpCodes, Holder<int>> opCounts)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Summary of operations loaded from edit log:\n  ");
			Joiner.On("\n  ").WithKeyValueSeparator("=").AppendTo(sb, opCounts);
			FSImage.Log.Debug(sb.ToString());
		}

		private void IncrOpCount(FSEditLogOpCodes opCode, EnumMap<FSEditLogOpCodes, Holder
			<int>> opCounts, Step step, StartupProgress.Counter counter)
		{
			Holder<int> holder = opCounts[opCode];
			if (holder == null)
			{
				holder = new Holder<int>(1);
				opCounts[opCode] = holder;
			}
			else
			{
				holder.held++;
			}
			counter.Increment();
		}

		/// <summary>
		/// Throw appropriate exception during upgrade from 203, when editlog loading
		/// could fail due to opcode conflicts.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void Check203UpgradeFailure(int logVersion, Exception e)
		{
			// 0.20.203 version version has conflicting opcodes with the later releases.
			// The editlog must be emptied by restarting the namenode, before proceeding
			// with the upgrade.
			if (Storage.Is203LayoutVersion(logVersion) && logVersion != HdfsConstants.NamenodeLayoutVersion)
			{
				string msg = "During upgrade failed to load the editlog version " + logVersion + 
					" from release 0.20.203. Please go back to the old " + " release and restart the namenode. This empties the editlog "
					 + " and saves the namespace. Resume the upgrade after this step.";
				throw new IOException(msg, e);
			}
		}

		/// <summary>Find the last valid transaction ID in the stream.</summary>
		/// <remarks>
		/// Find the last valid transaction ID in the stream.
		/// If there are invalid or corrupt transactions in the middle of the stream,
		/// validateEditLog will skip over them.
		/// This reads through the stream but does not close it.
		/// </remarks>
		internal static FSEditLogLoader.EditLogValidation ValidateEditLog(EditLogInputStream
			 @in)
		{
			long lastPos = 0;
			long lastTxId = HdfsConstants.InvalidTxid;
			long numValid = 0;
			FSEditLogOp op = null;
			while (true)
			{
				lastPos = @in.GetPosition();
				try
				{
					if ((op = @in.ReadOp()) == null)
					{
						break;
					}
				}
				catch (Exception t)
				{
					FSImage.Log.Warn("Caught exception after reading " + numValid + " ops from " + @in
						 + " while determining its valid length." + "Position was " + lastPos, t);
					@in.Resync();
					FSImage.Log.Warn("After resync, position is " + @in.GetPosition());
					continue;
				}
				if (lastTxId == HdfsConstants.InvalidTxid || op.GetTransactionId() > lastTxId)
				{
					lastTxId = op.GetTransactionId();
				}
				numValid++;
			}
			return new FSEditLogLoader.EditLogValidation(lastPos, lastTxId, false);
		}

		internal static FSEditLogLoader.EditLogValidation ScanEditLog(EditLogInputStream 
			@in)
		{
			long lastPos = 0;
			long lastTxId = HdfsConstants.InvalidTxid;
			long numValid = 0;
			FSEditLogOp op = null;
			while (true)
			{
				lastPos = @in.GetPosition();
				try
				{
					if ((op = @in.ReadOp()) == null)
					{
						// TODO
						break;
					}
				}
				catch (Exception t)
				{
					FSImage.Log.Warn("Caught exception after reading " + numValid + " ops from " + @in
						 + " while determining its valid length." + "Position was " + lastPos, t);
					@in.Resync();
					FSImage.Log.Warn("After resync, position is " + @in.GetPosition());
					continue;
				}
				if (lastTxId == HdfsConstants.InvalidTxid || op.GetTransactionId() > lastTxId)
				{
					lastTxId = op.GetTransactionId();
				}
				numValid++;
			}
			return new FSEditLogLoader.EditLogValidation(lastPos, lastTxId, false);
		}

		internal class EditLogValidation
		{
			private readonly long validLength;

			private readonly long endTxId;

			private readonly bool hasCorruptHeader;

			internal EditLogValidation(long validLength, long endTxId, bool hasCorruptHeader)
			{
				this.validLength = validLength;
				this.endTxId = endTxId;
				this.hasCorruptHeader = hasCorruptHeader;
			}

			internal virtual long GetValidLength()
			{
				return validLength;
			}

			internal virtual long GetEndTxId()
			{
				return endTxId;
			}

			internal virtual bool HasCorruptHeader()
			{
				return hasCorruptHeader;
			}
		}

		/// <summary>Stream wrapper that keeps track of the current stream position.</summary>
		/// <remarks>
		/// Stream wrapper that keeps track of the current stream position.
		/// This stream also allows us to set a limit on how many bytes we can read
		/// without getting an exception.
		/// </remarks>
		public class PositionTrackingInputStream : FilterInputStream, StreamLimiter
		{
			private long curPos = 0;

			private long markPos = -1;

			private long limitPos = long.MaxValue;

			public PositionTrackingInputStream(InputStream @is)
				: base(@is)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckLimit(long amt)
			{
				long extra = (curPos + amt) - limitPos;
				if (extra > 0)
				{
					throw new IOException("Tried to read " + amt + " byte(s) past " + "the limit at offset "
						 + limitPos);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				CheckLimit(1);
				int ret = base.Read();
				if (ret != -1)
				{
					curPos++;
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] data)
			{
				CheckLimit(data.Length);
				int ret = base.Read(data);
				if (ret > 0)
				{
					curPos += ret;
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] data, int offset, int length)
			{
				CheckLimit(length);
				int ret = base.Read(data, offset, length);
				if (ret > 0)
				{
					curPos += ret;
				}
				return ret;
			}

			public virtual void SetLimit(long limit)
			{
				limitPos = curPos + limit;
			}

			public virtual void ClearLimit()
			{
				limitPos = long.MaxValue;
			}

			public override void Mark(int limit)
			{
				base.Mark(limit);
				markPos = curPos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Reset()
			{
				if (markPos == -1)
				{
					throw new IOException("Not marked!");
				}
				base.Reset();
				curPos = markPos;
				markPos = -1;
			}

			public virtual long GetPos()
			{
				return curPos;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Skip(long amt)
			{
				long extra = (curPos + amt) - limitPos;
				if (extra > 0)
				{
					throw new IOException("Tried to skip " + extra + " bytes past " + "the limit at offset "
						 + limitPos);
				}
				long ret = base.Skip(amt);
				curPos += ret;
				return ret;
			}
		}

		public virtual long GetLastAppliedTxId()
		{
			return lastAppliedTxId;
		}

		/// <summary>
		/// Creates a Step used for updating startup progress, populated with
		/// information from the given edits.
		/// </summary>
		/// <remarks>
		/// Creates a Step used for updating startup progress, populated with
		/// information from the given edits.  The step always includes the log's name.
		/// If the log has a known length, then the length is included in the step too.
		/// </remarks>
		/// <param name="edits">EditLogInputStream to use for populating step</param>
		/// <returns>Step populated with information from edits</returns>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private static Step CreateStartupProgressStep(EditLogInputStream edits)
		{
			long length = edits.Length();
			string name = edits.GetCurrentStreamName();
			return length != -1 ? new Step(name, length) : new Step(name);
		}
	}
}
