using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class provides rudimentary checking of DFS volumes for errors and
	/// sub-optimal conditions.
	/// </summary>
	/// <remarks>
	/// This class provides rudimentary checking of DFS volumes for errors and
	/// sub-optimal conditions.
	/// <p>The tool scans all files and directories, starting from an indicated
	/// root path. The following abnormal conditions are detected and handled:</p>
	/// <ul>
	/// <li>files with blocks that are completely missing from all datanodes.<br/>
	/// In this case the tool can perform one of the following actions:
	/// <ul>
	/// <li>none (
	/// <see cref="#FIXING_NONE"/>
	/// )</li>
	/// <li>move corrupted files to /lost+found directory on DFS
	/// (
	/// <see cref="#FIXING_MOVE"/>
	/// ). Remaining data blocks are saved as a
	/// block chains, representing longest consecutive series of valid blocks.</li>
	/// <li>delete corrupted files (
	/// <see cref="#FIXING_DELETE"/>
	/// )</li>
	/// </ul>
	/// </li>
	/// <li>detect files with under-replicated or over-replicated blocks</li>
	/// </ul>
	/// Additionally, the tool collects a detailed overall DFS statistics, and
	/// optionally can print detailed statistics on block locations and replication
	/// factors of each file.
	/// </remarks>
	public class NamenodeFsck : DataEncryptionKeyFactory
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(NameNode).FullName);

		public const string CorruptStatus = "is CORRUPT";

		public const string HealthyStatus = "is HEALTHY";

		public const string DecommissioningStatus = "is DECOMMISSIONING";

		public const string DecommissionedStatus = "is DECOMMISSIONED";

		public const string NonexistentStatus = "does not exist";

		public const string FailureStatus = "FAILED";

		private readonly NameNode namenode;

		private readonly NetworkTopology networktopology;

		private readonly int totalDatanodes;

		private readonly IPAddress remoteAddress;

		private string lostFound = null;

		private bool lfInited = false;

		private bool lfInitedOk = false;

		private bool showFiles = false;

		private bool showOpenFiles = false;

		private bool showBlocks = false;

		private bool showLocations = false;

		private bool showRacks = false;

		private bool showStoragePolcies = false;

		private bool showCorruptFileBlocks = false;

		/// <summary>
		/// True if we encountered an internal error during FSCK, such as not being
		/// able to delete a corrupt file.
		/// </summary>
		private bool internalError = false;

		/// <summary>True if the user specified the -move option.</summary>
		/// <remarks>
		/// True if the user specified the -move option.
		/// Whe this option is in effect, we will copy salvaged blocks into the lost
		/// and found.
		/// </remarks>
		private bool doMove = false;

		/// <summary>True if the user specified the -delete option.</summary>
		/// <remarks>
		/// True if the user specified the -delete option.
		/// Whe this option is in effect, we will delete corrupted files.
		/// </remarks>
		private bool doDelete = false;

		internal string path = "/";

		private string blockIds = null;

		private readonly string[] currentCookie = new string[] { null };

		private readonly Configuration conf;

		private readonly PrintWriter @out;

		private IList<string> snapshottableDirs = null;

		private readonly BlockPlacementPolicy bpPolicy;

		private StoragePolicySummary storageTypeSummary = null;

		/// <summary>Filesystem checker.</summary>
		/// <param name="conf">configuration (namenode config)</param>
		/// <param name="namenode">namenode that this fsck is going to use</param>
		/// <param name="pmap">key=value[] map passed to the http servlet as url parameters</param>
		/// <param name="out">output stream to write the fsck output</param>
		/// <param name="totalDatanodes">number of live datanodes</param>
		/// <param name="remoteAddress">source address of the fsck request</param>
		internal NamenodeFsck(Configuration conf, NameNode namenode, NetworkTopology networktopology
			, IDictionary<string, string[]> pmap, PrintWriter @out, int totalDatanodes, IPAddress
			 remoteAddress)
		{
			// return string marking fsck status
			// We return back N files that are corrupt; the list of files returned is
			// ordered by block id; to allow continuation support, pass in the last block
			// # from previous call
			this.conf = conf;
			this.namenode = namenode;
			this.networktopology = networktopology;
			this.@out = @out;
			this.totalDatanodes = totalDatanodes;
			this.remoteAddress = remoteAddress;
			this.bpPolicy = BlockPlacementPolicy.GetInstance(conf, null, networktopology, namenode
				.GetNamesystem().GetBlockManager().GetDatanodeManager().GetHost2DatanodeMap());
			for (IEnumerator<string> it = pmap.Keys.GetEnumerator(); it.HasNext(); )
			{
				string key = it.Next();
				if (key.Equals("path"))
				{
					this.path = pmap["path"][0];
				}
				else
				{
					if (key.Equals("move"))
					{
						this.doMove = true;
					}
					else
					{
						if (key.Equals("delete"))
						{
							this.doDelete = true;
						}
						else
						{
							if (key.Equals("files"))
							{
								this.showFiles = true;
							}
							else
							{
								if (key.Equals("blocks"))
								{
									this.showBlocks = true;
								}
								else
								{
									if (key.Equals("locations"))
									{
										this.showLocations = true;
									}
									else
									{
										if (key.Equals("racks"))
										{
											this.showRacks = true;
										}
										else
										{
											if (key.Equals("storagepolicies"))
											{
												this.showStoragePolcies = true;
											}
											else
											{
												if (key.Equals("openforwrite"))
												{
													this.showOpenFiles = true;
												}
												else
												{
													if (key.Equals("listcorruptfileblocks"))
													{
														this.showCorruptFileBlocks = true;
													}
													else
													{
														if (key.Equals("startblockafter"))
														{
															this.currentCookie[0] = pmap["startblockafter"][0];
														}
														else
														{
															if (key.Equals("includeSnapshots"))
															{
																this.snapshottableDirs = new AList<string>();
															}
															else
															{
																if (key.Equals("blockId"))
																{
																	this.blockIds = pmap["blockId"][0];
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/// <summary>Check block information given a blockId number</summary>
		public virtual void BlockIdCK(string blockId)
		{
			if (blockId == null)
			{
				@out.WriteLine("Please provide valid blockId!");
				return;
			}
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			try
			{
				//get blockInfo
				Block block = new Block(Block.GetBlockId(blockId));
				//find which file this block belongs to
				BlockInfoContiguous blockInfo = bm.GetStoredBlock(block);
				if (blockInfo == null)
				{
					@out.WriteLine("Block " + blockId + " " + NonexistentStatus);
					Log.Warn("Block " + blockId + " " + NonexistentStatus);
					return;
				}
				BlockCollection bc = bm.GetBlockCollection(blockInfo);
				INode iNode = (INode)bc;
				NumberReplicas numberReplicas = bm.CountNodes(block);
				@out.WriteLine("Block Id: " + blockId);
				@out.WriteLine("Block belongs to: " + iNode.GetFullPathName());
				@out.WriteLine("No. of Expected Replica: " + bc.GetBlockReplication());
				@out.WriteLine("No. of live Replica: " + numberReplicas.LiveReplicas());
				@out.WriteLine("No. of excess Replica: " + numberReplicas.ExcessReplicas());
				@out.WriteLine("No. of stale Replica: " + numberReplicas.ReplicasOnStaleNodes());
				@out.WriteLine("No. of decommission Replica: " + numberReplicas.DecommissionedReplicas
					());
				@out.WriteLine("No. of corrupted Replica: " + numberReplicas.CorruptReplicas());
				//record datanodes that have corrupted block replica
				ICollection<DatanodeDescriptor> corruptionRecord = null;
				if (bm.GetCorruptReplicas(block) != null)
				{
					corruptionRecord = bm.GetCorruptReplicas(block);
				}
				//report block replicas status on datanodes
				for (int idx = (blockInfo.NumNodes() - 1); idx >= 0; idx--)
				{
					DatanodeDescriptor dn = blockInfo.GetDatanode(idx);
					@out.Write("Block replica on datanode/rack: " + dn.GetHostName() + dn.GetNetworkLocation
						() + " ");
					if (corruptionRecord != null && corruptionRecord.Contains(dn))
					{
						@out.Write(CorruptStatus + "\t ReasonCode: " + bm.GetCorruptReason(block, dn));
					}
					else
					{
						if (dn.IsDecommissioned())
						{
							@out.Write(DecommissionedStatus);
						}
						else
						{
							if (dn.IsDecommissionInProgress())
							{
								@out.Write(DecommissioningStatus);
							}
							else
							{
								@out.Write(HealthyStatus);
							}
						}
					}
					@out.Write("\n");
				}
			}
			catch (Exception e)
			{
				string errMsg = "Fsck on blockId '" + blockId;
				Log.Warn(errMsg, e);
				@out.WriteLine(e.Message);
				@out.Write("\n\n" + errMsg);
				Log.Warn("Error in looking up block", e);
			}
		}

		/// <summary>Check files on DFS, starting from the indicated path.</summary>
		public virtual void Fsck()
		{
			long startTime = Time.MonotonicNow();
			try
			{
				if (blockIds != null)
				{
					string[] blocks = blockIds.Split(" ");
					StringBuilder sb = new StringBuilder();
					sb.Append("FSCK started by " + UserGroupInformation.GetCurrentUser() + " from " +
						 remoteAddress + " at " + new DateTime());
					@out.WriteLine(sb.ToString());
					sb.Append(" for blockIds: \n");
					foreach (string blk in blocks)
					{
						if (blk == null || !blk.Contains(Block.BlockFilePrefix))
						{
							@out.WriteLine("Incorrect blockId format: " + blk);
							continue;
						}
						@out.Write("\n");
						BlockIdCK(blk);
						sb.Append(blk + "\n");
					}
					Log.Info(sb.ToString());
					namenode.GetNamesystem().LogFsckEvent("/", remoteAddress);
					@out.Flush();
					return;
				}
				string msg = "FSCK started by " + UserGroupInformation.GetCurrentUser() + " from "
					 + remoteAddress + " for path " + path + " at " + new DateTime();
				Log.Info(msg);
				@out.WriteLine(msg);
				namenode.GetNamesystem().LogFsckEvent(path, remoteAddress);
				if (snapshottableDirs != null)
				{
					SnapshottableDirectoryStatus[] snapshotDirs = namenode.GetRpcServer().GetSnapshottableDirListing
						();
					if (snapshotDirs != null)
					{
						foreach (SnapshottableDirectoryStatus dir in snapshotDirs)
						{
							snapshottableDirs.AddItem(dir.GetFullPath().ToString());
						}
					}
				}
				HdfsFileStatus file = namenode.GetRpcServer().GetFileInfo(path);
				if (file != null)
				{
					if (showCorruptFileBlocks)
					{
						ListCorruptFileBlocks();
						return;
					}
					if (this.showStoragePolcies)
					{
						storageTypeSummary = new StoragePolicySummary(namenode.GetNamesystem().GetBlockManager
							().GetStoragePolicies());
					}
					NamenodeFsck.Result res = new NamenodeFsck.Result(conf);
					Check(path, file, res);
					@out.WriteLine(res);
					@out.WriteLine(" Number of data-nodes:\t\t" + totalDatanodes);
					@out.WriteLine(" Number of racks:\t\t" + networktopology.GetNumOfRacks());
					if (this.showStoragePolcies)
					{
						@out.Write(storageTypeSummary.ToString());
					}
					@out.WriteLine("FSCK ended at " + new DateTime() + " in " + (Time.MonotonicNow() 
						- startTime + " milliseconds"));
					// If there were internal errors during the fsck operation, we want to
					// return FAILURE_STATUS, even if those errors were not immediately
					// fatal.  Otherwise many unit tests will pass even when there are bugs.
					if (internalError)
					{
						throw new IOException("fsck encountered internal errors!");
					}
					// DFSck client scans for the string HEALTHY/CORRUPT to check the status
					// of file system and return appropriate code. Changing the output
					// string might break testcases. Also note this must be the last line 
					// of the report.
					if (res.IsHealthy())
					{
						@out.Write("\n\nThe filesystem under path '" + path + "' " + HealthyStatus);
					}
					else
					{
						@out.Write("\n\nThe filesystem under path '" + path + "' " + CorruptStatus);
					}
				}
				else
				{
					@out.Write("\n\nPath '" + path + "' " + NonexistentStatus);
				}
			}
			catch (Exception e)
			{
				string errMsg = "Fsck on path '" + path + "' " + FailureStatus;
				Log.Warn(errMsg, e);
				@out.WriteLine("FSCK ended at " + new DateTime() + " in " + (Time.MonotonicNow() 
					- startTime + " milliseconds"));
				@out.WriteLine(e.Message);
				@out.Write("\n\n" + errMsg);
			}
			finally
			{
				@out.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ListCorruptFileBlocks()
		{
			ICollection<FSNamesystem.CorruptFileBlockInfo> corruptFiles = namenode.GetNamesystem
				().ListCorruptFileBlocks(path, currentCookie);
			int numCorruptFiles = corruptFiles.Count;
			string filler;
			if (numCorruptFiles > 0)
			{
				filler = Sharpen.Extensions.ToString(numCorruptFiles);
			}
			else
			{
				if (currentCookie[0].Equals("0"))
				{
					filler = "no";
				}
				else
				{
					filler = "no more";
				}
			}
			@out.WriteLine("Cookie:\t" + currentCookie[0]);
			foreach (FSNamesystem.CorruptFileBlockInfo c in corruptFiles)
			{
				@out.WriteLine(c.ToString());
			}
			@out.WriteLine("\n\nThe filesystem under path '" + path + "' has " + filler + " CORRUPT files"
				);
			@out.WriteLine();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void Check(string parent, HdfsFileStatus file, NamenodeFsck.Result
			 res)
		{
			string path = file.GetFullName(parent);
			bool isOpen = false;
			if (file.IsDir())
			{
				if (snapshottableDirs != null && snapshottableDirs.Contains(path))
				{
					string snapshotPath = (path.EndsWith(Path.Separator) ? path : path + Path.Separator
						) + HdfsConstants.DotSnapshotDir;
					HdfsFileStatus snapshotFileInfo = namenode.GetRpcServer().GetFileInfo(snapshotPath
						);
					Check(snapshotPath, snapshotFileInfo, res);
				}
				byte[] lastReturnedName = HdfsFileStatus.EmptyName;
				DirectoryListing thisListing;
				if (showFiles)
				{
					@out.WriteLine(path + " <dir>");
				}
				res.totalDirs++;
				do
				{
					System.Diagnostics.Debug.Assert(lastReturnedName != null);
					thisListing = namenode.GetRpcServer().GetListing(path, lastReturnedName, false);
					if (thisListing == null)
					{
						return;
					}
					HdfsFileStatus[] files = thisListing.GetPartialListing();
					for (int i = 0; i < files.Length; i++)
					{
						Check(path, files[i], res);
					}
					lastReturnedName = thisListing.GetLastName();
				}
				while (thisListing.HasMore());
				return;
			}
			if (file.IsSymlink())
			{
				if (showFiles)
				{
					@out.WriteLine(path + " <symlink>");
				}
				res.totalSymlinks++;
				return;
			}
			long fileLen = file.GetLen();
			// Get block locations without updating the file access time 
			// and without block access tokens
			LocatedBlocks blocks = null;
			FSNamesystem fsn = namenode.GetNamesystem();
			fsn.ReadLock();
			try
			{
				blocks = fsn.GetBlockLocations(fsn.GetPermissionChecker(), path, 0, fileLen, false
					, false).blocks;
			}
			catch (FileNotFoundException)
			{
				blocks = null;
			}
			finally
			{
				fsn.ReadUnlock();
			}
			if (blocks == null)
			{
				// the file is deleted
				return;
			}
			isOpen = blocks.IsUnderConstruction();
			if (isOpen && !showOpenFiles)
			{
				// We collect these stats about open files to report with default options
				res.totalOpenFilesSize += fileLen;
				res.totalOpenFilesBlocks += blocks.LocatedBlockCount();
				res.totalOpenFiles++;
				return;
			}
			res.totalFiles++;
			res.totalSize += fileLen;
			res.totalBlocks += blocks.LocatedBlockCount();
			if (showOpenFiles && isOpen)
			{
				@out.Write(path + " " + fileLen + " bytes, " + blocks.LocatedBlockCount() + " block(s), OPENFORWRITE: "
					);
			}
			else
			{
				if (showFiles)
				{
					@out.Write(path + " " + fileLen + " bytes, " + blocks.LocatedBlockCount() + " block(s): "
						);
				}
				else
				{
					@out.Write('.');
				}
			}
			if (res.totalFiles % 100 == 0)
			{
				@out.WriteLine();
				@out.Flush();
			}
			int missing = 0;
			int corrupt = 0;
			long missize = 0;
			int underReplicatedPerFile = 0;
			int misReplicatedPerFile = 0;
			StringBuilder report = new StringBuilder();
			int i_1 = 0;
			foreach (LocatedBlock lBlk in blocks.GetLocatedBlocks())
			{
				ExtendedBlock block = lBlk.GetBlock();
				bool isCorrupt = lBlk.IsCorrupt();
				string blkName = block.ToString();
				DatanodeInfo[] locs = lBlk.GetLocations();
				NumberReplicas numberReplicas = namenode.GetNamesystem().GetBlockManager().CountNodes
					(block.GetLocalBlock());
				int liveReplicas = numberReplicas.LiveReplicas();
				res.totalReplicas += liveReplicas;
				short targetFileReplication = file.GetReplication();
				res.numExpectedReplicas += targetFileReplication;
				if (liveReplicas < res.minReplication)
				{
					res.numUnderMinReplicatedBlocks++;
				}
				if (liveReplicas > targetFileReplication)
				{
					res.excessiveReplicas += (liveReplicas - targetFileReplication);
					res.numOverReplicatedBlocks += 1;
				}
				//keep track of storage tier counts
				if (this.showStoragePolcies && lBlk.GetStorageTypes() != null)
				{
					StorageType[] storageTypes = lBlk.GetStorageTypes();
					storageTypeSummary.Add(Arrays.CopyOf(storageTypes, storageTypes.Length), fsn.GetBlockManager
						().GetStoragePolicy(file.GetStoragePolicy()));
				}
				// Check if block is Corrupt
				if (isCorrupt)
				{
					corrupt++;
					res.corruptBlocks++;
					@out.Write("\n" + path + ": CORRUPT blockpool " + block.GetBlockPoolId() + " block "
						 + block.GetBlockName() + "\n");
				}
				if (liveReplicas >= res.minReplication)
				{
					res.numMinReplicatedBlocks++;
				}
				if (liveReplicas < targetFileReplication && liveReplicas > 0)
				{
					res.missingReplicas += (targetFileReplication - liveReplicas);
					res.numUnderReplicatedBlocks += 1;
					underReplicatedPerFile++;
					if (!showFiles)
					{
						@out.Write("\n" + path + ": ");
					}
					@out.WriteLine(" Under replicated " + block + ". Target Replicas is " + targetFileReplication
						 + " but found " + liveReplicas + " replica(s).");
				}
				// count mis replicated blocks
				BlockPlacementStatus blockPlacementStatus = bpPolicy.VerifyBlockPlacement(path, lBlk
					, targetFileReplication);
				if (!blockPlacementStatus.IsPlacementPolicySatisfied())
				{
					res.numMisReplicatedBlocks++;
					misReplicatedPerFile++;
					if (!showFiles)
					{
						if (underReplicatedPerFile == 0)
						{
							@out.WriteLine();
						}
						@out.Write(path + ": ");
					}
					@out.WriteLine(" Replica placement policy is violated for " + block + ". " + blockPlacementStatus
						.GetErrorDescription());
				}
				report.Append(i_1 + ". " + blkName + " len=" + block.GetNumBytes());
				if (liveReplicas == 0)
				{
					report.Append(" MISSING!");
					res.AddMissing(block.ToString(), block.GetNumBytes());
					missing++;
					missize += block.GetNumBytes();
				}
				else
				{
					report.Append(" repl=" + liveReplicas);
					if (showLocations || showRacks)
					{
						StringBuilder sb = new StringBuilder("[");
						for (int j = 0; j < locs.Length; j++)
						{
							if (j > 0)
							{
								sb.Append(", ");
							}
							if (showRacks)
							{
								sb.Append(NodeBase.GetPath(locs[j]));
							}
							else
							{
								sb.Append(locs[j]);
							}
						}
						sb.Append(']');
						report.Append(" " + sb.ToString());
					}
				}
				report.Append('\n');
				i_1++;
			}
			if ((missing > 0) || (corrupt > 0))
			{
				if (!showFiles && (missing > 0))
				{
					@out.Write("\n" + path + ": MISSING " + missing + " blocks of total size " + missize
						 + " B.");
				}
				res.corruptFiles++;
				if (isOpen)
				{
					Log.Info("Fsck: ignoring open file " + path);
				}
				else
				{
					if (doMove)
					{
						CopyBlocksToLostFound(parent, file, blocks);
					}
					if (doDelete)
					{
						DeleteCorruptedFile(path);
					}
				}
			}
			if (showFiles)
			{
				if (missing > 0)
				{
					@out.Write(" MISSING " + missing + " blocks of total size " + missize + " B\n");
				}
				else
				{
					if (underReplicatedPerFile == 0 && misReplicatedPerFile == 0)
					{
						@out.Write(" OK\n");
					}
				}
				if (showBlocks)
				{
					@out.Write(report.ToString() + "\n");
				}
			}
		}

		private void DeleteCorruptedFile(string path)
		{
			try
			{
				namenode.GetRpcServer().Delete(path, true);
				Log.Info("Fsck: deleted corrupt file " + path);
			}
			catch (Exception e)
			{
				Log.Error("Fsck: error deleting corrupted file " + path, e);
				internalError = true;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool HdfsPathExists(string path)
		{
			try
			{
				HdfsFileStatus hfs = namenode.GetRpcServer().GetFileInfo(path);
				return (hfs != null);
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CopyBlocksToLostFound(string parent, HdfsFileStatus file, LocatedBlocks
			 blocks)
		{
			DFSClient dfs = new DFSClient(NameNode.GetAddress(conf), conf);
			string fullName = file.GetFullName(parent);
			OutputStream fos = null;
			try
			{
				if (!lfInited)
				{
					LostFoundInit(dfs);
				}
				if (!lfInitedOk)
				{
					throw new IOException("failed to initialize lost+found");
				}
				string target = lostFound + fullName;
				if (HdfsPathExists(target))
				{
					Log.Warn("Fsck: can't copy the remains of " + fullName + " to " + "lost+found, because "
						 + target + " already exists.");
					return;
				}
				if (!namenode.GetRpcServer().Mkdirs(target, file.GetPermission(), true))
				{
					throw new IOException("failed to create directory " + target);
				}
				// create chains
				int chain = 0;
				bool copyError = false;
				foreach (LocatedBlock lBlk in blocks.GetLocatedBlocks())
				{
					LocatedBlock lblock = lBlk;
					DatanodeInfo[] locs = lblock.GetLocations();
					if (locs == null || locs.Length == 0)
					{
						if (fos != null)
						{
							fos.Flush();
							fos.Close();
							fos = null;
						}
						continue;
					}
					if (fos == null)
					{
						fos = dfs.Create(target + "/" + chain, true);
						chain++;
					}
					// copy the block. It's a pity it's not abstracted from DFSInputStream ...
					try
					{
						CopyBlock(dfs, lblock, fos);
					}
					catch (Exception e)
					{
						Log.Error("Fsck: could not copy block " + lblock.GetBlock() + " to " + target, e);
						fos.Flush();
						fos.Close();
						fos = null;
						internalError = true;
						copyError = true;
					}
				}
				if (copyError)
				{
					Log.Warn("Fsck: there were errors copying the remains of the " + "corrupted file "
						 + fullName + " to /lost+found");
				}
				else
				{
					Log.Info("Fsck: copied the remains of the corrupted file " + fullName + " to /lost+found"
						);
				}
			}
			catch (Exception e)
			{
				Log.Error("copyBlocksToLostFound: error processing " + fullName, e);
				internalError = true;
			}
			finally
			{
				if (fos != null)
				{
					fos.Close();
				}
				dfs.Close();
			}
		}

		/*
		* XXX (ab) Bulk of this method is copied verbatim from {@link DFSClient}, which is
		* bad. Both places should be refactored to provide a method to copy blocks
		* around.
		*/
		/// <exception cref="System.Exception"/>
		private void CopyBlock(DFSClient dfs, LocatedBlock lblock, OutputStream fos)
		{
			int failures = 0;
			IPEndPoint targetAddr = null;
			TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
			BlockReader blockReader = null;
			ExtendedBlock block = lblock.GetBlock();
			while (blockReader == null)
			{
				DatanodeInfo chosenNode;
				try
				{
					chosenNode = BestNode(dfs, lblock.GetLocations(), deadNodes);
					targetAddr = NetUtils.CreateSocketAddr(chosenNode.GetXferAddr());
				}
				catch (IOException ie)
				{
					if (failures >= DFSConfigKeys.DfsClientMaxBlockAcquireFailuresDefault)
					{
						throw new IOException("Could not obtain block " + lblock, ie);
					}
					Log.Info("Could not obtain block from any node:  " + ie);
					try
					{
						Sharpen.Thread.Sleep(10000);
					}
					catch (Exception)
					{
					}
					deadNodes.Clear();
					failures++;
					continue;
				}
				try
				{
					string file = BlockReaderFactory.GetFileName(targetAddr, block.GetBlockPoolId(), 
						block.GetBlockId());
					blockReader = new BlockReaderFactory(dfs.GetConf()).SetFileName(file).SetBlock(block
						).SetBlockToken(lblock.GetBlockToken()).SetStartOffset(0).SetLength(-1).SetVerifyChecksum
						(true).SetClientName("fsck").SetDatanodeInfo(chosenNode).SetInetSocketAddress(targetAddr
						).SetCachingStrategy(CachingStrategy.NewDropBehind()).SetClientCacheContext(dfs.
						GetClientContext()).SetConfiguration(namenode.conf).SetRemotePeerFactory(new _RemotePeerFactory_744
						(this, dfs)).Build();
				}
				catch (IOException ex)
				{
					// Put chosen node into dead list, continue
					Log.Info("Failed to connect to " + targetAddr + ":" + ex);
					deadNodes.AddItem(chosenNode);
				}
			}
			byte[] buf = new byte[1024];
			int cnt = 0;
			bool success = true;
			long bytesRead = 0;
			try
			{
				while ((cnt = blockReader.Read(buf, 0, buf.Length)) > 0)
				{
					fos.Write(buf, 0, cnt);
					bytesRead += cnt;
				}
				if (bytesRead != block.GetNumBytes())
				{
					throw new IOException("Recorded block size is " + block.GetNumBytes() + ", but datanode returned "
						 + bytesRead + " bytes");
				}
			}
			catch (Exception e)
			{
				Log.Error("Error reading block", e);
				success = false;
			}
			finally
			{
				blockReader.Close();
			}
			if (!success)
			{
				throw new Exception("Could not copy block data for " + lblock.GetBlock());
			}
		}

		private sealed class _RemotePeerFactory_744 : RemotePeerFactory
		{
			public _RemotePeerFactory_744(NamenodeFsck _enclosing, DFSClient dfs)
			{
				this._enclosing = _enclosing;
				this.dfs = dfs;
			}

			/// <exception cref="System.IO.IOException"/>
			public Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
			{
				Peer peer = null;
				Socket s = NetUtils.GetDefaultSocketFactory(this._enclosing.conf).CreateSocket();
				try
				{
					s.Connect(addr, HdfsServerConstants.ReadTimeout);
					s.ReceiveTimeout = HdfsServerConstants.ReadTimeout;
					peer = TcpPeerServer.PeerFromSocketAndKey(dfs.GetSaslDataTransferClient(), s, this
						._enclosing, blockToken, datanodeId);
				}
				finally
				{
					if (peer == null)
					{
						IOUtils.CloseQuietly(s);
					}
				}
				return peer;
			}

			private readonly NamenodeFsck _enclosing;

			private readonly DFSClient dfs;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DataEncryptionKey NewDataEncryptionKey()
		{
			return namenode.GetRpcServer().GetDataEncryptionKey();
		}

		/*
		* XXX (ab) See comment above for copyBlock().
		*
		* Pick the best node from which to stream the data.
		* That's the local one, if available.
		*/
		/// <exception cref="System.IO.IOException"/>
		private DatanodeInfo BestNode(DFSClient dfs, DatanodeInfo[] nodes, TreeSet<DatanodeInfo
			> deadNodes)
		{
			if ((nodes == null) || (nodes.Length - deadNodes.Count < 1))
			{
				throw new IOException("No live nodes contain current block");
			}
			DatanodeInfo chosenNode;
			do
			{
				chosenNode = nodes[DFSUtil.GetRandom().Next(nodes.Length)];
			}
			while (deadNodes.Contains(chosenNode));
			return chosenNode;
		}

		private void LostFoundInit(DFSClient dfs)
		{
			lfInited = true;
			try
			{
				string lfName = "/lost+found";
				HdfsFileStatus lfStatus = dfs.GetFileInfo(lfName);
				if (lfStatus == null)
				{
					// not exists
					lfInitedOk = dfs.Mkdirs(lfName, null, true);
					lostFound = lfName;
				}
				else
				{
					if (!lfStatus.IsDir())
					{
						// exists but not a directory
						Log.Warn("Cannot use /lost+found : a regular file with this name exists.");
						lfInitedOk = false;
					}
					else
					{
						// exists and is a directory
						lostFound = lfName;
						lfInitedOk = true;
					}
				}
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				lfInitedOk = false;
			}
			if (lostFound == null)
			{
				Log.Warn("Cannot initialize /lost+found .");
				lfInitedOk = false;
				internalError = true;
			}
		}

		/// <summary>FsckResult of checking, plus overall DFS statistics.</summary>
		internal class Result
		{
			internal readonly IList<string> missingIds = new AList<string>();

			internal long missingSize = 0L;

			internal long corruptFiles = 0L;

			internal long corruptBlocks = 0L;

			internal long excessiveReplicas = 0L;

			internal long missingReplicas = 0L;

			internal long numUnderMinReplicatedBlocks = 0L;

			internal long numOverReplicatedBlocks = 0L;

			internal long numUnderReplicatedBlocks = 0L;

			internal long numMisReplicatedBlocks = 0L;

			internal long numMinReplicatedBlocks = 0L;

			internal long totalBlocks = 0L;

			internal long numExpectedReplicas = 0L;

			internal long totalOpenFilesBlocks = 0L;

			internal long totalFiles = 0L;

			internal long totalOpenFiles = 0L;

			internal long totalDirs = 0L;

			internal long totalSymlinks = 0L;

			internal long totalSize = 0L;

			internal long totalOpenFilesSize = 0L;

			internal long totalReplicas = 0L;

			internal readonly short replication;

			internal readonly int minReplication;

			internal Result(Configuration conf)
			{
				// blocks that do not satisfy block placement policy
				// minimally replicatedblocks
				this.replication = (short)conf.GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys
					.DfsReplicationDefault);
				this.minReplication = (short)conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey
					, DFSConfigKeys.DfsNamenodeReplicationMinDefault);
			}

			/// <summary>DFS is considered healthy if there are no missing blocks.</summary>
			internal virtual bool IsHealthy()
			{
				return ((missingIds.Count == 0) && (corruptBlocks == 0));
			}

			/// <summary>Add a missing block name, plus its size.</summary>
			internal virtual void AddMissing(string id, long size)
			{
				missingIds.AddItem(id);
				missingSize += size;
			}

			/// <summary>Return the actual replication factor.</summary>
			internal virtual float GetReplicationFactor()
			{
				if (totalBlocks == 0)
				{
					return 0.0f;
				}
				return (float)(totalReplicas) / (float)totalBlocks;
			}

			public override string ToString()
			{
				StringBuilder res = new StringBuilder();
				res.Append("Status: ").Append((IsHealthy() ? "HEALTHY" : "CORRUPT")).Append("\n Total size:\t"
					).Append(totalSize).Append(" B");
				if (totalOpenFilesSize != 0)
				{
					res.Append(" (Total open files size: ").Append(totalOpenFilesSize).Append(" B)");
				}
				res.Append("\n Total dirs:\t").Append(totalDirs).Append("\n Total files:\t").Append
					(totalFiles);
				res.Append("\n Total symlinks:\t\t").Append(totalSymlinks);
				if (totalOpenFiles != 0)
				{
					res.Append(" (Files currently being written: ").Append(totalOpenFiles).Append(")"
						);
				}
				res.Append("\n Total blocks (validated):\t").Append(totalBlocks);
				if (totalBlocks > 0)
				{
					res.Append(" (avg. block size ").Append((totalSize / totalBlocks)).Append(" B)");
				}
				if (totalOpenFilesBlocks != 0)
				{
					res.Append(" (Total open file blocks (not validated): ").Append(totalOpenFilesBlocks
						).Append(")");
				}
				if (corruptFiles > 0 || numUnderMinReplicatedBlocks > 0)
				{
					res.Append("\n  ********************************");
					if (numUnderMinReplicatedBlocks > 0)
					{
						res.Append("\n  UNDER MIN REPL'D BLOCKS:\t").Append(numUnderMinReplicatedBlocks);
						if (totalBlocks > 0)
						{
							res.Append(" (").Append(((float)(numUnderMinReplicatedBlocks * 100) / (float)totalBlocks
								)).Append(" %)");
						}
						res.Append("\n  ").Append(DFSConfigKeys.DfsNamenodeReplicationMinKey + ":\t").Append
							(minReplication);
					}
					if (corruptFiles > 0)
					{
						res.Append("\n  CORRUPT FILES:\t").Append(corruptFiles);
						if (missingSize > 0)
						{
							res.Append("\n  MISSING BLOCKS:\t").Append(missingIds.Count).Append("\n  MISSING SIZE:\t\t"
								).Append(missingSize).Append(" B");
						}
						if (corruptBlocks > 0)
						{
							res.Append("\n  CORRUPT BLOCKS: \t").Append(corruptBlocks);
						}
					}
					res.Append("\n  ********************************");
				}
				res.Append("\n Minimally replicated blocks:\t").Append(numMinReplicatedBlocks);
				if (totalBlocks > 0)
				{
					res.Append(" (").Append(((float)(numMinReplicatedBlocks * 100) / (float)totalBlocks
						)).Append(" %)");
				}
				res.Append("\n Over-replicated blocks:\t").Append(numOverReplicatedBlocks);
				if (totalBlocks > 0)
				{
					res.Append(" (").Append(((float)(numOverReplicatedBlocks * 100) / (float)totalBlocks
						)).Append(" %)");
				}
				res.Append("\n Under-replicated blocks:\t").Append(numUnderReplicatedBlocks);
				if (totalBlocks > 0)
				{
					res.Append(" (").Append(((float)(numUnderReplicatedBlocks * 100) / (float)totalBlocks
						)).Append(" %)");
				}
				res.Append("\n Mis-replicated blocks:\t\t").Append(numMisReplicatedBlocks);
				if (totalBlocks > 0)
				{
					res.Append(" (").Append(((float)(numMisReplicatedBlocks * 100) / (float)totalBlocks
						)).Append(" %)");
				}
				res.Append("\n Default replication factor:\t").Append(replication).Append("\n Average block replication:\t"
					).Append(GetReplicationFactor()).Append("\n Corrupt blocks:\t\t").Append(corruptBlocks
					).Append("\n Missing replicas:\t\t").Append(missingReplicas);
				if (totalReplicas > 0)
				{
					res.Append(" (").Append(((float)(missingReplicas * 100) / (float)numExpectedReplicas
						)).Append(" %)");
				}
				return res.ToString();
			}
		}
	}
}
